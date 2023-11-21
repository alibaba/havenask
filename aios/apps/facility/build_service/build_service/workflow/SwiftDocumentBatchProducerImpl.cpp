/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "build_service/workflow/SwiftDocumentBatchProducerImpl.h"

#include <algorithm>
#include <assert.h>
#include <ostream>
#include <stddef.h>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "beeper/beeper.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/Locator.h"
#include "build_service/common/SwiftLinkFreshnessReporter.h"
#include "build_service/document/DocumentDefine.h"
#include "build_service/document/SwiftMessageIterator.h"
#include "build_service/util/LocatorUtil.h"
#include "build_service/util/Monitor.h"
#include "build_service/util/RangeUtil.h"
#include "indexlib/base/Progress.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/customized_config.h"
#include "indexlib/config/index_config.h"
#include "indexlib/document/BuiltinParserInitParam.h"
#include "indexlib/document/DocumentInitParam.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/IDocumentFactory.h"
#include "indexlib/document/IDocumentParser.h"
#include "indexlib/document/normal/ClassifiedDocument.h"
#include "indexlib/document/raw_document/raw_document_define.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/framework/TabletFactoryCreator.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/TaskItem.h"
#include "indexlib/util/counter/StateCounter.h"
#include "kmonitor/client/MetricType.h"
#include "swift/client/SwiftClient.h"
#include "swift/client/SwiftPartitionStatus.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;
using namespace indexlib::document;
using namespace indexlib::config;

using namespace build_service::document;
using namespace build_service::common;
namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, SwiftDocumentBatchProducerImpl);

#define LOG_PREFIX _pid.buildid().ShortDebugString().c_str()

class SwiftDocumentBatchProducerImpl::ReportFreshnessMetricTaskItem : public indexlib::util::TaskItem
{
public:
    explicit ReportFreshnessMetricTaskItem(SwiftDocumentBatchProducer* producer) : _producer(producer) {}
    ~ReportFreshnessMetricTaskItem() = default;

    ReportFreshnessMetricTaskItem(const SwiftDocumentBatchProducer&) = delete;
    ReportFreshnessMetricTaskItem& operator=(const SwiftDocumentBatchProducer&) = delete;

public:
    void Run() override { _producer->reportFreshnessMetrics(INVALID_TIMESTAMP, /* no more msg */ false); }

private:
    SwiftDocumentBatchProducer* _producer;
};

SwiftDocumentBatchProducerImpl::SwiftDocumentBatchProducerImpl(
    const common::SwiftParam& swiftParam, const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
    const proto::PartitionId& partitionId, const indexlib::util::TaskSchedulerPtr& taskScheduler)
    : _pid(partitionId)
    , _swiftParam(swiftParam)
    , _swiftReader(swiftParam.reader)
    , _lastMessageUint16Payload(0)
    , _lastMessageUint8Payload(0)
    , _lastMessageId(-1)
    , _sourceSignature(-1)
    , _startTimestamp(0)
    , _stopTimestamp(numeric_limits<int64_t>::max())
    , _lastReportTime(0)
    , _noMoreMsgBeginTs(-1)
    , _lastReadTs(0)
    , _lastValidReadTs(0)
    , _lastDocTs(INVALID_TIMESTAMP)
    , _lastIngestionTs(INVALID_TIMESTAMP)
    , _schema(schema)
    , _taskScheduler(taskScheduler)
    , _reportMetricTaskId(TaskScheduler::INVALID_TASK_ID)
{
}

SwiftDocumentBatchProducerImpl::~SwiftDocumentBatchProducerImpl()
{
    _linkReporter.reset();
    if (_taskScheduler != nullptr && _reportMetricTaskId != TaskScheduler::INVALID_TASK_ID) {
        _taskScheduler->DeleteTask(_reportMetricTaskId);
    }
}

bool SwiftDocumentBatchProducerImpl::init(const indexlib::util::MetricProviderPtr& metricProvider,
                                          const indexlib::util::CounterMapPtr& counterMap, int64_t startTimestamp,
                                          int64_t stopTimestamp, uint64_t sourceSignature)
{
    _exceptionHandler.init(metricProvider);
    _e2eLatencyReporter.init(metricProvider);
    _readLatencyMetric = DECLARE_METRIC(metricProvider, "basic/readLatency", kmonitor::GAUGE, "ms");
    _swiftReadDelayMetric = DECLARE_METRIC(metricProvider, "basic/swiftReadDelay", kmonitor::GAUGE, "s");
    _gapToStopTsMetric = DECLARE_METRIC(metricProvider, "basic/gapToStopTimestamp", kmonitor::STATUS, "s");
    _batchDocSizeMetric = DECLARE_METRIC(metricProvider, "perf/batchDocSize", kmonitor::GAUGE, "byte");
    _processingTimeMetric = DECLARE_METRIC(metricProvider, "basic/processingTime", kmonitor::GAUGE, "ms");
    _msgSizePerSecMetric = DECLARE_METRIC(metricProvider, "perf/msgSizePerSec", kmonitor::QPS, "byte");
    _batchReadDocQpsMetric = DECLARE_METRIC(metricProvider, "perf/batchReadDocQps", kmonitor::QPS, "count");
    _swiftReadDelayCounter = GET_STATE_COUNTER(counterMap, swiftReadDelay);
    if (!_swiftReadDelayCounter) {
        BS_LOG(ERROR, "can not get swiftReadDelay from counterMap");
    }
    if (startTimestamp > stopTimestamp) {
        BS_PREFIX_LOG(ERROR, "startTimestamp[%ld] > stopTimestamp[%ld]", startTimestamp, stopTimestamp);
        string errorMsg = "startTimestamp[" + StringUtil::toString(startTimestamp) + "] > stopTimestamp[" +
                          StringUtil::toString(stopTimestamp) + "]";
        BEEPER_REPORT(WORKER_ERROR_COLLECTOR_NAME, errorMsg);
        return false;
    }
    _sourceSignature = sourceSignature;
    BS_LOG(INFO, "use source signature [%lu]", _sourceSignature);
    _startTimestamp = startTimestamp;
    if (_swiftReader == nullptr) {
        BS_LOG(ERROR, "swift reader is nullptr");
        return false;
    }
    if (_startTimestamp > 0) {
        swift::protocol::ErrorCode ec = _swiftReader->seekByTimestamp(_startTimestamp);
        if (ec != swift::protocol::ERROR_NONE) {
            stringstream ss;
            ss << "seek to " << _startTimestamp << " failed, "
               << "error: " << swift::protocol::ErrorCode_Name(ec);
            BS_PREFIX_LOG(ERROR, "%s", ss.str().c_str());
            BEEPER_REPORT(WORKER_ERROR_COLLECTOR_NAME, ss.str());
            return false;
        }
        BS_PREFIX_LOG(INFO, "SwiftDocumentBatchProducerImpl seek to [%ld] at startup", _startTimestamp);
        string msg =
            "SwiftDocumentBatchProducerImpl seek to [" + StringUtil::toString(_startTimestamp) + "] at startup";
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    }

    if (stopTimestamp != numeric_limits<int64_t>::max()) {
        _stopTimestamp = stopTimestamp;
        BS_PREFIX_LOG(INFO, "stopTimestamp: %ld", stopTimestamp);
        _swiftReader->setTimestampLimit(stopTimestamp, _stopTimestamp);
        BS_PREFIX_LOG(INFO, "actual stopTimestamp: %ld", _stopTimestamp);

        string msg = "stopTimestamp: " + StringUtil::toString(stopTimestamp) +
                     ", actual stopTimestamp: " + StringUtil::toString(_stopTimestamp);
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    }
    if (_taskScheduler != nullptr) {
        if (!_taskScheduler->DeclareTaskGroup("report_freshness_metric", 500 * 1000 /* 500ms */)) {
            BS_PREFIX_LOG(ERROR, "declare task group [report_freshness_metric] failed.");
            return false;
        }
        TaskItemPtr taskItem(new ReportFreshnessMetricTaskItem(this));
        _reportMetricTaskId = _taskScheduler->AddTask("report_freshness_metric", taskItem);
        if (_reportMetricTaskId == TaskScheduler::INVALID_TASK_ID) {
            BS_PREFIX_LOG(WARN, "create report_freshness_metric task failed");
            return false;
        }
    }
    if (_swiftParam.isMultiTopic) {
        BS_PREFIX_LOG(ERROR, "multiTopic not supported in SwiftDocumentBatchProducer");
        return false;
    }
    _linkReporter = CreateSwiftLinkFreshnessReporter(metricProvider);
    return initDocumentParser(metricProvider, counterMap);
}

common::SwiftLinkFreshnessReporterPtr SwiftDocumentBatchProducerImpl::CreateSwiftLinkFreshnessReporter(
    const indexlib::util::MetricProviderPtr& metricProvider) const
{
    if (!common::SwiftLinkFreshnessReporter::needReport() || _swiftParam.swiftClient == nullptr) {
        return nullptr;
    }
    std::string topicName = _swiftParam.topicName;
    swift::network::SwiftAdminAdapterPtr swiftAdapter;
    swiftAdapter = _swiftParam.swiftClient->getAdminAdapter();
    int64_t totalSwiftPartitionCount = -1;
    if (swiftAdapter == nullptr) {
        BS_LOG(ERROR, "no valid swift adapter for topic [%s]", topicName.c_str());
        return nullptr;
    } else {
        if (!util::RangeUtil::getPartitionCount(swiftAdapter.get(), topicName, &totalSwiftPartitionCount)) {
            BS_LOG(ERROR, "failed to get swift partition count for topic [%s]", topicName.c_str());
            return nullptr;
        }
    }
    common::SwiftLinkFreshnessReporterPtr reporter(new common::SwiftLinkFreshnessReporter(_pid));
    if (!reporter->init(metricProvider, totalSwiftPartitionCount, topicName)) {
        BS_LOG(ERROR, "init swift link reporter fail for topic [%s].", topicName.c_str());
        return nullptr;
    }
    return reporter;
}

std::unique_ptr<indexlibv2::document::IDocumentFactory> SwiftDocumentBatchProducerImpl::createDocumentFactory() const
{
    assert(_schema);
    const auto& tableType = _schema->GetTableType();
    auto tabletFactoryCreator = indexlibv2::framework::TabletFactoryCreator::GetInstance();
    auto tabletFactory = tabletFactoryCreator->Create(tableType);
    if (!tabletFactory) {
        BS_LOG(ERROR, "create tablet factory with type [%s] failed, registered types [%s]", tableType.c_str(),
               autil::legacy::ToJsonString(tabletFactoryCreator->GetRegisteredType()).c_str());
        return nullptr;
    }
    auto tabletOptions = std::make_shared<indexlibv2::config::TabletOptions>();
    if (!tabletFactory->Init(tabletOptions, nullptr)) {
        BS_LOG(ERROR, "init tablet factory with type [%s] failed", tableType.c_str());
        return nullptr;
    }
    return tabletFactory->CreateDocumentFactory(_schema);
}

bool SwiftDocumentBatchProducerImpl::initDocumentParser(indexlib::util::MetricProviderPtr metricProvider,
                                                        const indexlib::util::CounterMapPtr& counterMap)
{
    if (!_schema) {
        BS_LOG(ERROR, "init document parser failed, no schema");
        return false;
    }
    const auto& tableName = _schema->GetTableName();
    if (_schema) {
        BS_LOG(INFO, "init DocumentFactoryWrapper by using schema_name [%s]", tableName.c_str());
    }

    auto documentFactory = createDocumentFactory();
    if (!documentFactory) {
        BS_LOG(ERROR, "create document factory failed, table_name[%s]", tableName.c_str());
        return false;
    }

    indexlibv2::document::BuiltInParserInitResource resource;
    resource.counterMap = counterMap;
    resource.metricProvider = metricProvider;
    std::shared_ptr<indexlibv2::document::DocumentInitParam> initParam;
    // TODO: get document parser config from tablet schema
    CustomizedConfigPtr docParserConfig;
    if (docParserConfig) {
        initParam.reset(new indexlibv2::document::BuiltInParserInitParam(docParserConfig->GetParameters(), resource));
    } else {
        indexlibv2::document::DocumentInitParam::KeyValueMap kvMap;
        initParam.reset(new indexlibv2::document::BuiltInParserInitParam(kvMap, resource));
    }
    _docParser.reset(documentFactory->CreateDocumentParser(_schema, initParam).release());
    return _docParser != nullptr;
}

FlowError SwiftDocumentBatchProducerImpl::handleSwiftNoMoreMessage(
    int64_t locatorTimestamp, const swift::protocol::ReaderProgress& readerProgress,
    std::shared_ptr<indexlibv2::document::IDocumentBatch>& docBatch)
{
    int64_t noMoreMsgBeginTs = _noMoreMsgBeginTs.load(std::memory_order_relaxed);
    int64_t currentTs = TimeUtility::currentTime();
    if (noMoreMsgBeginTs == -1) {
        _noMoreMsgBeginTs.store(currentTs, std::memory_order_relaxed);
    } else {
        int64_t gapTsInSeconds = (currentTs - noMoreMsgBeginTs) / 1000000;
        if (gapTsInSeconds > 60) {
            static int64_t logTs;
            if (currentTs - logTs > 300000000) { // 300s = 5min
                logTs = currentTs;
                BS_LOG(INFO,
                       "[%s] read from swift return SWIFT_CLIENT_NO_MORE_MSG"
                       " last over [%ld] seconds, locatorTimestamp [%ld]",
                       LOG_PREFIX, gapTsInSeconds, locatorTimestamp);
                BEEPER_FORMAT_REPORT_WITHOUT_TAGS(WORKER_ERROR_COLLECTOR_NAME,
                                                  "[%s] read from swift return SWIFT_CLIENT_NO_MORE_MSG"
                                                  " last over [%ld] seconds, locatorTimestamp [%ld]",
                                                  LOG_PREFIX, gapTsInSeconds, locatorTimestamp);
            }
            if (currentTs - _lastSkipDocTs > 60000000) {
                docBatch.reset(createDocumentBatch(nullptr));
                if (docBatch == nullptr) {
                    AUTIL_LOG(WARN, "emtpy message is not supported in tablet[%s]", _schema->GetTableName().c_str());
                    return FE_WAIT;
                }
                auto [success, progress] = util::LocatorUtil::convertSwiftMultiProgress(readerProgress);
                if (!success) {
                    AUTIL_LOG(ERROR, "convert swift progress failed [%s]", readerProgress.ShortDebugString().c_str());
                    return FE_FATAL;
                }
                if (!_swiftMessageFilter.rewriteProgress(&progress)) {
                    return FE_SKIP;
                }
                _lastSkipDocTs = currentTs;
                auto locator = common::Locator(_sourceSignature, locatorTimestamp);
                locator.SetMultiProgress({progress});
                docBatch->SetBatchLocator(locator);
                return FE_OK;
            }
        }
    }
    return FE_WAIT;
}

FlowError
SwiftDocumentBatchProducerImpl::handleSwiftError(swift::protocol::ErrorCode ec, int64_t locatorTimestamp,
                                                 const swift::protocol::ReaderProgress& readerProgress,
                                                 std::shared_ptr<indexlibv2::document::IDocumentBatch>& docBatch)
{
    assert(ec != swift::protocol::ERROR_NONE);
    FlowError ret = FE_OK;
    switch (ec) {
    case swift::protocol::ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT: {
        BS_PREFIX_LOG(INFO,
                      "swift read exceed the limited timestamp, "
                      "last message msgId[%ld] uint16Payload[%u] uint8Payload[%u]",
                      _lastMessageId, _lastMessageUint16Payload, _lastMessageUint8Payload);
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, "swift read exceed limit timestamp, reach eof.");
        ret = FE_EOF;
        break;
    }
    case swift::protocol::ERROR_SEALED_TOPIC_READ_FINISH: {
        BS_PREFIX_LOG(INFO,
                      "swift topic sealed, "
                      "last message msgId[%ld] uint16Payload[%u] uint8Payload[%u]",
                      _lastMessageId, _lastMessageUint16Payload, _lastMessageUint8Payload);
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, "swift topic sealed.");
        ret = FE_SEALED;
        break;
    }
    case swift::protocol::ERROR_CLIENT_NO_MORE_MESSAGE: {
        ret = handleSwiftNoMoreMessage(locatorTimestamp, readerProgress, docBatch);
        break;
    }
    default: {
        string errorMsg = "read from swift fail, errorCode[" + swift::protocol::ErrorCode_Name(ec) + "]";
        BS_PREFIX_LOG(WARN, "%s", errorMsg.c_str());
        BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
        ret = FE_RETRY;
        break;
    }
    };
    reportFreshnessMetrics(locatorTimestamp, /* no more msg*/ true);
    return ret;
}

// builder get processed doc from swift topic
FlowError SwiftDocumentBatchProducerImpl::produce(std::shared_ptr<indexlibv2::document::IDocumentBatch>& docBatch)
{
    int64_t timestamp;
    swift::protocol::ErrorCode ec = swift::protocol::ERROR_NONE;
    swift::protocol::ReaderProgress readerProgress;
    {
        ScopeLatencyReporter reporter(_readLatencyMetric.get());
        _swiftMessages.clear_msgs();
        ec = _swiftReader->read(timestamp, _swiftMessages, DEFAULT_WAIT_READER_TIME);
        if (ec == swift::protocol::ERROR_NONE || ec == swift::protocol::ERROR_CLIENT_NO_MORE_MESSAGE) {
            if (_swiftReader->getReaderProgress(readerProgress) != swift::protocol::ERROR_NONE) {
                // message already readed, if return FE_RETRY or similiar, message will lost
                return FE_FATAL;
            }
        }
    }
    _lastReadTs.store(timestamp, std::memory_order_relaxed);
    if (ec != swift::protocol::ERROR_NONE) {
        return handleSwiftError(ec, timestamp, readerProgress, docBatch);
    }
    size_t msgCount = _swiftMessages.msgs_size();
    if (msgCount <= 0) {
        BS_LOG(WARN, "Unexpected msg size[%zu]", msgCount);
        return FE_WAIT;
    }
    const auto& lastMsgInBatch = _swiftMessages.msgs(msgCount - 1);

    _noMoreMsgBeginTs.store(-1, std::memory_order_relaxed);
    _lastValidReadTs.store(timestamp, std::memory_order_relaxed);

    if (unlikely(-1 == _lastMessageId)) {
        BS_PREFIX_LOG(INFO, "read swift from msgId[%ld] uint16Payload[%u] uint8Payload[%u]", lastMsgInBatch.msgid(),
                      lastMsgInBatch.uint16payload(), lastMsgInBatch.uint8maskpayload());
        string msg = "read swift from msgId[" + StringUtil::toString(lastMsgInBatch.msgid()) + "] uint16Payload[" +
                     StringUtil::toString(lastMsgInBatch.uint16payload()) + "] uint8Payload[" +
                     StringUtil::toString(lastMsgInBatch.uint8maskpayload()) + "]";
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    }
    _lastMessageUint16Payload = lastMsgInBatch.uint16payload();
    _lastMessageUint8Payload = lastMsgInBatch.uint8maskpayload();
    _lastMessageId = lastMsgInBatch.msgid();
    try {
        // #TODO: handle progress with less-overhead
        auto [success, progress] = util::LocatorUtil::convertSwiftMultiProgress(readerProgress);
        if (!success) {
            AUTIL_LOG(ERROR, "convert swift progress failed [%s]", readerProgress.ShortDebugString().c_str());
            return FE_FATAL;
        }
        indexlibv2::framework::Locator::DocInfo docInfo(lastMsgInBatch.uint16payload(), lastMsgInBatch.timestamp(),
                                                        lastMsgInBatch.offsetinrawmsg(), 0);
        if (_swiftMessageFilter.needSkip(docInfo)) {
            BS_INTERVAL_LOG2(10, WARN, "filter message payload [%u] timestamp [%ld] progress [%s]",
                             lastMsgInBatch.uint16payload(), lastMsgInBatch.timestamp(),
                             readerProgress.ShortDebugString().c_str());
            return FE_SKIP;
        }
        _swiftMessageFilter.rewriteProgress(&progress);
        docBatch.reset(createDocumentBatch(&_swiftMessages));
        if (!docBatch) {
            BS_PREFIX_LOG(ERROR, "create processed document failed");
            return _exceptionHandler.transferProcessResult(false);
        }
        auto locator = common::Locator(_sourceSignature, timestamp);
        locator.SetMultiProgress({progress});
        docBatch->SetBatchLocator(locator);
        _lastDocTs = lastMsgInBatch.timestamp();
        ReportMetrics(_swiftMessages, timestamp);
    } catch (const autil::legacy::ExceptionBase& e) {
        stringstream ss;
        ss << "swift message msgId[" << lastMsgInBatch.msgid() << "] timestamp[" << lastMsgInBatch.timestamp()
           << "] uint16Payload[" << lastMsgInBatch.uint16payload() << "] uint8MaskPayload["
           << lastMsgInBatch.uint8maskpayload() << "]";
        string errorMsg = ss.str();
        BS_PREFIX_LOG(ERROR, "%s", errorMsg.c_str());
        errorMsg = "create processed document failed [" + string(e.what()) + "]";
        BS_PREFIX_LOG(ERROR, "%s", errorMsg.c_str());
        BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
    } catch (...) {
        stringstream ss;
        ss << "swift message msgId[" << lastMsgInBatch.msgid() << "] timestamp[" << lastMsgInBatch.timestamp()
           << "] uint16Payload[" << lastMsgInBatch.uint16payload() << "] uint8MaskPayload["
           << lastMsgInBatch.uint8maskpayload() << "]";
        string errorMsg = ss.str();
        BS_PREFIX_LOG(ERROR, "%s", errorMsg.c_str());
        errorMsg = "create processed document failed";
        BS_PREFIX_LOG(ERROR, "%s", errorMsg.c_str());
        BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
    }
    if (_linkReporter) {
        _linkReporter->collectSwiftMessageMeta(lastMsgInBatch);
        _linkReporter->collectSwiftMessageOffset(timestamp);
        _linkReporter->collectTimestampFieldValue(timestamp, lastMsgInBatch.uint16payload());
    }
    return FE_OK;
}

bool SwiftDocumentBatchProducerImpl::seek(const common::Locator& locator) { return seekAndGetLocator(locator).first; }

// return [success, curLocator]
std::pair<bool, common::Locator> SwiftDocumentBatchProducerImpl::seekAndGetLocator(const common::Locator& locator)
{
    auto progress = locator.GetMultiProgress();
    assert(progress.size() == 1);
    BS_INTERVAL_LOG2(120, INFO, "[%s] seek locator [%s], start timestamp[%ld]",
                     _pid.buildid().ShortDebugString().c_str(), locator.DebugString().c_str(), _startTimestamp);
    common::Locator sourceLocator(_sourceSignature, 0);
    // 中转 topic 兼容场景(tablet在线加载离线partition增量)下忽略 src 比较
    if (!locator.IsSameSrc(common::Locator(_sourceSignature, 0), true)) {
        // do not seek when src not equal
        BS_LOG(WARN, "[%s] will ignore seek locator [%s], src not match sourceSignature [%ld]",
               _pid.buildid().ShortDebugString().c_str(), locator.DebugString().c_str(), _sourceSignature);
        common::Locator resultLocator = locator;
        resultLocator.SetMultiProgress(
            {{indexlibv2::base::Progress(_swiftParam.from, _swiftParam.to, {_startTimestamp, 0})}});
        return {true, resultLocator};
    }

    int64_t timestamp = locator.GetOffset().first;
    uint32_t concurrentIdx = locator.GetOffset().second;
    BS_INTERVAL_LOG2(120, INFO, "[%s] seek swift timestamp [%ld] concurrentIdx [%u]",
                     _pid.buildid().ShortDebugString().c_str(), timestamp, concurrentIdx);
    string msg = "SwiftDocumentBatchProducerImpl seek to [" + StringUtil::toString(timestamp) + "," +
                 StringUtil::toString(concurrentIdx) + "]";
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    swift::protocol::ReaderProgress swiftProgress =
        util::LocatorUtil::convertLocatorMultiProgress(locator.GetMultiProgress(), _swiftParam.topicName,
                                                       _swiftParam.maskFilterPairs, _swiftParam.disableSwiftMaskFilter);
    if (_swiftReader->seekByProgress(swiftProgress, false /*forceSeek*/) != swift::protocol::ERROR_NONE) {
        return {false, locator};
    }
    indexlibv2::framework::Locator tmpLocator = locator;
    if (!tmpLocator.ShrinkToRange(_swiftParam.from, _swiftParam.to)) {
        BS_LOG(ERROR, "seek failed, shrink failed, locator [%s], from [%d], to [%d]", tmpLocator.DebugString().c_str(),
               _swiftParam.from, _swiftParam.to);
        return {false, locator};
    }
    _swiftMessageFilter.setSeekProgress(tmpLocator.GetMultiProgress());
    return {true, locator};
}

bool SwiftDocumentBatchProducerImpl::stop(StopOption stopOption)
{
    if (_reportMetricTaskId != TaskScheduler::INVALID_TASK_ID) {
        if (_taskScheduler != nullptr) {
            _taskScheduler->DeleteTask(_reportMetricTaskId);
        }
        _reportMetricTaskId = TaskScheduler::INVALID_TASK_ID;
    }
    return true;
}

void SwiftDocumentBatchProducerImpl::ReportMetrics(const swift::protocol::Messages& swiftMessage,
                                                   int64_t locatorTimestamp)
{
    size_t batchDocSize = swiftMessage.ByteSizeLong();
    int64_t currentTime = TimeUtility::currentTimeInMicroSeconds();
    auto lastDocTs = _lastDocTs.load();
    if (lastDocTs != INVALID_TIMESTAMP) {
        _lastE2eLatency = TimeUtility::us2ms(currentTime - lastDocTs);
        if (_lastE2eLatency > _maxE2eLatency) {
            _maxE2eLatency = _lastE2eLatency;
        }
    }
    _msgSizeSinceLastReport += batchDocSize;
    _msgCountSinceLastReport += _swiftMessages.msgs_size();
    if (currentTime - _lastReportTime <= REPORT_INTERVAL) {
        return;
    }
    _lastReportTime = currentTime;
    REPORT_METRIC(_batchDocSizeMetric, batchDocSize);
    INCREASE_VALUE(_msgSizePerSecMetric, _msgSizeSinceLastReport);
    INCREASE_VALUE(_batchReadDocQpsMetric, _msgCountSinceLastReport);
    _msgSizeSinceLastReport = 0;
    _msgCountSinceLastReport = 0;
    reportFreshnessMetrics(locatorTimestamp, /* no more msg */ false);
}

void SwiftDocumentBatchProducerImpl::reportEnd2EndLatencyMetrics(const string& docSource)
{
    if (_noMoreMsgBeginTs.load() != -1) {
        // no more msg in swift, report zero directly
        _e2eLatencyReporter.report(docSource, 0);
        return;
    }
    if (_lastE2eLatency != INVALID_TIMESTAMP) {
        _e2eLatencyReporter.report(docSource, _lastE2eLatency);
    }
}

void SwiftDocumentBatchProducerImpl::reportInflightDelayMetrics()
{
    auto lastIngestionTs = _lastIngestionTs.load();
    if (lastIngestionTs != (int64_t)INVALID_TIMESTAMP) {
        int64_t currentTime = TimeUtility::currentTimeInMicroSeconds();
        if (_isServiceRecovered) {
            REPORT_METRIC2(_processingTimeMetric, &End2EndLatencyReporter::TAGS_IS_RECOVERED,
                           TimeUtility::us2ms(currentTime - lastIngestionTs));
        } else {
            REPORT_METRIC2(_processingTimeMetric, &End2EndLatencyReporter::TAGS_NOT_RECOVERED,
                           TimeUtility::us2ms(currentTime - lastIngestionTs));
        }
    }
}

indexlibv2::document::IDocumentBatch*
SwiftDocumentBatchProducerImpl::createDocumentBatch(const swift::protocol::Messages* msgs)
{
    assert(_docParser != nullptr);
    auto stringViewIter = std::make_unique<SwiftMessageIterator>(msgs);
    auto [status, docBatch] = _docParser->BatchParse(stringViewIter.get());
    if (!status.IsOK()) {
        BS_LOG(ERROR, "parse document failed [%s], docBatch [%p]", status.ToString().c_str(), docBatch.get());
        return nullptr;
    }
    return docBatch.release();
}

int64_t SwiftDocumentBatchProducerImpl::suspendReadAtTimestamp(int64_t timestamp, common::ExceedTsAction action)
{
    int64_t actualTimestamp = timestamp;
    _swiftReader->setTimestampLimit(timestamp, actualTimestamp);

    string msg = "suspendReadAtTimestamp [" + StringUtil::toString(timestamp) + "], actual suspendTimestamp [" +
                 StringUtil::toString(actualTimestamp) + "]";
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    _stopTimestamp = actualTimestamp;
    BS_PREFIX_LOG(INFO, "suspendReadAtTimestamp [%ld], actual suspendTimestamp [%ld]", timestamp, _stopTimestamp);
    return actualTimestamp;
}

bool SwiftDocumentBatchProducerImpl::getMaxTimestamp(int64_t& timestamp) const
{
    swift::client::SwiftPartitionStatus status = _swiftReader->getSwiftPartitionStatus();
    if (TimeUtility::currentTime() - status.refreshTime > CONNECT_TIMEOUT_INTERVAL) {
        stringstream ss;
        ss << "currentTime: " << TimeUtility::currentTime() << " - refreshTime: " << status.refreshTime
           << " greater than connect timeout interval";
        string errorMsg = ss.str();
        BS_PREFIX_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }

    if (status.maxMessageId < 0) {
        timestamp = -1;
    } else {
        timestamp = status.maxMessageTimestamp;
    }
    return true;
}

void SwiftDocumentBatchProducerImpl::reportFreshnessMetrics(int64_t locatorTimestamp, bool noMoreMsg)
{
    _e2eLatencyReporter.reportMaxLatency(_maxE2eLatency);
    _maxE2eLatency = INVALID_TIMESTAMP;
    if (noMoreMsg) {
        if (_isServiceRecovered) {
            REPORT_METRIC2(_swiftReadDelayMetric, &End2EndLatencyReporter::TAGS_IS_RECOVERED, 0);
            REPORT_METRIC2(_processingTimeMetric, &End2EndLatencyReporter::TAGS_IS_RECOVERED, 0);
        } else {
            REPORT_METRIC2(_swiftReadDelayMetric, &End2EndLatencyReporter::TAGS_NOT_RECOVERED, 0);
            REPORT_METRIC2(_processingTimeMetric, &End2EndLatencyReporter::TAGS_NOT_RECOVERED, 0);
        }
        if (_swiftReadDelayCounter) {
            _swiftReadDelayCounter->Set(0);
        }
        _e2eLatencyReporter.report("", 0);
        return;
    }
    reportSwiftReadDelayMetrics(locatorTimestamp);
    reportEnd2EndLatencyMetrics(/*empty doc soource*/ "");
    reportInflightDelayMetrics();
}

void SwiftDocumentBatchProducerImpl::reportSwiftReadDelayMetrics(int64_t locatorTimestamp)
{
    int64_t maxTimestamp;
    if (!getMaxTimestamp(maxTimestamp)) {
        string errorMsg = "get max timestamp for swift failed, use current time.";
        BS_PREFIX_LOG(WARN, "%s", errorMsg.c_str());
        maxTimestamp = TimeUtility::currentTime();
    }
    int64_t freshness = 0;
    {
        int64_t interval = (_stopTimestamp != numeric_limits<int64_t>::max()) ? 120 : 300;
        static int64_t logTimestamp;
        int64_t now = autil::TimeUtility::currentTimeInSeconds();
        // INVALID_TIMESTAMP coming from ReportFreshnessMetricTaskItem::Run in taskScheduler's thread
        // maybe cause of the stopping of realtime build
        if (locatorTimestamp == INVALID_TIMESTAMP) {
            auto lastReadTs = _lastReadTs.load();
            if (lastReadTs == 0) {
                return;
            }
            locatorTimestamp = _lastReadTs;
        }
        freshness = std::max(maxTimestamp - locatorTimestamp, 0L);

        if (now - logTimestamp > interval) {
            string msg =
                "swift processed doc producer freshness [" + StringUtil::toString(freshness / 1000000) + "] seconds";
            if (_stopTimestamp != numeric_limits<int64_t>::max()) {
                int64_t gapTs = (_stopTimestamp - locatorTimestamp) / 1000000;
                msg += ", current gap [" + StringUtil::toString(gapTs) + "] seconds to stopTimestamp [" +
                       StringUtil::toString(_stopTimestamp) + "]";
            }
            BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
            logTimestamp = now;
        }
    }

    if (_stopTimestamp != numeric_limits<int64_t>::max()) {
        int64_t gapTs = _stopTimestamp - locatorTimestamp;
        REPORT_METRIC(_gapToStopTsMetric, gapTs);
    }

    if (_isServiceRecovered) {
        REPORT_METRIC2(_swiftReadDelayMetric, &End2EndLatencyReporter::TAGS_IS_RECOVERED,
                       (double)freshness / (1000 * 1000));
    } else {
        REPORT_METRIC2(_swiftReadDelayMetric, &End2EndLatencyReporter::TAGS_NOT_RECOVERED,
                       (double)freshness / (1000 * 1000));
    }

    if (_swiftReadDelayCounter) {
        _swiftReadDelayCounter->Set(freshness / (1000 * 1000));
    }
}

void SwiftDocumentBatchProducerImpl::setRecovered(bool isServiceRecovered)
{
    _isServiceRecovered = isServiceRecovered;
    _e2eLatencyReporter.setIsServiceRecovered(isServiceRecovered);
}

#undef LOG_PREFIX
}} // namespace build_service::workflow
