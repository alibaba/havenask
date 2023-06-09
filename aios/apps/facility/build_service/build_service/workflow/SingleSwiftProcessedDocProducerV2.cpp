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
#include "build_service/workflow/SingleSwiftProcessedDocProducerV2.h"

#include "autil/DataBuffer.h"
#include "autil/TimeUtility.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/Locator.h"
#include "build_service/common/PkTracer.h"
#include "build_service/util/LocatorUtil.h"
#include "build_service/util/Monitor.h"
#include "build_service/workflow/ReportFreshnessMetricTaskItem.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/IDocumentFactory.h"
#include "indexlib/document/IDocumentParser.h"
#include "indexlib/document/kv/KVDocument.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/framework/TabletFactoryCreator.h"
#include "indexlib/util/DocTracer.h"
#include "indexlib/util/counter/StateCounter.h"
#include "swift/protocol/SwiftMessage.pb.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

using namespace swift::client;
using namespace swift::protocol;

using namespace indexlib::document;
using namespace indexlib::config;

using namespace build_service::document;
using namespace build_service::common;
namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, SingleSwiftProcessedDocProducerV2);

#define LOG_PREFIX _buildIdStr.c_str()

SingleSwiftProcessedDocProducerV2::SingleSwiftProcessedDocProducerV2(
    const common::SwiftParam& swiftParam, const std::shared_ptr<indexlibv2::config::TabletSchema>& schema,
    const proto::PartitionId& partitionId, const indexlib::util::TaskSchedulerPtr& taskScheduler)
    : _swiftParam(swiftParam)
    , _lastMessageUint16Payload(0)
    , _lastMessageUint8Payload(0)
    , _lastMessageId(-1)
    , _sourceSignature(-1)
    , _startTimestamp(0)
    , _stopTimestamp(numeric_limits<int64_t>::max())
    , _lastReportTime(0)
    , _noMoreMsgBeginTs(-1)
    , _noMoreMsgPeriod(-1)
    , _lastReadTs(0)
    , _lastValidReadTs(0)
    , _lastUpdateCommittedCheckpointTs(std::numeric_limits<int64_t>::max())
    , _maxCommitInterval(std::numeric_limits<int64_t>::max())
    , _allowSeekCrossSrc(false)
    , _ckpDocReportInterval(10)
    , _lastDocTs(INVALID_TIMESTAMP)
    , _lastIngestionTs(INVALID_TIMESTAMP)
    , _schema(schema)
    , _taskScheduler(taskScheduler)
    , _reportMetricTaskId(TaskScheduler::INVALID_TASK_ID)
    , _fastQueueLastReportTs(-1)
{
    _ckpDocReportTime = autil::TimeUtility::currentTimeInSeconds();
    const char* param = getenv("checkpoint_report_interval_in_s");
    int64_t interval;
    if (param && StringUtil::fromString(string(param), interval)) {
        _ckpDocReportInterval = interval;
    }
    if (_ckpDocReportInterval >= 0) {
        AUTIL_LOG(INFO, "Use CHECKPOINT_DOC, _ckpDocReportInterval is [%ld]s.", _ckpDocReportInterval);
    } else {
        AUTIL_LOG(INFO, "Forbidden CHECKPOINT_DOC, _ckpDocReportInterval is [%ld]s.", _ckpDocReportInterval);
    }
    for (auto cluster : partitionId.clusternames()) {
        _clusters.push_back(cluster);
    }
    _buildIdStr = partitionId.buildid().ShortDebugString();
}

SingleSwiftProcessedDocProducerV2::~SingleSwiftProcessedDocProducerV2()
{
    _linkReporter.reset();
    if (_taskScheduler != nullptr && _reportMetricTaskId != TaskScheduler::INVALID_TASK_ID) {
        _taskScheduler->DeleteTask(_reportMetricTaskId);
    }

    DELETE_AND_SET_NULL(_swiftParam.reader);
}

bool SingleSwiftProcessedDocProducerV2::init(indexlib::util::MetricProviderPtr metricProvider,
                                             const std::string& pluginPath,
                                             const indexlib::util::CounterMapPtr& counterMap, int64_t startTimestamp,
                                             int64_t stopTimestamp, int64_t noMoreMsgPeriod, int64_t maxCommitInterval,
                                             uint64_t sourceSignature, bool allowSeekCrossSrc)
{
    _exceptionHandler.init(metricProvider);
    _allowSeekCrossSrc = allowSeekCrossSrc;
    _e2eLatencyReporter.init(metricProvider);
    _readLatencyMetric = DECLARE_METRIC(metricProvider, "basic/readLatency", kmonitor::GAUGE, "ms");
    _swiftReadDelayMetric = DECLARE_METRIC(metricProvider, "basic/swiftReadDelay", kmonitor::GAUGE, "s");
    _gapToStopTsMetric = DECLARE_METRIC(metricProvider, "basic/gapToStopTimestamp", kmonitor::STATUS, "s");
    _processedDocSizeMetric = DECLARE_METRIC(metricProvider, "perf/processedDocSize", kmonitor::GAUGE, "byte");
    _swiftReadBlockedQpsMetric = DECLARE_METRIC(metricProvider, "perf/swiftReadBlockedQps", kmonitor::QPS, "count");
    _commitIntervalTimeoutQpsMetric =
        DECLARE_METRIC(metricProvider, "perf/commitIntervalTimeoutQps", kmonitor::QPS, "count");
    _processingTimeMetric = DECLARE_METRIC(metricProvider, "basic/processingTime", kmonitor::GAUGE, "ms");
    _fastQueueSwiftReadDelayMetric =
        DECLARE_METRIC(metricProvider, "basic/swiftReadDelay_fastQ", kmonitor::GAUGE, "ms");
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
    BS_LOG(INFO, "use source signature [%lu]", _sourceSignature);
    if (_swiftParam.disableSwiftMaskFilter && _swiftParam.maskFilterPairs.size() == 1) {
        BS_LOG(INFO, "use disableSwiftMaskFilter = true, maskFilterPair {[%u, %u]}",
               _swiftParam.maskFilterPairs[0].first, _swiftParam.maskFilterPairs[0].second);
    }
    _sourceSignature = sourceSignature;
    _startTimestamp = startTimestamp;
    if (_startTimestamp > 0) {
        swift::protocol::ErrorCode ec = _swiftParam.reader->seekByTimestamp(_startTimestamp);
        if (ec != swift::protocol::ERROR_NONE) {
            stringstream ss;
            ss << "seek to " << _startTimestamp << " failed, "
               << "error: " << swift::protocol::ErrorCode_Name(ec);
            BS_PREFIX_LOG(ERROR, "%s", ss.str().c_str());
            BEEPER_REPORT(WORKER_ERROR_COLLECTOR_NAME, ss.str());
            return false;
        }
        BS_PREFIX_LOG(INFO, "SingleSwiftProcessedDocProducerV2 seek to [%ld] at startup", _startTimestamp);
        string msg =
            "SingleSwiftProcessedDocProducerV2 seek to [" + StringUtil::toString(_startTimestamp) + "] at startup";
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    }

    if (stopTimestamp != numeric_limits<int64_t>::max()) {
        _stopTimestamp = stopTimestamp;
        BS_PREFIX_LOG(INFO, "stopTimestamp: %ld", stopTimestamp);
        _swiftParam.reader->setTimestampLimit(stopTimestamp, _stopTimestamp);
        BS_PREFIX_LOG(INFO, "actual stopTimestamp: %ld", _stopTimestamp);

        string msg = "stopTimestamp: " + StringUtil::toString(stopTimestamp) +
                     ", actual stopTimestamp: " + StringUtil::toString(_stopTimestamp);
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    }

    if (noMoreMsgPeriod <= 0) {
        BS_PREFIX_LOG(ERROR, "noMoreMsgPeriod[%ld] should be a positive integer", noMoreMsgPeriod);
        return false;
    }
    _noMoreMsgPeriod = noMoreMsgPeriod;

    if (maxCommitInterval <= 0) {
        BS_PREFIX_LOG(ERROR, "maxCommitInterval[%ld] should be a positive integer", maxCommitInterval);
        return false;
    }
    _maxCommitInterval = maxCommitInterval;
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
    return initDocumentParser(metricProvider, counterMap);
}

std::unique_ptr<indexlibv2::document::IDocumentFactory> SingleSwiftProcessedDocProducerV2::createDocumentFactory() const
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

bool SingleSwiftProcessedDocProducerV2::initDocumentParser(indexlib::util::MetricProviderPtr metricProvider,
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

// builder get processed doc from swift topic
FlowError SingleSwiftProcessedDocProducerV2::produce(document::ProcessedDocumentVecPtr& docVec)
{
    Message message;
    int64_t timestamp;
    ErrorCode ec = ERROR_NONE;

    swift::protocol::ReaderProgress readerProgress;
    {
        ScopeLatencyReporter reporter(_readLatencyMetric.get());
        ec = _swiftParam.reader->read(timestamp, message);
        if (ec == ERROR_NONE) {
            ec = _swiftParam.reader->getReaderProgress(readerProgress);
        }
    }
    _lastReadTs.store(timestamp, std::memory_order_relaxed);

    static const std::string emptyDocSource("");
    if (ec == ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT) {
        BS_PREFIX_LOG(INFO,
                      "swift read exceed the limited timestamp, "
                      "last message msgId[%ld] uint16Payload[%u] uint8Payload[%u]",
                      _lastMessageId, _lastMessageUint16Payload, _lastMessageUint8Payload);
        reportFreshnessMetrics(0, /* no more msg */ true, emptyDocSource, /* not report fast queue delay */ false);
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, "swift read exceed limit timestamp, reach eof.");
        return FE_EOF;
    } else if (ec == ERROR_SEALED_TOPIC_READ_FINISH) {
        BS_PREFIX_LOG(INFO,
                      "swift topic sealed, "
                      "last message msgId[%ld] uint16Payload[%u] uint8Payload[%u]",
                      _lastMessageId, _lastMessageUint16Payload, _lastMessageUint8Payload);
        reportFreshnessMetrics(0, /* no more msg */ true, emptyDocSource, /* not report fast queue delay */ false);
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, "swift topic sealed.");
        return FE_SEALED;
    } else if (ec != ERROR_NONE) {
        if (ec != ERROR_CLIENT_NO_MORE_MESSAGE) {
            string errorMsg = "read from swift fail, errorCode[" + swift::protocol::ErrorCode_Name(ec) + "]";
            BS_PREFIX_LOG(WARN, "%s", errorMsg.c_str());
            BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
            return FE_RETRY;
        }

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
                           " last over [%ld] seconds, lastReadTs [%ld]",
                           LOG_PREFIX, gapTsInSeconds, timestamp);
                    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(WORKER_ERROR_COLLECTOR_NAME,
                                                      "[%s] read from swift return SWIFT_CLIENT_NO_MORE_MSG"
                                                      " last over [%ld] seconds, lastReadTs [%ld]",
                                                      LOG_PREFIX, gapTsInSeconds, timestamp);
                }
            }
        }
        reportFreshnessMetrics(0, /* no more msg */ true, emptyDocSource, /* not report fast queue delay */ false);
        int64_t currentTime = autil::TimeUtility::currentTimeInSeconds();
        if (_ckpDocReportInterval >= 0 && currentTime - _ckpDocReportTime >= _ckpDocReportInterval) {
            BS_PREFIX_LOG(DEBUG, "Create CHECKPOINT_DOC, locator: src[%lu], offset[%ld].", _sourceSignature, timestamp);
            _ckpDocReportTime = currentTime;
            docVec.reset(createSkipProcessedDocument(timestamp));
            return FE_OK;
            // no message more than 60s, report processor checkpoint use current time
        }
        return FE_WAIT;
    }
    _noMoreMsgBeginTs.store(-1, std::memory_order_relaxed);
    _lastValidReadTs.store(timestamp, std::memory_order_relaxed);
    auto [success, progress] = util::LocatorUtil::convertSwiftProgress(readerProgress, _swiftParam.isMultiTopic);
    if (!success) {
        AUTIL_LOG(ERROR, "convert swift progress failed [%s]", readerProgress.ShortDebugString().c_str());
        return FE_FATAL;
    }

    if (unlikely(-1 == _lastMessageId)) {
        BS_PREFIX_LOG(INFO, "read swift from msgId[%ld] uint16Payload[%u] uint8Payload[%u]", message.msgid(),
                      message.uint16payload(), message.uint8maskpayload());
        string msg = "read swift from msgId[" + StringUtil::toString(message.msgid()) + "] uint16Payload[" +
                     StringUtil::toString(message.uint16payload()) + "] uint8Payload[" +
                     StringUtil::toString(message.uint8maskpayload()) + "]";
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    }
    _lastMessageUint16Payload = message.uint16payload();
    _lastMessageUint8Payload = message.uint8maskpayload();
    _lastMessageId = message.msgid();
    bool handleProcessedDocSuccess = false;
    try {
        if (_swiftParam.disableSwiftMaskFilter && _swiftParam.maskFilterPairs.size() == 1) {
            // disableSwiftMaskFilter=true will filter invalid msg by bs producer
            auto mask = _swiftParam.maskFilterPairs[0].first;
            auto result = _swiftParam.maskFilterPairs[0].second;
            if ((_lastMessageUint8Payload & mask) != result) {
                BS_PREFIX_LOG(
                    DEBUG,
                    "msg filtered by mask [%u], will Create CHECKPOINT_DOC, locator: src[%lu], msg timestamp[%ld].",
                    _lastMessageUint8Payload, _sourceSignature, message.timestamp());
                docVec.reset(createSkipProcessedDocument(message.timestamp()));
                return FE_OK;
            }
        }
        docVec.reset(createProcessedDocument(message.data(), message.timestamp(), timestamp, _lastMessageUint16Payload,
                                             progress));
        if (!docVec) {
            BS_PREFIX_LOG(ERROR, "create processed document failed");
            return _exceptionHandler.transferProcessResult(handleProcessedDocSuccess);
        }
        const std::shared_ptr<indexlibv2::document::IDocument>& document = (*docVec)[0]->getDocument();
        std::string pk;
        const auto& normalDoc = std::dynamic_pointer_cast<indexlibv2::document::NormalDocument>(document);
        if (normalDoc) {
            pk = normalDoc->GetPrimaryKey();
        }
        if (_lastMessageUint8Payload == ProcessedDocument::SWIFT_FILTER_BIT_FASTQUEUE ||
            _lastMessageUint8Payload ==
                (ProcessedDocument::SWIFT_FILTER_BIT_FASTQUEUE | ProcessedDocument::SWIFT_FILTER_BIT_REALTIME)) {
            BS_PREFIX_LOG(DEBUG, "pk[%s] payload[%d] read from fast queue", pk.c_str(), _lastMessageUint8Payload);
        }
        PkTracer::fromSwiftTrace(pk, document->GetLocatorV2().DebugString(), message.msgid());
        IE_DOC_TRACE(document, "read processed doc from swift");
        handleProcessedDocSuccess = true;
    } catch (const autil::legacy::ExceptionBase& e) {
        stringstream ss;
        ss << "swift message msgId[" << message.msgid() << "] timestamp[" << message.timestamp() << "] uint16Payload["
           << message.uint16payload() << "] uint8MaskPayload[" << message.uint8maskpayload() << "]";
        string errorMsg = ss.str();
        BS_PREFIX_LOG(ERROR, "%s", errorMsg.c_str());
        errorMsg = "create processed document failed [" + string(e.what()) + "]";
        BS_PREFIX_LOG(ERROR, "%s", errorMsg.c_str());
        BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
    } catch (...) {
        stringstream ss;
        ss << "swift message msgId[" << message.msgid() << "] timestamp[" << message.timestamp() << "] uint16Payload["
           << message.uint16payload() << "] uint8MaskPayload[" << message.uint8maskpayload() << "]";
        string errorMsg = ss.str();
        BS_PREFIX_LOG(ERROR, "%s", errorMsg.c_str());
        errorMsg = "create processed document failed";
        BS_PREFIX_LOG(ERROR, "%s", errorMsg.c_str());
        BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
    }

    if (_linkReporter) {
        _linkReporter->collectSwiftMessageMeta(message);
        _linkReporter->collectSwiftMessageOffset(timestamp);
    }
    return _exceptionHandler.transferProcessResult(handleProcessedDocSuccess);
}

bool SingleSwiftProcessedDocProducerV2::seek(const common::Locator& locator)
{
    return seekAndGetLocator(locator).first;
}

// return [success, curLocator]
std::pair<bool, common::Locator> SingleSwiftProcessedDocProducerV2::seekAndGetLocator(const common::Locator& locator)
{
    auto progress = locator.GetProgress();
    assert(!progress.empty());
    BS_INTERVAL_LOG2(120, INFO, "[%s] seek locator [%s], start timestamp[%ld]", _buildIdStr.c_str(),
                     locator.DebugString().c_str(), _startTimestamp);
    bool forceSeek = false;
    if (_allowSeekCrossSrc) {
        forceSeek = true;
    } else {
        common::Locator sourceLocator(_sourceSignature, 0);
        // 中转 topic 兼容场景(tablet在线加载离线partition增量)下忽略 src 比较
        if (!locator.IsSameSrc(common::Locator(_sourceSignature, 0), true)) {
            // do not seek when src not equal
            BS_LOG(WARN, "[%s] will ignore seek locator [%s], src not match sourceSignature [%ld]", _buildIdStr.c_str(),
                   locator.DebugString().c_str(), _sourceSignature);
            common::Locator resultLocator = locator;
            std::vector<indexlibv2::base::Progress> progress;
            progress.push_back(indexlibv2::base::Progress(_swiftParam.from, _swiftParam.to, _startTimestamp));
            resultLocator.SetProgress(progress);
            return {true, resultLocator};
        }
    }
    int64_t timestamp = locator.GetOffset();
    BS_INTERVAL_LOG2(120, INFO, "[%s] seek swift timestamp [%ld]", _buildIdStr.c_str(), timestamp);
    string msg = "SingleSwiftProcessedDocProducerV2 seek to [" + StringUtil::toString(timestamp) + "]";
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    swift::protocol::ReaderProgress swiftProgress = util::LocatorUtil::convertLocatorProgress(
        locator.GetProgress(), _swiftParam.topicName, _swiftParam.maskFilterPairs, _swiftParam.disableSwiftMaskFilter);
    return {_swiftParam.reader->seekByProgress(swiftProgress, forceSeek) == swift::protocol::ERROR_NONE, locator};
}

bool SingleSwiftProcessedDocProducerV2::stop(StopOption stopOption)
{
    if (_reportMetricTaskId != TaskScheduler::INVALID_TASK_ID) {
        if (_taskScheduler != nullptr) {
            _taskScheduler->DeleteTask(_reportMetricTaskId);
        }
        _reportMetricTaskId = TaskScheduler::INVALID_TASK_ID;
    }
    return true;
}

void SingleSwiftProcessedDocProducerV2::ReportMetrics(const string& docSource, int64_t docSize,
                                                      int64_t locatorTimestamp)
{
    int64_t currentTime = TimeUtility::currentTimeInMicroSeconds();
    auto lastDocTs = _lastDocTs.load();
    if (lastDocTs != INVALID_TIMESTAMP) {
        _lastE2eLatency = TimeUtility::us2ms(currentTime - lastDocTs);
        if (_lastE2eLatency > _maxE2eLatency) {
            _maxE2eLatency = _lastE2eLatency;
        }
    }
    if (currentTime - _lastReportTime <= REPORT_INTERVAL) {
        return;
    }
    _lastReportTime = currentTime;
    REPORT_METRIC(_processedDocSizeMetric, docSize);
    reportFreshnessMetrics(locatorTimestamp, /* no more msg */ false, docSource,
                           /* not report fast queue delay */ false);
}

void SingleSwiftProcessedDocProducerV2::reportEnd2EndLatencyMetrics(const string& docSource)
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

void SingleSwiftProcessedDocProducerV2::reportInflightDelayMetrics()
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

void SingleSwiftProcessedDocProducerV2::reportFastQueueSwiftReadDelayMetrics()
{
    int64_t currentTime = TimeUtility::currentTimeInMicroSeconds();
    if (_fastQueueLastReportTs < 0 || currentTime - _fastQueueLastReportTs > FAST_QUEUE_REPORT_INTERVAL) {
        _fastQueueLastReportTs = currentTime;
        if (_swiftParam.reader == nullptr) {
            return;
        }
        swift::protocol::ReaderProgress progress;
        swift::protocol::ErrorCode ec = _swiftParam.reader->getReaderProgress(progress);
        if (ec != swift::protocol::ERROR_NONE) {
            stringstream ss;
            ss << "get swift reader progress failed, "
               << "error: " << swift::protocol::ErrorCode_Name(ec);
            BS_PREFIX_LOG(ERROR, "%s", ss.str().c_str());
            BEEPER_REPORT(WORKER_ERROR_COLLECTOR_NAME, ss.str());
            return;
        }
        if (progress.progress_size() >= 2) {
            for (size_t i = 0; i < progress.progress_size(); ++i) {
                const swift::protocol::SingleReaderProgress& singleReaderProgress = progress.progress(i);
                if (singleReaderProgress.filter().uint8maskresult() & ProcessedDocument::SWIFT_FILTER_BIT_FASTQUEUE) {
                    int64_t ts = singleReaderProgress.timestamp();
                    if (ts == INVALID_TIMESTAMP) {
                        continue;
                    }
                    if (currentTime - ts >= 0) {
                        if (_isServiceRecovered) {
                            REPORT_METRIC2(_fastQueueSwiftReadDelayMetric, &End2EndLatencyReporter::TAGS_IS_RECOVERED,
                                           TimeUtility::us2ms(currentTime - ts));
                        } else {
                            REPORT_METRIC2(_fastQueueSwiftReadDelayMetric, &End2EndLatencyReporter::TAGS_NOT_RECOVERED,
                                           TimeUtility::us2ms(currentTime - ts));
                        }
                    } else {
                        BS_LOG(WARN, "get reader progress time too long.");
                    }
                }
            }
        }
    }
}

ProcessedDocumentVec*
SingleSwiftProcessedDocProducerV2::createProcessedDocument(const string& docStr, int64_t docTimestamp,
                                                           int64_t locatorTimestamp, uint16_t hashId,
                                                           const std::vector<indexlibv2::base::Progress>& progress)
{
    std::shared_ptr<indexlibv2::document::IDocumentBatch> docBatch = transDocStrToDocumentBatch(docStr);
    if (!docBatch) {
        BS_LOG(ERROR, "transfer serrialized doc string to document batch failed");
        return nullptr;
    }
    assert(docBatch->GetBatchSize() == 1);
    if (_linkReporter) {
        _linkReporter->collectTimestampFieldValue(locatorTimestamp, _lastMessageUint16Payload);
    }
    auto document = (*docBatch)[0];
    string docSource = document->GetSource().to_string();
    _lastDocTs = document->GetDocInfo().timestamp;
    _lastIngestionTs = document->GetIngestionTimestamp();
    document->SetDocInfo({hashId, docTimestamp});

    bool hasReport = false;
    auto normDoc = std::dynamic_pointer_cast<indexlibv2::document::NormalDocument>(document);
    if (normDoc) {
        if (normDoc->GetUserTimestamp() == INVALID_TIMESTAMP) {
            normDoc->SetUserTimestamp(_lastDocTs);
        } else {
            hasReport = true;
            _lastDocTs = normDoc->GetUserTimestamp();
            ReportMetrics(docSource, docStr.length(), locatorTimestamp);
        }
    }

    auto kvDoc = std::dynamic_pointer_cast<indexlibv2::document::KVDocument>(document);
    if (kvDoc) {
        auto docInfo = kvDoc->GetDocInfo();
        docInfo.timestamp = docTimestamp;
        kvDoc->SetDocInfo(docInfo);
        auto userTs = kvDoc->GetUserTimestamp();
        if (userTs != INVALID_TIMESTAMP) {
            _lastDocTs = userTs;
        }
    }

    if (!hasReport) {
        ReportMetrics(docSource, docStr.length(), locatorTimestamp);
    }

    unique_ptr<ProcessedDocumentVec> processedDocumentVec(new ProcessedDocumentVec);
    ProcessedDocumentPtr processedDoc(new ProcessedDocument);
    auto locator = common::Locator(_sourceSignature, locatorTimestamp);
    locator.SetProgress(progress);
    processedDoc->setLocator(locator);
    processedDoc->setDocumentBatch(docBatch);

    ProcessedDocument::DocClusterMetaVec metas;
    metas.reserve(_clusters.size());
    for (const string& cluster : _clusters) {
        ProcessedDocument::DocClusterMeta meta;
        meta.buildType = _lastMessageUint8Payload;
        meta.hashId = _lastMessageUint16Payload;
        meta.clusterName = cluster;
        metas.push_back(meta);
    }
    processedDoc->setDocClusterMetaVec(metas);
    processedDocumentVec->push_back(processedDoc);

    return processedDocumentVec.release();
}

ProcessedDocumentVec* SingleSwiftProcessedDocProducerV2::createSkipProcessedDocument(int64_t locatorTimestamp)
{
    unique_ptr<ProcessedDocumentVec> processedDocumentVec(new ProcessedDocumentVec);
    ProcessedDocumentPtr processedDoc(new ProcessedDocument);
    processedDoc->setLocator(common::Locator(_sourceSignature, locatorTimestamp));
    processedDoc->setNeedSkip(true);
    processedDocumentVec->push_back(processedDoc);
    return processedDocumentVec.release();
}

std::unique_ptr<indexlibv2::document::IDocumentBatch>
SingleSwiftProcessedDocProducerV2::transDocStrToDocumentBatch(const string& docStr)
{
    if (!_docParser) {
        BS_LOG(ERROR, "no document parser!");
        return nullptr;
    }
    auto [status, docBatch] = _docParser->Parse(StringView(docStr));
    if (!status.IsOK() || !docBatch) {
        BS_LOG(ERROR, "parse document failed [%s], docBatch [%p]", status.ToString().c_str(), docBatch.get());
        return nullptr;
    }
    return std::move(docBatch);
}

int64_t SingleSwiftProcessedDocProducerV2::suspendReadAtTimestamp(int64_t timestamp, common::ExceedTsAction action)
{
    int64_t actualTimestamp = timestamp;
    _swiftParam.reader->setTimestampLimit(timestamp, actualTimestamp);

    string msg = "suspendReadAtTimestamp [" + StringUtil::toString(timestamp) + "], actual suspendTimestamp [" +
                 StringUtil::toString(actualTimestamp) + "]";
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    _stopTimestamp = actualTimestamp;
    BS_PREFIX_LOG(INFO, "suspendReadAtTimestamp [%ld], actual suspendTimestamp [%ld]", timestamp, _stopTimestamp);
    return actualTimestamp;
}

void SingleSwiftProcessedDocProducerV2::resumeRead()
{
    int64_t notUsed;
    _swiftParam.reader->setTimestampLimit(numeric_limits<int64_t>::max(), notUsed);
}

bool SingleSwiftProcessedDocProducerV2::getMaxTimestamp(int64_t& timestamp)
{
    swift::client::SwiftPartitionStatus status = _swiftParam.reader->getSwiftPartitionStatus();
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

void SingleSwiftProcessedDocProducerV2::reportFreshnessMetrics(int64_t locatorTimestamp, bool noMoreMsg,
                                                               const std::string& docSource,
                                                               bool isReportFastQueueSwiftReadDelay)
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
    reportEnd2EndLatencyMetrics(docSource);
    reportInflightDelayMetrics();
    if (isReportFastQueueSwiftReadDelay) {
        reportFastQueueSwiftReadDelayMetrics();
    }
}

void SingleSwiftProcessedDocProducerV2::reportSwiftReadDelayMetrics(int64_t locatorTimestamp)
{
    int64_t maxTimestamp;
    if (!getMaxTimestamp(maxTimestamp)) {
        string errorMsg = "get max timestamp for swift failed, use current time.";
        BS_PREFIX_LOG(WARN, "%s", errorMsg.c_str());
        maxTimestamp = TimeUtility::currentTime();
    }
    int64_t freshness = std::max(maxTimestamp - locatorTimestamp, 0L);

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
            freshness = std::max(maxTimestamp - locatorTimestamp, 0L);
        }
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

int64_t SingleSwiftProcessedDocProducerV2::getStartTimestamp() const { return _startTimestamp; }

void SingleSwiftProcessedDocProducerV2::setRecovered(bool isServiceRecovered)
{
    _isServiceRecovered = isServiceRecovered;
    _e2eLatencyReporter.setIsServiceRecovered(isServiceRecovered);
}

bool SingleSwiftProcessedDocProducerV2::updateCommittedCheckpoint(int64_t checkpoint)
{
    if (_swiftParam.reader) {
        bool ret = _swiftParam.reader->updateCommittedCheckpoint(checkpoint);
        if (ret) {
            int64_t curTs = TimeUtility::currentTime();
            _lastUpdateCommittedCheckpointTs = curTs;
        }
        return ret;
    }
    return false;
}

bool SingleSwiftProcessedDocProducerV2::needUpdateCommittedCheckpoint() const
{
    int64_t curTs = TimeUtility::currentTime();

    // isBlocked
    int64_t beginTs = _noMoreMsgBeginTs.load(std::memory_order_relaxed);
    if (beginTs != -1) {
        if (curTs - beginTs > _noMoreMsgPeriod) {
            INCREASE_QPS(_swiftReadBlockedQpsMetric);
            return true;
        }
    }

    // if there is a slow long-tailed processor, it may cause multi swift
    // brokers to commit hdfs simutaneously, which will bring huge pressure to
    // hdfs in a short time.
    // To prevert this situation, SingleSwiftProcessedDocProducers need to update
    // committedCheckpoints before swift brokers' DFS_COMMIT_INTERVAL_WHEN_DELAY (600s by default).

    // However, slow builders will not follow this rule, otherwise many small
    // segments will be generated.

    if (curTs - _lastValidReadTs.load(std::memory_order_relaxed) < MAX_READ_DELAY) {
        bool ret = curTs - _lastUpdateCommittedCheckpointTs > _maxCommitInterval;
        if (ret) {
            INCREASE_QPS(_commitIntervalTimeoutQpsMetric);
        }
        return ret;
    }
    return false;
}

#undef LOG_PREFIX

}} // namespace build_service::workflow
