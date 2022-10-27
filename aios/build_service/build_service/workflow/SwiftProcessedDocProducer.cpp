#include "build_service/workflow/SwiftProcessedDocProducer.h"
#include "build_service/util/Monitor.h"
#include "build_service/common/Locator.h"
#include "build_service/common/PkTracer.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include <autil/TimeUtility.h>
#include <autil/DataBuffer.h>
#include <indexlib/misc/pool_util.h>
#include <indexlib/document/index_document/normal_document/normal_document.h>
#include <indexlib/util/counter/state_counter.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

SWIFT_USE_NAMESPACE(client);
SWIFT_USE_NAMESPACE(protocol);

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);

using namespace build_service::document;
using namespace build_service::common;

namespace build_service {
namespace workflow {

BS_LOG_SETUP(workflow, SwiftProcessedDocProducer);

#define LOG_PREFIX _buildIdStr.c_str()

SwiftProcessedDocProducer::SwiftProcessedDocProducer(
    SwiftReader *reader, const IndexPartitionSchemaPtr& schema,
    const string &buildIdStr)
    : _reader(reader)
    , _buildIdStr(buildIdStr)
    , _lastMessageUint16Payload(0)
    , _lastMessageUint8Payload(0)
    , _lastMessageId(-1)
    , _startTimestamp(0)
    , _stopTimestamp(numeric_limits<int64_t>::max())
    , _lastReportTime(0)
    , _noMoreMsgBeginTs(-1)
    , _noMoreMsgPeriod(-1)
    , _lastReadTs(0)
    , _lastValidReadTs(0)
    , _lastUpdateCommittedCheckpointTs(std::numeric_limits<int64_t>::max())
    , _maxCommitInterval(std::numeric_limits<int64_t>::max())
    , _schema(schema)
{
    
}

SwiftProcessedDocProducer::~SwiftProcessedDocProducer() {
    DELETE_AND_SET_NULL(_reader);
}

bool SwiftProcessedDocProducer::init(
        IE_NAMESPACE(misc)::MetricProviderPtr metricProvider,
        const string& pluginPath,
        const IE_NAMESPACE(util)::CounterMapPtr &counterMap,
        int64_t startTimestamp, int64_t stopTimestamp,
        int64_t noMoreMsgPeriod, int64_t maxCommitInterval)
{
    _e2eLatencyReporter.init(metricProvider);
    _readLatencyMetric = DECLARE_METRIC(metricProvider, basic/readLatency, kmonitor::GAUGE, "ms");
    _swiftReadDelayMetric = DECLARE_METRIC(metricProvider, basic/swiftReadDelay, kmonitor::STATUS, "s");
    _processedDocSizeMetric = DECLARE_METRIC(metricProvider, perf/processedDocSize, kmonitor::GAUGE, "byte");
    _swiftReadBlockedQpsMetric = DECLARE_METRIC(metricProvider, perf/swiftReadBlockedQps, kmonitor::QPS, "count");
    _commitIntervalTimeoutQpsMetric =
        DECLARE_METRIC(metricProvider, perf/commitIntervalTimeoutQps, kmonitor::QPS, "count");
    _swiftReadDelayCounter = GET_STATE_COUNTER(counterMap, swiftReadDelay);
    if (!_swiftReadDelayCounter) {
        BS_LOG(ERROR, "can not get swiftReadDelay from counterMap");
    }

    if (startTimestamp > stopTimestamp) {
        BS_PREFIX_LOG(ERROR, "startTimestamp[%ld] > stopTimestamp[%ld]", startTimestamp, stopTimestamp);
        string errorMsg = "startTimestamp[" + StringUtil::toString(startTimestamp) +
                          "] > stopTimestamp[" + StringUtil::toString(stopTimestamp) + "]";
        BEEPER_REPORT(WORKER_ERROR_COLLECTOR_NAME, errorMsg);
        return false;
    }

    _startTimestamp = startTimestamp;
    if (_startTimestamp > 0) {
        SWIFT_NS(protocol)::ErrorCode ec = _reader->seekByTimestamp(_startTimestamp);
        if (ec != SWIFT_NS(protocol)::ERROR_NONE) {
            stringstream ss;
            ss << "seek to " << _startTimestamp << " failed, "
               << "error: " << SWIFT_NS(protocol)::ErrorCode_Name(ec);
            BS_PREFIX_LOG(ERROR, "%s", ss.str().c_str());
            BEEPER_REPORT(WORKER_ERROR_COLLECTOR_NAME, ss.str());
            return false;
        }
        BS_PREFIX_LOG(INFO, "SwiftProcessedDocProducer seek to [%ld] at startup", _startTimestamp);
        string msg = "SwiftProcessedDocProducer seek to [" +
                     StringUtil::toString(_startTimestamp) + "] at startup";
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    }

    if (stopTimestamp != numeric_limits<int64_t>::max()) {
        _stopTimestamp = stopTimestamp;
        BS_PREFIX_LOG(INFO, "stopTimestamp: %ld", stopTimestamp);
        _reader->setTimestampLimit(stopTimestamp, _stopTimestamp);
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
    return initDocumentParser(metricProvider, counterMap, pluginPath);
}

bool SwiftProcessedDocProducer::initDocumentParser(
    IE_NAMESPACE(misc)::MetricProviderPtr metricProvider,
    const IE_NAMESPACE(util)::CounterMapPtr &counterMap,
    const std::string &pluginPath)
{
    if (!_schema) {
        BS_LOG(ERROR, "SwiftProcessedDocProducer init DocumentFactoryWrapper failed, no schema");
        return false;
    }
    if (_schema) {
        BS_LOG(INFO, "SwiftProcessedDocProducer init DocumentFactoryWrapper "
               "by using schema_name [%s]", _schema->GetSchemaName().c_str());
    }
    _docFactoryWrapper = DocumentFactoryWrapper::CreateDocumentFactoryWrapper(
        _schema, CUSTOMIZED_DOCUMENT_CONFIG_PARSER, pluginPath);
    
    if (!_docFactoryWrapper) {
        return false;
    }
    CustomizedConfigPtr docParserConfig =
        CustomizedConfigHelper::FindCustomizedConfig(
            _schema->GetCustomizedDocumentConfigs(), CUSTOMIZED_DOCUMENT_CONFIG_PARSER); 
    BuiltInParserInitResource resource;
    resource.counterMap = counterMap;
    resource.metricProvider = metricProvider;
    DocumentInitParamPtr initParam;
    if (docParserConfig) {
        initParam.reset(new BuiltInParserInitParam(
                            docParserConfig->GetParameters(), resource));
    } else {
        DocumentInitParam::KeyValueMap kvMap;
        initParam.reset(new BuiltInParserInitParam(kvMap, resource));
    }
    _docParser.reset(_docFactoryWrapper->CreateDocumentParser(initParam));
    return _docParser.get() != nullptr;
}

FlowError SwiftProcessedDocProducer::produce(
        document::ProcessedDocumentVecPtr &docVec)
{
    Message message;
    int64_t timestamp;
    ErrorCode ec = ERROR_NONE;

    {
        ScopeLatencyReporter reporter(_readLatencyMetric.get());
        ec = _reader->read(timestamp, message);
    }
    _lastReadTs.store(timestamp, std::memory_order_relaxed);

    if (ec == ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT) {
        BS_PREFIX_LOG(INFO, "swift read exceed the limited timestamp, "
               "last message msgId[%ld] uint16Payload[%u] uint8Payload[%u]",
               _lastMessageId, _lastMessageUint16Payload, _lastMessageUint8Payload);
        _e2eLatencyReporter.report("", 0);  // report end2endlatency 0, when eof
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME,
                      "swift read exceed limit timestamp, reach eof.");
        return FE_EOF;
    } else if (ec != ERROR_NONE) {
        if (ec != ERROR_CLIENT_NO_MORE_MESSAGE) {
            string errorMsg = "read from swift fail, errorCode["
                              + SWIFT_NS(protocol)::ErrorCode_Name(ec) + "]";
            BS_PREFIX_LOG(WARN, "%s", errorMsg.c_str());
            BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
            return FE_EXCEPTION;
        }
        if (_noMoreMsgBeginTs.load(std::memory_order_relaxed) == -1) {
            _noMoreMsgBeginTs.store(TimeUtility::currentTime(), std::memory_order_relaxed);
        }
        _e2eLatencyReporter.report("", 0); // report end2endlatency 0, when no more message to read
        return FE_WAIT;
    }
    _noMoreMsgBeginTs.store(-1, std::memory_order_relaxed);
    _lastValidReadTs.store(timestamp, std::memory_order_relaxed);

    if (unlikely(-1 == _lastMessageId)) {
        BS_PREFIX_LOG(INFO, "read swift from msgId[%ld] uint16Payload[%u] uint8Payload[%u]",
               message.msgid(), message.uint16payload(), message.uint8maskpayload());
        string msg = "read swift from msgId[" + StringUtil::toString(message.msgid()) +
                     "] uint16Payload[" + StringUtil::toString(message.uint16payload()) +
                     "] uint8Payload[" + StringUtil::toString(message.uint8maskpayload()) + "]";
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    }
    _lastMessageUint16Payload = message.uint16payload();
    _lastMessageUint8Payload = message.uint8maskpayload();
    _lastMessageId = message.msgid();
    try {
        docVec.reset(createProcessedDocument(message.data(), message.timestamp(), timestamp));
        const DocumentPtr &document = (*docVec)[0]->getDocument();
        PkTracer::fromSwiftTrace(document->GetPrimaryKey(),
                document->GetLocator().ToString(),
                message.msgid());
        IE_INDEX_DOC_TRACE(document, "read processed doc from swift");
    } catch (const autil::legacy::ExceptionBase &e) {
        stringstream ss;
        ss << "swift message msgId[" << message.msgid() << "] timestamp["
           << message.timestamp() << "] uint16Payload[" << message.uint16payload()
           << "] uint8MaskPayload[" << message.uint8maskpayload() << "]";
        string errorMsg = ss.str();
        BS_PREFIX_LOG(ERROR, "%s", errorMsg.c_str());
        errorMsg = "create processed document failed [" + string(e.what()) + "]";
        BS_PREFIX_LOG(ERROR, "%s", errorMsg.c_str());
        BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
        return FE_EXCEPTION;
    } catch (...) {
        stringstream ss;
        ss << "swift message msgId[" << message.msgid() << "] timestamp["
           << message.timestamp() << "] uint16Payload[" << message.uint16payload()
           << "] uint8MaskPayload[" << message.uint8maskpayload() << "]";
        string errorMsg = ss.str();
        BS_PREFIX_LOG(ERROR, "%s", errorMsg.c_str());
        errorMsg = "create processed document failed";
        BS_PREFIX_LOG(ERROR, "%s", errorMsg.c_str());
        BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, errorMsg);        
        return FE_EXCEPTION;
    }
    return FE_OK;
}

bool SwiftProcessedDocProducer::seek(const common::Locator &locator) {
    BS_INTERVAL_LOG2(120, INFO, "[%s] seek locator [%s], start timestamp[%ld]",
                     _buildIdStr.c_str(),
                     locator.toDebugString().c_str(), _startTimestamp);
    if (locator.getSrc() != (uint64_t)_startTimestamp) {
        // do not seek when src not equal
        BS_LOG(WARN, "[%s] will ignore seek locator [%s], src not match startTimestamp [%ld]",
               _buildIdStr.c_str(), locator.toDebugString().c_str(), _startTimestamp);
        return true;
    }
    int64_t timestamp = locator.getOffset();
    BS_INTERVAL_LOG2(120, INFO, "[%s] seek swift timestamp [%ld]",
                     _buildIdStr.c_str(), timestamp);
    return _reader->seekByTimestamp(timestamp) == SWIFT_NS(protocol)::ERROR_NONE;
}

bool SwiftProcessedDocProducer::stop() {
    return true;
}

void SwiftProcessedDocProducer::ReportMetrics(
    const string& docSource, int64_t docSize, int64_t locatorTimestamp, int64_t docTimestamp)
{
    int64_t currentTime = TimeUtility::currentTimeInMicroSeconds();
    if (currentTime - _lastReportTime <= REPORT_INTERVAL) {
        return;
    }
    _lastReportTime = currentTime;
    REPORT_METRIC(_processedDocSizeMetric, docSize);
    reportFreshnessMetrics(locatorTimestamp);
    if (docTimestamp != (int64_t)INVALID_TIMESTAMP) {
        int64_t end2EndLatency = TimeUtility::us2ms(currentTime - docTimestamp);
        _e2eLatencyReporter.report(docSource, end2EndLatency);
    }    
}

ProcessedDocumentVec *SwiftProcessedDocProducer::createProcessedDocument(
        const string &docStr, int64_t docTimestamp, int64_t locatorTimestamp)
{
    DocumentPtr document = transDocStrToDocument(docStr);
    int64_t ts = document->GetTimestamp();
    string docSource = document->GetSource();
    ReportMetrics(docSource, docStr.length(), locatorTimestamp, ts);
    document->SetTimestamp(docTimestamp);

    NormalDocumentPtr normDoc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    if (normDoc)
    {
        normDoc->SetUserTimestamp(ts);
        const NormalDocument::DocumentVector& subDocuments = normDoc->GetSubDocuments();
        size_t subDocumentCount = subDocuments.size();
        for (size_t i = 0; i < subDocumentCount; i++) {
            subDocuments[i]->SetTimestamp(docTimestamp);
        }
    }
    
    unique_ptr<ProcessedDocumentVec> processedDocumentVec(new ProcessedDocumentVec);
    ProcessedDocumentPtr processedDoc(new ProcessedDocument);
    processedDoc->setLocator(common::Locator(_startTimestamp, locatorTimestamp));
    processedDoc->setDocument(document);
    processedDocumentVec->push_back(processedDoc);

    return processedDocumentVec.release();
}

DocumentPtr SwiftProcessedDocProducer::transDocStrToDocument(const string &docStr)
{
    if (!_docParser)
    {
        BS_LOG(ERROR, "no document parser!");
        return DocumentPtr();
    }
    return _docParser->Parse(ConstString(docStr));
}

int64_t SwiftProcessedDocProducer::suspendReadAtTimestamp(int64_t timestamp) {
    int64_t actualTimestamp = timestamp;
    _reader->setTimestampLimit(timestamp, actualTimestamp);

    string msg = "suspendReadAtTimestamp [" + StringUtil::toString(timestamp) +
                 "], actual suspendTimestamp [" + StringUtil::toString(actualTimestamp) + "]";
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    _stopTimestamp = actualTimestamp;
    return actualTimestamp;
}

void SwiftProcessedDocProducer::resumeRead() {
    int64_t notUsed;
    _reader->setTimestampLimit(numeric_limits<int64_t>::max(), notUsed);
}

bool SwiftProcessedDocProducer::getMaxTimestamp(int64_t &timestamp) {
    SWIFT_NS(client)::SwiftPartitionStatus status =
        _reader->getSwiftPartitionStatus();
    if (TimeUtility::currentTime() - status.refreshTime
        > CONNECT_TIMEOUT_INTERVAL)
    {
        stringstream ss;
        ss << "currentTime: " << TimeUtility::currentTime() << " - refreshTime: "
           << status.refreshTime << " greater than connect timeout interval";
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

void SwiftProcessedDocProducer::reportFreshnessMetrics(int64_t locatorTimestamp)
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
        if (now - logTimestamp > interval) {
            string msg = "swift processed doc producer freshness [" +
                         StringUtil::toString(freshness / 1000000) + "] seconds";
            if (_stopTimestamp != numeric_limits<int64_t>::max())
            {
                int64_t gapTs = (_stopTimestamp - locatorTimestamp) / 1000000;
                msg += ", current gap [" + StringUtil::toString(gapTs) +
                       "] seconds to stopTimestamp [" +
                       StringUtil::toString(_stopTimestamp) + "]";
            }
            BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);            
            logTimestamp = now;
        }
    }

    REPORT_METRIC(_swiftReadDelayMetric, (double)freshness / (1000 * 1000));
    if (_swiftReadDelayCounter) {
        _swiftReadDelayCounter->Set(freshness / (1000 * 1000));
    }
}

int64_t SwiftProcessedDocProducer::getStartTimestamp() const {
    return _startTimestamp;
}

bool SwiftProcessedDocProducer::updateCommittedCheckpoint(int64_t checkpoint) {
    if (_reader) {
        bool ret = _reader->updateCommittedCheckpoint(checkpoint);
        if (ret) {
            int64_t curTs = TimeUtility::currentTime();
            _lastUpdateCommittedCheckpointTs = curTs;
        }
        return ret;
    }
    return false;
}

bool SwiftProcessedDocProducer::needUpdateCommittedCheckpoint() const {
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
    // To prevert this situation, SwiftProcessedDocProducers need to update
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

}
}
