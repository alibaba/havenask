#include "build_service/reader/SwiftRawDocumentReader.h"
#include "build_service/reader/ParserCreator.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/util/Monitor.h"
#include "build_service/util/EnvUtil.h"
#include "build_service/reader/SwiftFieldFilterRawDocumentParser.h"
#include <indexlib/misc/metric_provider.h>
#include <indexlib/misc/metric.h>
#include <autil/TimeUtility.h>
#include <autil/StringUtil.h>
#include <autil/HashAlgorithm.h>
#include <indexlib/util/counter/state_counter.h>

using namespace std;
using namespace autil;

SWIFT_USE_NAMESPACE(client);
SWIFT_USE_NAMESPACE(protocol);
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::common;

namespace build_service {
namespace reader {

BS_LOG_SETUP(reader, SwiftRawDocumentReader);

SwiftRawDocumentReader::SwiftRawDocumentReader(const util::SwiftClientCreatorPtr& swiftClientCreator)
    : _swiftReader(NULL)
    , _swiftClient(NULL)
    , _lastDocTimestamp(0)
    , _swiftReadDelayMetric(NULL)
    , _swiftClientCreator(swiftClientCreator)
    , _startTimestamp(0)
    , _exceedTsAction(RawDocumentReader::ETA_STOP)
    , _exceedSuspendTimestamp(false)
{
}

SwiftRawDocumentReader::~SwiftRawDocumentReader() {
    DELETE_AND_SET_NULL(_swiftReader);
    _swiftClient = NULL;
}

bool SwiftRawDocumentReader::init(const ReaderInitParam &params) {
    if (!RawDocumentReader::init(params)) {
        return false;
    }

    string zfsRoot = getValueFromKeyValueMap(params.kvMap, SWIFT_ZOOKEEPER_ROOT);
    string swiftClientConfig = getValueFromKeyValueMap(params.kvMap, SWIFT_CLIENT_CONFIG);
    string tmp;
    if (util::EnvUtil::getValueFromEnv("raw_topic_swift_client_config", tmp)) {
        swiftClientConfig = tmp;
    }
    BS_LOG(INFO, "swift raw topic use client config [%s]", swiftClientConfig.c_str());
    _swiftClient = createSwiftClient(zfsRoot, swiftClientConfig);
    if (_swiftClient == NULL) {
        return false;
    }

    _swiftReadDelayMetric = DECLARE_METRIC(params.metricProvider, basic/swiftReadDelay, kmonitor::STATUS, "s");
    _swiftReadDelayCounter = GET_STATE_COUNTER(params.counterMap, processor.swiftReadDelay);
    if (!_swiftReadDelayCounter) {
        BS_LOG(ERROR, "can not get swiftReadDelay from counterMap");
    }

    SWIFT_NS(protocol)::ErrorInfo errorInfo;
    string swiftReaderConfig = createSwiftReaderConfig(params);
    BS_LOG(INFO, "create swift reader with config %s", swiftReaderConfig.c_str());
    _swiftReader = createSwiftReader(swiftReaderConfig, &errorInfo);
    if (!_swiftReader) {
        stringstream ss;
        ss << "create reader failed, config[" << swiftReaderConfig << "]" << endl;
        REPORT_ERROR_WITH_ADVICE(READER_ERROR_SWIFT_CREATE_READER, ss.str(), BS_STOP);
        return false;
    }
    return initSwiftReader(_swiftReader, params.kvMap);
}

string SwiftRawDocumentReader::createSwiftReaderConfig(const ReaderInitParam &params) {
    string topicName = getValueFromKeyValueMap(params.kvMap, SWIFT_TOPIC_NAME);
    string readerConfig = getValueFromKeyValueMap(params.kvMap, SWIFT_READER_CONFIG);
    string tmp;
    if (util::EnvUtil::getValueFromEnv("raw_topic_swift_reader_config", tmp)) {
        readerConfig = tmp;
    }
    BS_LOG(INFO, "swift raw topic use reader config [%s]", readerConfig.c_str());
    stringstream ss;
    ss << "topicName=" << topicName << ";";
    if (!readerConfig.empty()) {
        ss << readerConfig << ";";
    }
    ss << "from=" << params.range.from() << ";" << "to=" << params.range.to();
    return ss.str();
}

bool SwiftRawDocumentReader::initSwiftReader(
        SwiftReaderWrapper *swiftReader, const KeyValueMap &kvMap)
{
    int64_t startTimestamp = 0;
    KeyValueMap::const_iterator it = kvMap.find(SWIFT_START_TIMESTAMP);
    if (it != kvMap.end() && !StringUtil::fromString<int64_t>(it->second, startTimestamp)) {
        BS_LOG(ERROR, "invalid startTimestamp[%s]", it->second.c_str());
        return false;
    }

    int64_t checkpoint = 0;
    it = kvMap.find(CHECKPOINT);
    if (it != kvMap.end() && !StringUtil::fromString<int64_t>(it->second, checkpoint)) {
        BS_LOG(ERROR, "invalid checkpoint[%s]", it->second.c_str());
        return false;
    }
    startTimestamp = std::max(startTimestamp, checkpoint);

    if (startTimestamp > 0 && !seek(startTimestamp)) {
        BS_LOG(ERROR, "swift reader seek to %ld failed", startTimestamp);
        return false;
    }

    if (startTimestamp > 0) {
        _startTimestamp = startTimestamp;
        BS_LOG(INFO, "swift reader seek to [%ld] at startup", _startTimestamp);
    }

    auto it1 = kvMap.find(SWIFT_SUSPEND_TIMESTAMP);
    auto it2 = kvMap.find(SWIFT_STOP_TIMESTAMP);

    if (it1 != kvMap.end() && it2 != kvMap.end()) {
        BS_LOG(ERROR, "cannot specify %s and %s at the same time",
               SWIFT_SUSPEND_TIMESTAMP.c_str(), SWIFT_STOP_TIMESTAMP.c_str());
        return false;
    }

    return setLimitTime(swiftReader, kvMap, SWIFT_SUSPEND_TIMESTAMP) &&
        setLimitTime(swiftReader, kvMap, SWIFT_STOP_TIMESTAMP);
}

bool SwiftRawDocumentReader::setLimitTime(
    SwiftReaderWrapper *swiftReader,
    const KeyValueMap &kvMap, const string &limitTimeType)
{
    auto it = kvMap.find(limitTimeType);
    if (it == kvMap.end()) {
        return true;
    }

    string msg;
    int64_t limitTime;
    if (!StringUtil::fromString(it->second, limitTime)) {
        BS_LOG(ERROR, "invalid %s[%s]", limitTimeType.c_str(), it->second.c_str());
        msg = "invalid " + limitTimeType + "[" + it->second + "]";
        BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, msg);
        return false;
    }
        
    if (_startTimestamp > limitTime) {
        BS_LOG(ERROR, "startTimestamp[%ld] > %s[%ld]",
               _startTimestamp, limitTimeType.c_str(), limitTime);
        msg = "startTimestamp[" + StringUtil::toString(_startTimestamp) + "] > " +
                   limitTimeType + "[" + StringUtil::toString(limitTime) + "]";
        BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, msg);
        return false;
    }
        
    if (limitTime != numeric_limits<int64_t>::max()) {
        swiftReader->setTimestampLimit(limitTime, limitTime);
        BS_LOG(INFO, "swift reader will suspend at timestamp[%ld]", limitTime);
        msg = "swift reader will suspend at timestamp[" + StringUtil::toString(limitTime) + "]";
        BEEPER_INTERVAL_REPORT(10, WORKER_STATUS_COLLECTOR_NAME, msg);
    }
    _exceedTsAction = (limitTimeType == SWIFT_SUSPEND_TIMESTAMP) ?
        RawDocumentReader::ETA_SUSPEND : RawDocumentReader::ETA_STOP;
    return true;
}
    
bool SwiftRawDocumentReader::seek(int64_t offset) {
    return doSeek(offset);
}

bool SwiftRawDocumentReader::doSeek(int64_t timestamp) {
    assert(_swiftReader);
    BS_LOG(INFO, "swift seek timestamp[%ld]", timestamp);
    string msg = "swift seek timestamp [" + StringUtil::toString(timestamp) + "]";
    BEEPER_INTERVAL_REPORT(10, WORKER_STATUS_COLLECTOR_NAME, msg);
    
    SWIFT_NS(protocol)::ErrorCode ec = _swiftReader->seekByTimestamp(timestamp, false);
    if (ec != SWIFT_NS(protocol)::ERROR_NONE
        && ec != SWIFT_NS(protocol)::ERROR_CLIENT_NO_MORE_MESSAGE)
    {
        string errorMsg = "seek failed for swift source with "
                          + StringUtil::toString(timestamp);
        REPORT_ERROR_WITH_ADVICE(READER_ERROR_SWIFT_SEEK, errorMsg, BS_RETRY);
        return false;
    }
    return true;
}

bool SwiftRawDocumentReader::isEof() const {
    return false;
}

int64_t SwiftRawDocumentReader::getFreshness() {
    int64_t currentTime;
    if (!getMaxTimestamp(currentTime)) {
        string errorMsg = "get max timestamp failed, freshness use current timestamp";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        BEEPER_INTERVAL_REPORT(20, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
        currentTime = TimeUtility::currentTime();
    }
    int64_t freshness = std::max(currentTime - _lastDocTimestamp, 0L);
    int64_t gapInSec = freshness / 1000000;
    if (gapInSec > 900) // 15 min
    {
        string errMsg = "swift raw doc reader freshness delay over 15 min, current [" +
                        StringUtil::toString(gapInSec) + "] seconds";
        BEEPER_INTERVAL_REPORT(60, WORKER_ERROR_COLLECTOR_NAME, errMsg);
    }

    int64_t interval = (gapInSec > 10) ? 60 : 300;  // gap over 10s will report every minute, otherwise 5 min
    {
        static int64_t logTimestamp;
        int64_t now = autil::TimeUtility::currentTimeInSeconds();
        if (now - logTimestamp > interval) {
            string msg = "swift raw doc reader freshness [" +
                         StringUtil::toString(gapInSec) + "] seconds";
            BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
            logTimestamp = now;
        }
    }
    return freshness;
}

void SwiftRawDocumentReader::suspendReadAtTimestamp(
    int64_t timestamp, ExceedTsAction action)
{
    autil::ScopedLock lock(_mutex);
    int64_t actualTimestamp = timestamp;
    _swiftReader->setTimestampLimit(timestamp, actualTimestamp);
    _exceedTsAction = action;
    _exceedSuspendTimestamp = false;
    BS_LOG(INFO, "swift reader will suspend at [%ld]", actualTimestamp);
    string msg = "swift reader will suspend at [" + StringUtil::toString(actualTimestamp) + "]";
    BEEPER_INTERVAL_REPORT(10, WORKER_STATUS_COLLECTOR_NAME, msg);
}

RawDocumentReader::ErrorCode SwiftRawDocumentReader::readDocStr(
        std::string &docStr, int64_t &offset, int64_t &msgTimestamp)
{
    autil::ScopedLock lock(_mutex);
    SWIFT_NS(protocol)::Message message;
    SWIFT_NS(protocol)::ErrorCode ec = _swiftReader->read(
            offset, message, DEFAULT_WAIT_READER_TIME);

    if (SWIFT_NS(protocol)::ERROR_NONE != ec) {
        if (SWIFT_NS(protocol)::ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT == ec) {
            BS_LOG(INFO, "swift read exceed the limited timestamp");
            string msg = "swift read exceed the limited timestamp, exceedTsAction:";
            if (_exceedTsAction == RawDocumentReader::ETA_SUSPEND) {
                msg += "suspend";
                BEEPER_INTERVAL_REPORT(10, WORKER_STATUS_COLLECTOR_NAME, msg);
                _exceedSuspendTimestamp = true;
                return ERROR_WAIT;
            } else {
                msg += "eof";
                BEEPER_INTERVAL_REPORT(10, WORKER_STATUS_COLLECTOR_NAME, msg);                
                return ERROR_EOF;
            }
        } else if (SWIFT_NS(protocol)::ERROR_CLIENT_NO_MORE_MESSAGE == ec) {
            return ERROR_WAIT;
        } else {
            BS_LOG(WARN, "read from swift fail [ec = %d]", ec);
            string errorMsg = "read from swift fail [ec = " + StringUtil::toString(ec) + "]";
            BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
            return ERROR_WAIT;
        }
    }

    _lastDocTimestamp = offset;
    docStr = message.data();
    msgTimestamp = message.timestamp();
    _exceedSuspendTimestamp = false;

    int64_t delay = getFreshness() / (1000 * 1000);
    REPORT_METRIC(_swiftReadDelayMetric, (double)delay);
    if (_swiftReadDelayCounter) {
        _swiftReadDelayCounter->Set(delay);
    }
    return ERROR_NONE;
}

RawDocumentReader::ErrorCode SwiftRawDocumentReader::readDocStr(std::vector<Message> &msgs,
                                                                int64_t &offset,
                                                                size_t maxMessageCount) {
    autil::ScopedLock lock(_mutex);
    SWIFT_NS(protocol)::Messages messages;
    SWIFT_NS(protocol)::ErrorCode ec =
        _swiftReader->read(offset, messages, maxMessageCount, DEFAULT_WAIT_READER_TIME);
    
    if (SWIFT_NS(protocol)::ERROR_NONE != ec) {
        if (SWIFT_NS(protocol)::ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT == ec) {
            BS_LOG(INFO, "swift read exceed the limited timestamp");
            if (_exceedTsAction == RawDocumentReader::ETA_SUSPEND) {
                _exceedSuspendTimestamp = true;
                return ERROR_WAIT;
            } else {
                return ERROR_EOF;
            }
        } else if (SWIFT_NS(protocol)::ERROR_CLIENT_NO_MORE_MESSAGE == ec) {
            return ERROR_WAIT;
        } else {
            BS_LOG(WARN, "read from swift fail [ec = %d]", ec);
            return ERROR_WAIT;
        }
    }

    _lastDocTimestamp = offset;
    msgs.clear();
    auto msgCnt = messages.msgs_size();
    msgs.reserve(msgCnt);
    for (decltype(msgCnt) i = 0; i < msgCnt; ++i) {
        msgs.emplace_back();
        auto &msg = msgs.back();
        msg.data = std::move(messages.mutable_msgs(i)->data());
        msg.timestamp = messages.msgs(i).timestamp();
    }
    _exceedSuspendTimestamp = false;

    int64_t delay = getFreshness() / (1000 * 1000);
    REPORT_METRIC(_swiftReadDelayMetric, (double)delay);
    if (_swiftReadDelayCounter) {
        _swiftReadDelayCounter->Set(delay);
    }
    return ERROR_NONE;
}

SwiftClient *SwiftRawDocumentReader::createSwiftClient(
        const string& zkPath,
        const string& swiftClientConfig) {
    return _swiftClientCreator->createSwiftClient(zkPath, swiftClientConfig);
}

SwiftReaderWrapper *SwiftRawDocumentReader::createSwiftReader(
    const string &swiftReaderConfig, SWIFT_NS(protocol)::ErrorInfo *errorInfo) {
    auto reader = _swiftClient->createReader(swiftReaderConfig, errorInfo);
    if (reader) {
        return new SwiftReaderWrapper(reader);
    }
    return nullptr;
}

bool SwiftRawDocumentReader::getMaxTimestampAfterStartTimestamp(int64_t &timestamp) {
    int64_t maxTimestamp;
    if (!getMaxTimestamp(maxTimestamp)) {
        return false;
    }
    timestamp = maxTimestamp >= _startTimestamp ? maxTimestamp : -1;
    return true;
}

bool SwiftRawDocumentReader::getMaxTimestamp(int64_t &timestamp) {
    SWIFT_NS(client)::SwiftPartitionStatus status =
        _swiftReader->getSwiftPartitionStatus();

    if (TimeUtility::currentTime() - status.refreshTime
        > CONNECT_TIMEOUT_INTERVAL)
    {
        stringstream ss;
        ss << "currentTime: " << TimeUtility::currentTime() << " - refreshTime: "
           << status.refreshTime << " greater than connect timeout interval";
        string errorMsg = ss.str();
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    
    if (status.maxMessageId < 0) {
        timestamp = -1;
    } else {
        timestamp = status.maxMessageTimestamp;
    }
    return true;
}

}
}
