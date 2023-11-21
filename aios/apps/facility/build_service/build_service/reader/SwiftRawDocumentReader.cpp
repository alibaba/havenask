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
#include "build_service/reader/SwiftRawDocumentReader.h"

#include "autil/EnvUtil.h"
#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "beeper/beeper.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/document/ProcessedDocument.h"
#include "build_service/reader/SwiftSchemaBasedRawDocumentParser.h"
#include "build_service/util/LocatorUtil.h"
#include "build_service/util/Monitor.h"
#include "build_service/util/RangeUtil.h"
#include "indexlib/util/counter/StateCounter.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"

using namespace std;
using namespace autil;

using namespace swift::client;
using namespace swift::network;
using namespace swift::protocol;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::common;

namespace build_service { namespace reader {

BS_LOG_SETUP(reader, SwiftRawDocumentReader);

SwiftRawDocumentReader::SwiftRawDocumentReader(const util::SwiftClientCreatorPtr& swiftClientCreator)
    : _swiftReader(NULL)
    , _swiftClient(NULL)
    , _lastDocTimestamp(0)
    , _noMoreMsgStartTs(-1)
    , _swiftReadDelayMetric(NULL)
    , _swiftClientCreator(swiftClientCreator)
    , _startTimestamp(0)
    , _exceedTsAction(common::ETA_STOP)
    , _exceedSuspendTimestamp(false)
    , _isEof(false)
    , _enableSchema(false)
    , _enableRawTopicFastSlowQueue(false)
    , _lastMsgTimestamp(0)
    , _lastMsgId(0)
    , _lastDocU16Payload(0)
{
}

SwiftRawDocumentReader::~SwiftRawDocumentReader()
{
    _swiftLinkReporter.reset();
    DELETE_AND_SET_NULL(_swiftReader);
    _swiftClient = NULL;
}

bool SwiftRawDocumentReader::init(const ReaderInitParam& params)
{
    if (!RawDocumentReader::init(params)) {
        return false;
    }

    initMetrics(params);

    if (!createSwiftClient(params)) {
        return false;
    }

    _swiftReader = createSwiftReader(params);
    if (!_swiftReader) {
        return false;
    }

    if (!_swiftReader->init()) {
        stringstream ss;
        ss << "reader init failed" << std::endl;
        REPORT_ERROR_WITH_ADVICE(READER_ERROR_SWIFT_CREATE_READER, ss.str(), BS_STOP);
        return false;
    }

    SwiftSchemaBasedRawDocumentParser* schemaParser = dynamic_cast<SwiftSchemaBasedRawDocumentParser*>(_parser);
    if (schemaParser) {
        _enableSchema = true;
        schemaParser->setSwiftReader(_swiftReader->getSwiftReader());
    }

    if (!initSwiftReader(_swiftReader, params.kvMap)) {
        return false;
    }

    if (!initSwiftLinkReporter(params)) {
        string topicName = getValueFromKeyValueMap(params.kvMap, SWIFT_TOPIC_NAME);
        string msg = "create swift link reporter for topic [" + topicName + "] failed";
        BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, msg);
    }
    return true;
}

bool SwiftRawDocumentReader::initMetrics(const ReaderInitParam& params)
{
    _swiftReadDelayMetric = DECLARE_METRIC(params.metricProvider, "basic/swiftReadDelay", kmonitor::GAUGE, "s");
    _msgTimeToCurDelayMetric = DECLARE_METRIC(params.metricProvider, "swift/msgTimeToCurDelay", kmonitor::GAUGE, "ms");
    _swiftReadDelayCounter = GET_STATE_COUNTER(params.counterMap, processor.swiftReadDelay);
    if (!_swiftReadDelayCounter) {
        BS_LOG(ERROR, "can not get swiftReadDelay from counterMap");
        return false;
    } else {
        return true;
    }
}

bool SwiftRawDocumentReader::createSwiftClient(const ReaderInitParam& params)
{
    string zfsRoot = getValueFromKeyValueMap(params.kvMap, SWIFT_ZOOKEEPER_ROOT);
    string swiftClientConfig = getValueFromKeyValueMap(params.kvMap, SWIFT_CLIENT_CONFIG);
    swiftClientConfig = EnvUtil::getEnv("raw_topic_swift_client_config", swiftClientConfig);
    BS_LOG(INFO, "swift raw topic use client config [%s]", swiftClientConfig.c_str());
    _swiftClient = createSwiftClient(zfsRoot, swiftClientConfig);
    return _swiftClient != nullptr;
}

SwiftReaderWrapper* SwiftRawDocumentReader::createSwiftReader(const ReaderInitParam& params)
{
    string swiftReaderConfig = createSwiftReaderConfig(params);
    if (swiftReaderConfig.empty()) {
        IE_LOG(ERROR, "create swift reader config failed");
        return nullptr;
    }
    BS_LOG(INFO, "create swift reader with config %s", swiftReaderConfig.c_str());
    swift::protocol::ErrorInfo errorInfo;
    auto swiftReader = createSwiftReader(swiftReaderConfig, &errorInfo);
    if (!swiftReader) {
        stringstream ss;
        ss << "create reader failed, config[" << swiftReaderConfig << "]" << endl;
        REPORT_ERROR_WITH_ADVICE(READER_ERROR_SWIFT_CREATE_READER, ss.str(), BS_STOP);
        return nullptr;
    }
    return swiftReader;
}

string SwiftRawDocumentReader::createSwiftReaderConfig(const ReaderInitParam& params)
{
    string topicName = getValueFromKeyValueMap(params.kvMap, SWIFT_TOPIC_NAME);
    string readerConfig = getValueFromKeyValueMap(params.kvMap, SWIFT_READER_CONFIG);
    string filterConfig = getValueFromKeyValueMap(params.kvMap, RAW_TOPIC_SWIFT_READER_CONFIG);
    string rawTopicFastSlowMask = getValueFromKeyValueMap(params.kvMap, RAW_TOPIC_FAST_SLOW_QUEUE_MASK);
    string rawTopicFastSlowResult = getValueFromKeyValueMap(params.kvMap, RAW_TOPIC_FAST_SLOW_QUEUE_RESULT);

    string tmp;
    bool isMultiTopic = (topicName.find("#") != std::string::npos);
    _enableRawTopicFastSlowQueue = !rawTopicFastSlowMask.empty() && !rawTopicFastSlowResult.empty();

    string innerBatchMaskStr = getValueFromKeyValueMap(params.kvMap, USE_INNER_BATCH_MASK_FILTER);
    bool useBSInnerMaskFilter = (innerBatchMaskStr == "true");
    if (useBSInnerMaskFilter) {
        BS_LOG(ERROR, "not support use_inner_batch_mask_filter = true");
        return "";
    }

    std::vector<std::pair<uint8_t, uint8_t>> maskPairs;
    auto addMaskPair = [&](const char* maskStr, const char* resultStr) {
        uint8_t mask = 0, result = 0;
        bool convertResult = autil::StringUtil::strToUInt8(maskStr, mask);

        if (!convertResult) {
            BS_LOG(ERROR, "convert mask [%s] failed", maskStr);
            return false;
        }
        convertResult = autil::StringUtil::strToUInt8(resultStr, result);
        if (!convertResult) {
            BS_LOG(ERROR, "convert mask result [%s] failed", resultStr);
            return false;
        }
        maskPairs.push_back(std::make_pair(mask, result));
        return true;
    };

    if (autil::EnvUtil::getEnvWithoutDefault("raw_topic_swift_reader_config", tmp)) {
        readerConfig = tmp;
    }
    BS_LOG(INFO, "swift raw topic use reader config [%s], enable FastSlowQueue[%s]", readerConfig.c_str(),
           _enableRawTopicFastSlowQueue ? "true" : "false");
    stringstream ss;
    ss << "topicName=" << topicName << ";";
    if (!readerConfig.empty()) {
        ss << readerConfig << ";";
    }
    ss << "from=" << params.range.from() << ";"
       << "to=" << params.range.to() << ";";
    if (!filterConfig.empty()) {
        if (useBSInnerMaskFilter) {
            ss << "uint8FilterMask=0;uint8MaskResult=0;";
        } else {
            ss << filterConfig << ";";
        }

        string mask;
        string result;
        StringTokenizer st(filterConfig, ";", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
        for (auto it = st.begin(); it != st.end(); it++) {
            StringTokenizer st2(*it, "=", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
            if (st2.getNumTokens() != 2) {
                AUTIL_LOG(ERROR, "Invalid filter config item: [%s]", (*it).c_str());
                return "";
            }

            string key = st2[0];
            string valueStr = st2[1];

            if (key == "uint8FilterMask") {
                mask = valueStr;
            }

            if (key == "uint8MaskResult") {
                result = valueStr;
            }
        }
        if (!addMaskPair(mask.c_str(), result.c_str())) {
            return "";
        }
        if (_enableRawTopicFastSlowQueue) {
            BS_LOG(ERROR, "filter_config conflict with raw topic fast_slow_queue");
            return "";
        }
    } else if (_enableRawTopicFastSlowQueue) {
        ss << "uint8FilterMask=" << rawTopicFastSlowMask.c_str() << ";"
           << "uint8MaskResult=0;";
        if (!addMaskPair(rawTopicFastSlowMask.c_str(), "0")) {
            return "";
        }

        string zfsRoot = getValueFromKeyValueMap(params.kvMap, SWIFT_ZOOKEEPER_ROOT);
        if (zfsRoot.empty()) {
            BS_LOG(ERROR, "create swift reader failed, no zkPath");
            return "";
        }
        ss << "zkPath=" << zfsRoot << ";"
           << "multiReaderOrder=sequence##"
           // multiReader for fast-slow-queue.
           << "topicName=" << topicName << ";"
           << "zkPath=" << zfsRoot << ";";
        if (!readerConfig.empty()) {
            ss << readerConfig << ";";
        }
        ss << "from=" << params.range.from() << ";"
           << "to=" << params.range.to() << ";";
        ss << "uint8FilterMask=" << rawTopicFastSlowMask.c_str() << ";"
           << "uint8MaskResult=" << rawTopicFastSlowResult.c_str() << ";";
        if (!addMaskPair(rawTopicFastSlowMask.c_str(), rawTopicFastSlowResult.c_str())) {
            return "";
        }
    }
    if (maskPairs.empty()) {
        maskPairs.push_back(std::make_pair(0, 0));
    }
    _swiftParam = common::SwiftParam(nullptr, nullptr, topicName, isMultiTopic, maskPairs, params.range.from(),
                                     params.range.to(), useBSInnerMaskFilter);
    return ss.str();
}

bool SwiftRawDocumentReader::initSwiftReader(SwiftReaderWrapper* swiftReader, const KeyValueMap& kvMap)
{
    int64_t startTimestamp = 0;
    KeyValueMap::const_iterator it = kvMap.find(SWIFT_START_TIMESTAMP);
    if (it != kvMap.end() && !StringUtil::fromString<int64_t>(it->second, startTimestamp)) {
        BS_LOG(ERROR, "invalid startTimestamp[%s]", it->second.c_str());
        return false;
    }

    auto it1 = kvMap.find(SWIFT_SUSPEND_TIMESTAMP);
    auto it2 = kvMap.find(SWIFT_STOP_TIMESTAMP);

    if (it1 != kvMap.end() && it2 != kvMap.end()) {
        BS_LOG(ERROR, "cannot specify %s and %s at the same time", SWIFT_SUSPEND_TIMESTAMP.c_str(),
               SWIFT_STOP_TIMESTAMP.c_str());
        return false;
    }

    int64_t limitTimestamp = 0;
    if (it1 != kvMap.end() && !StringUtil::fromString<int64_t>(it1->second, limitTimestamp)) {
        string msg = "invalid " + SWIFT_SUSPEND_TIMESTAMP + "[" + it1->second + "]";
        BS_LOG(ERROR, "%s", msg.c_str());
        BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, msg);
        return false;
    }
    if (it2 != kvMap.end() && !StringUtil::fromString<int64_t>(it2->second, limitTimestamp)) {
        string msg = "invalid " + SWIFT_STOP_TIMESTAMP + "[" + it2->second + "]";
        BS_LOG(ERROR, "%s", msg.c_str());
        BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, msg);
        return false;
    }

    int64_t checkpoint = 0;
    it = kvMap.find(CHECKPOINT);
    if (it != kvMap.end() && !StringUtil::fromString<int64_t>(it->second, checkpoint)) {
        BS_LOG(ERROR, "invalid checkpoint[%s]", it->second.c_str());
        return false;
    }
    if (startTimestamp < limitTimestamp && limitTimestamp < checkpoint) {
        startTimestamp = std::max(startTimestamp, limitTimestamp);
    } else {
        startTimestamp = std::max(startTimestamp, checkpoint);
    }

    indexlibv2::base::ProgressVector progress;
    progress.push_back({_swiftParam.from, _swiftParam.to, {startTimestamp, 0}});
    if (startTimestamp > 0 && !seek(Checkpoint(/*offset=*/startTimestamp, /*progress=*/ {progress}, /*userData=*/""))) {
        BS_LOG(ERROR, "swift reader seek to %ld failed", startTimestamp);
        return false;
    }

    if (startTimestamp > 0) {
        _startTimestamp = startTimestamp;
        BS_LOG(INFO, "swift reader seek to [%ld] at startup", _startTimestamp);
    }

    return setLimitTime(swiftReader, kvMap, SWIFT_SUSPEND_TIMESTAMP) &&
           setLimitTime(swiftReader, kvMap, SWIFT_STOP_TIMESTAMP);
}

bool SwiftRawDocumentReader::setLimitTime(SwiftReaderWrapper* swiftReader, const KeyValueMap& kvMap,
                                          const string& limitTimeType)
{
    auto it = kvMap.find(limitTimeType);
    string limitTimeStr = "";
    if (it != kvMap.end()) {
        limitTimeStr = it->second;
    } else {
        limitTimeStr = EnvUtil::getEnv(limitTimeType, limitTimeStr);
    }
    if (limitTimeStr.empty()) {
        return true;
    }

    string msg;
    int64_t limitTime;
    if (!StringUtil::fromString(limitTimeStr, limitTime)) {
        BS_LOG(ERROR, "invalid %s[%s]", limitTimeType.c_str(), limitTimeStr.c_str());
        msg = "invalid " + limitTimeType + "[" + limitTimeStr + "]";
        BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, msg);
        return false;
    }

    if (_startTimestamp > limitTime) {
        BS_LOG(ERROR, "startTimestamp[%ld] > %s[%ld]", _startTimestamp, limitTimeType.c_str(), limitTime);
        msg = "startTimestamp[" + StringUtil::toString(_startTimestamp) + "] > " + limitTimeType + "[" +
              StringUtil::toString(limitTime) + "]";
        BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, msg);
        return false;
    }

    if (limitTime != numeric_limits<int64_t>::max()) {
        swiftReader->setTimestampLimit(limitTime, limitTime);
        BS_LOG(INFO, "swift reader will suspend at timestamp[%ld]", limitTime);
        msg = "swift reader will suspend at timestamp[" + StringUtil::toString(limitTime) + "]";
        BEEPER_INTERVAL_REPORT(10, WORKER_STATUS_COLLECTOR_NAME, msg);
    }
    _exceedTsAction = (limitTimeType == SWIFT_SUSPEND_TIMESTAMP) ? common::ETA_SUSPEND : common::ETA_STOP;
    return true;
}

bool SwiftRawDocumentReader::seek(const Checkpoint& checkpoint)
{
    const auto& topicName = checkpoint.userData.empty() ? _swiftParam.topicName : checkpoint.userData;
    return doSeek(checkpoint.offset, checkpoint.progress, topicName);
}

bool SwiftRawDocumentReader::doSeek(int64_t timestamp, const indexlibv2::base::MultiProgress& progress,
                                    const std::string& topicName)
{
    assert(_swiftReader);
    BS_LOG(INFO, "swift seek timestamp[%ld]", timestamp);
    string msg = "swift seek timestamp [" + StringUtil::toString(timestamp) + "]";
    BEEPER_INTERVAL_REPORT(10, WORKER_STATUS_COLLECTOR_NAME, msg);

    if (!_swiftParam.isMultiTopic) {
        assert(progress.size() == 1);
        swift::protocol::ReaderProgress swiftProgress = util::LocatorUtil::convertLocatorProgress(
            progress[0], topicName, _swiftParam.maskFilterPairs, _swiftParam.disableSwiftMaskFilter);
        swift::protocol::ErrorCode ec = _swiftReader->seekByProgress(swiftProgress, false);
        if (ec != swift::protocol::ERROR_NONE && ec != swift::protocol::ERROR_CLIENT_NO_MORE_MESSAGE) {
            string errorMsg = "seek by progress failed for swift source with " + StringUtil::toString(timestamp);
            REPORT_ERROR_WITH_ADVICE(READER_ERROR_SWIFT_SEEK, errorMsg, BS_RETRY);
            return false;
        }
        _messageFilter.setSeekProgress(progress[0]);
    } else {
        swift::protocol::ErrorCode ec = _swiftReader->seekByTimestamp(timestamp, false);
        if (ec != swift::protocol::ERROR_NONE && ec != swift::protocol::ERROR_CLIENT_NO_MORE_MESSAGE) {
            string errorMsg = "seek by timestamp failed for swift source with " + StringUtil::toString(timestamp);
            REPORT_ERROR_WITH_ADVICE(READER_ERROR_SWIFT_SEEK, errorMsg, BS_RETRY);
            return false;
        }
    }
    return true;
}

bool SwiftRawDocumentReader::isEof() const { return _isEof; }

int64_t SwiftRawDocumentReader::getFreshness()
{
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
        string errMsg = "swift raw doc reader freshness delay over 15 min, current [" + StringUtil::toString(gapInSec) +
                        "] seconds";
        BEEPER_INTERVAL_REPORT(60, WORKER_ERROR_COLLECTOR_NAME, errMsg);
    }

    int64_t interval = (gapInSec > 10) ? 60 : 300; // gap over 10s will report every minute, otherwise 5 min
    {
        static int64_t logTimestamp;
        int64_t now = autil::TimeUtility::currentTimeInSeconds();
        if (now - logTimestamp > interval) {
            string msg = "swift raw doc reader freshness [" + StringUtil::toString(gapInSec) + "] seconds";
            BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
            logTimestamp = now;
        }
    }
    return freshness;
}

void SwiftRawDocumentReader::suspendReadAtTimestamp(int64_t timestamp, common::ExceedTsAction action)
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

RawDocumentReader::ErrorCode SwiftRawDocumentReader::readDocStr(std::string& docStr, Checkpoint* checkpoint,
                                                                DocInfo& docInfo)
{
    autil::ScopedLock lock(_mutex);
    swift::protocol::Message message;
    swift::protocol::ReaderProgress readerProgress;
    swift::protocol::ErrorCode ec =
        _swiftReader->read(DEFAULT_WAIT_READER_TIME, checkpoint->offset, message, readerProgress);

    if (swift::protocol::ERROR_NONE != ec) {
        if (swift::protocol::ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT == ec) {
            BS_LOG(INFO, "swift read exceed the limited timestamp");
            string msg = "swift read exceed the limited timestamp, exceedTsAction:";
            if (_exceedTsAction == common::ETA_SUSPEND) {
                msg += "suspend";
                BEEPER_INTERVAL_REPORT(10, WORKER_STATUS_COLLECTOR_NAME, msg);
                _exceedSuspendTimestamp = true;
                return ERROR_WAIT;
            } else {
                _isEof = true;
                msg += "eof";
                BEEPER_INTERVAL_REPORT(10, WORKER_STATUS_COLLECTOR_NAME, msg);
                return ERROR_EOF;
            }
        } else if (swift::protocol::ERROR_SEALED_TOPIC_READ_FINISH == ec) {
            BS_LOG(INFO, "swift sealed topic read finish");
            string msg = "swift sealed topic read finish";
            _isEof = true;
            BEEPER_INTERVAL_REPORT(10, WORKER_STATUS_COLLECTOR_NAME, msg);
            return ERROR_SEALED;
        } else if (swift::protocol::ERROR_CLIENT_NO_MORE_MESSAGE == ec) {
            int64_t now = TimeUtility::currentTime();
            if (_noMoreMsgStartTs == -1) {
                _noMoreMsgStartTs = now;
            } else {
                int64_t gapTsInSeconds = (now - _noMoreMsgStartTs) / 1000000;
                if (gapTsInSeconds > 60) {
                    static int64_t logTs;
                    if (now - logTs > 300000000) { // 300s = 5min
                        logTs = now;
                        BS_LOG(INFO,
                               "rawDoc swift reader return SWIFT_CLIENT_NO_MORE_MSG"
                               " last over [%ld] seconds, lastReadTs [%ld]",
                               gapTsInSeconds, checkpoint->offset);
                        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(WORKER_ERROR_COLLECTOR_NAME,
                                                          "rawDoc swift reader return SWIFT_CLIENT_NO_MORE_MSG"
                                                          " last over [%ld] seconds, lastReadTs [%ld]",
                                                          gapTsInSeconds, checkpoint->offset);
                    }
                }
            }
            return ERROR_WAIT;
        } else {
            BS_LOG(WARN, "read from swift fail [ec = %d]", ec);
            string errorMsg = "read from swift fail [ec = " + StringUtil::toString(ec) + "]";
            BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
            return ERROR_WAIT;
        }
    }
    _lastDocTimestamp = checkpoint->offset;
    _lastDocU16Payload = message.uint16payload();
    docInfo.timestamp = message.timestamp();
    docInfo.hashId = _lastDocU16Payload;
    docInfo.concurrentIdx = message.offsetinrawmsg();
    _noMoreMsgStartTs = -1;

    auto [success, progress] = util::LocatorUtil::convertSwiftProgress(
        readerProgress, _swiftParam.isMultiTopic || _enableRawTopicFastSlowQueue);
    if (!success) {
        BS_LOG(ERROR, "convert swift progress failed [%s]", readerProgress.ShortDebugString().c_str());
        return ERROR_EXCEPTION;
    }
    if (_messageFilter.filterOrRewriteProgress(docInfo.hashId, {docInfo.timestamp, docInfo.concurrentIdx}, &progress)) {
        BS_INTERVAL_LOG2(10, WARN, "filter message payload [%u] timestamp [%ld] progress [%s]", docInfo.hashId,
                         docInfo.timestamp, readerProgress.ShortDebugString().c_str());
        return ERROR_SKIP;
    }
    checkpoint->progress = {progress};
    if (!_swiftParam.isMultiTopic) {
        // TODO: assert topic equal
        if (readerProgress.topicprogress_size() > 0) {
            checkpoint->userData = readerProgress.topicprogress(0).topicname();
        } else {
            checkpoint->userData = _swiftParam.topicName;
        }
    }
    if (_enableSchema) {
        encodeSchemaDocString(message, docStr);
    } else {
        docStr = message.data();
    }
    _lastMsgTimestamp = docInfo.timestamp;
    _lastMsgId = message.msgid();
    _exceedSuspendTimestamp = false;

    if (_swiftLinkReporter) {
        _swiftLinkReporter->collectSwiftMessageMeta(message);
        _swiftLinkReporter->collectSwiftMessageOffset(checkpoint->offset);
    }
    int64_t freshness = getFreshness();
    double delay = (double)freshness / 1000 / 1000;
    int64_t msgDeley = TimeUtility::currentTime() - message.timestamp();
    REPORT_METRIC(_swiftReadDelayMetric, delay);
    REPORT_METRIC(_msgTimeToCurDelayMetric, (double)msgDeley / 1000);
    if (_swiftReadDelayCounter) {
        _swiftReadDelayCounter->Set(delay);
    }
    return ERROR_NONE;
}

RawDocumentReader::ErrorCode SwiftRawDocumentReader::readDocStr(std::vector<Message>& msgs, Checkpoint* checkpoint,
                                                                size_t maxMessageCount)
{
    autil::ScopedLock lock(_mutex);
    swift::protocol::Messages messages;
    swift::protocol::ReaderProgress readerProgress;
    swift::protocol::ErrorCode ec =
        _swiftReader->read(maxMessageCount, DEFAULT_WAIT_READER_TIME, checkpoint->offset, messages, readerProgress);

    if (swift::protocol::ERROR_NONE != ec) {
        if (swift::protocol::ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT == ec) {
            BS_LOG(INFO, "swift read exceed the limited timestamp");
            string msg = "swift read exceed the limited timestamp, exceedTsAction:";
            if (_exceedTsAction == common::ETA_SUSPEND) {
                msg += "suspend";
                BEEPER_INTERVAL_REPORT(10, WORKER_STATUS_COLLECTOR_NAME, msg);
                _exceedSuspendTimestamp = true;
                return ERROR_WAIT;
            } else {
                msg += "eof";
                BEEPER_INTERVAL_REPORT(10, WORKER_STATUS_COLLECTOR_NAME, msg);
                return ERROR_EOF;
            }
        } else if (swift::protocol::ERROR_SEALED_TOPIC_READ_FINISH == ec) {
            BS_LOG(INFO, "swift sealed topic read finish");
            string msg = "swift sealed topic read finish";
            _isEof = true;
            BEEPER_INTERVAL_REPORT(10, WORKER_STATUS_COLLECTOR_NAME, msg);
            return ERROR_SEALED;
        } else if (swift::protocol::ERROR_CLIENT_NO_MORE_MESSAGE == ec) {
            int64_t now = TimeUtility::currentTime();
            if (_noMoreMsgStartTs == -1) {
                _noMoreMsgStartTs = now;
            } else {
                int64_t gapTsInSeconds = (now - _noMoreMsgStartTs) / 1000000;
                if (gapTsInSeconds > 60) {
                    static int64_t logTs;
                    if (now - logTs > 300000000) { // 300s = 5min
                        logTs = now;
                        BS_LOG(INFO,
                               "rawDoc swift reader return SWIFT_CLIENT_NO_MORE_MSG"
                               " last over [%ld] seconds, lastReadTs [%ld]",
                               gapTsInSeconds, checkpoint->offset);
                        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(WORKER_ERROR_COLLECTOR_NAME,
                                                          "rawDoc swift reader return SWIFT_CLIENT_NO_MORE_MSG"
                                                          " last over [%ld] seconds, lastReadTs [%ld]",
                                                          gapTsInSeconds, checkpoint->offset);
                    }
                }
            }
            return ERROR_WAIT;
        } else {
            BS_LOG(WARN, "read from swift fail [ec = %d]", ec);
            string errorMsg = "read from swift fail [ec = " + StringUtil::toString(ec) + "]";
            BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
            return ERROR_WAIT;
        }
    }

    _lastDocTimestamp = checkpoint->offset;
    _noMoreMsgStartTs = -1;
    auto [success, progress] = util::LocatorUtil::convertSwiftProgress(
        readerProgress, _swiftParam.isMultiTopic || _enableRawTopicFastSlowQueue);
    if (!success) {
        AUTIL_LOG(ERROR, "convert swift progress failed [%s]", readerProgress.ShortDebugString().c_str());
        return ERROR_EXCEPTION;
    }
    checkpoint->progress = {progress};

    if (!_swiftParam.isMultiTopic) {
        if (readerProgress.topicprogress_size() > 0) {
            checkpoint->userData = readerProgress.topicprogress(0).topicname();
        } else {
            checkpoint->userData = _swiftParam.topicName;
        }
    }

    msgs.clear();
    auto msgCnt = messages.msgs_size();
    msgs.reserve(msgCnt);
    int64_t msgMaxTimestamp = 0;
    int64_t msgMinTimestamp = std::numeric_limits<int64_t>::max();
    for (decltype(msgCnt) i = 0; i < msgCnt; ++i) {
        msgs.emplace_back();
        auto& msg = msgs.back();
        if (_enableSchema) {
            encodeSchemaDocString(*messages.mutable_msgs(i), msg.data);
        } else {
            msg.data = std::move(messages.mutable_msgs(i)->data());
        }
        msg.timestamp = messages.msgs(i).timestamp();
        msg.hashId = messages.msgs(i).uint16payload();
        if (_swiftLinkReporter) {
            _swiftLinkReporter->collectSwiftMessageMeta(messages.msgs(i));
        }
        msgMaxTimestamp = msgMaxTimestamp > msg.timestamp ? msgMaxTimestamp : msg.timestamp;
        msgMinTimestamp = msgMinTimestamp < msg.timestamp ? msgMinTimestamp : msg.timestamp;
    }
    if (msgCnt != 0) {
        _lastDocU16Payload = messages.msgs(msgCnt - 1).uint16payload();
    }

    if (_swiftLinkReporter) {
        _swiftLinkReporter->collectSwiftMessageOffset(checkpoint->offset);
    }
    _exceedSuspendTimestamp = false;

    int64_t freshness = getFreshness();
    double delay = (double)freshness / 1000 / 1000;
    REPORT_METRIC(_swiftReadDelayMetric, delay);
    if (msgCnt > 0) {
        REPORT_METRIC(_msgTimeToCurDelayMetric, (TimeUtility::currentTime() - msgMaxTimestamp) / 1000);
        REPORT_METRIC(_msgTimeToCurDelayMetric, (TimeUtility::currentTime() - msgMinTimestamp) / 1000);
    }
    if (_swiftReadDelayCounter) {
        _swiftReadDelayCounter->Set(delay);
    }
    return ERROR_NONE;
}

SwiftClient* SwiftRawDocumentReader::createSwiftClient(const string& zkPath, const string& swiftClientConfig)
{
    return _swiftClientCreator->createSwiftClient(zkPath, swiftClientConfig);
}

SwiftReaderWrapper* SwiftRawDocumentReader::createSwiftReader(const string& swiftReaderConfig,
                                                              swift::protocol::ErrorInfo* errorInfo)
{
    auto reader = _swiftClient->createReader(swiftReaderConfig, errorInfo);
    if (reader) {
        return new SwiftReaderWrapper(reader);
    }
    return nullptr;
}

bool SwiftRawDocumentReader::getMaxTimestampAfterStartTimestamp(int64_t& timestamp)
{
    int64_t maxTimestamp;
    if (!getMaxTimestamp(maxTimestamp)) {
        return false;
    }
    timestamp = maxTimestamp >= _startTimestamp ? maxTimestamp : -1;
    return true;
}

bool SwiftRawDocumentReader::getMaxTimestamp(int64_t& timestamp)
{
    swift::client::SwiftPartitionStatus status = _swiftReader->getSwiftPartitionStatus();

    if (TimeUtility::currentTime() - status.refreshTime > CONNECT_TIMEOUT_INTERVAL) {
        stringstream ss;
        ss << "currentTime: " << TimeUtility::currentTime() << " - refreshTime: " << status.refreshTime
           << " greater than connect timeout interval";
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

bool SwiftRawDocumentReader::initSwiftLinkReporter(const ReaderInitParam& params)
{
    if (!SwiftLinkFreshnessReporter::needReport()) {
        return true;
    }

    SwiftAdminAdapterPtr adapter = _swiftClient->getAdminAdapter();
    if (adapter.get() == nullptr) {
        BS_LOG(ERROR, "initSwiftLinkReporter fail, no valid adapter.");
        return false;
    }
    string topicName = getValueFromKeyValueMap(params.kvMap, SWIFT_TOPIC_NAME);
    int64_t totalSwiftPartitionCount = -1;
    if (!util::RangeUtil::getPartitionCount(adapter.get(), topicName, &totalSwiftPartitionCount)) {
        BS_LOG(ERROR, "failed to get swift partition count for topic [%s]", topicName.c_str());
        return false;
    }
    _swiftLinkReporter.reset(new SwiftLinkFreshnessReporter(params.partitionId));
    return _swiftLinkReporter->init(params.metricProvider, totalSwiftPartitionCount, topicName);
}

void SwiftRawDocumentReader::reportTimestampFieldValue(int64_t value)
{
    if (_swiftLinkReporter) {
        _swiftLinkReporter->collectTimestampFieldValue(value, _lastDocU16Payload);
    }
}

void SwiftRawDocumentReader::encodeSchemaDocString(const swift::protocol::Message& msg, string& docStr)
{
    // 4byte type
    docStr.resize(msg.data().size() + sizeof(uint32_t));
    char* data = (char*)docStr.data();
    *(uint32_t*)data = (uint32_t)msg.datatype();
    memcpy(data + sizeof(uint32_t), msg.data().data(), msg.data().size());
}

void SwiftRawDocumentReader::doFillDocTags(document::RawDocument& rawDoc)
{
    rawDoc.AddTag("swift_hashId", StringUtil::toString(_lastDocU16Payload));
    rawDoc.AddTag("swift_msgTime", StringUtil::toString(_lastMsgTimestamp));
    rawDoc.AddTag("swift_msgId", StringUtil::toString(_lastMsgId));
    rawDoc.AddTag("swift_offset", StringUtil::toString(_lastDocTimestamp));
}

}} // namespace build_service::reader
