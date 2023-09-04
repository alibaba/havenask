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
#include "swift/client/SwiftWriterConfig.h"

#include <cstdint>
#include <iosfwd>
#include <limits>
#include <vector>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"

using namespace std;
using namespace autil;

namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, SwiftWriterConfig);

const uint32_t SwiftWriterConfig::DEFAULT_RETRY_TIMES = 3;
const uint64_t SwiftWriterConfig::DEFAULT_RETRY_TIME_INTERVAL = 1000 * 1000;                           // 1s
const uint64_t SwiftWriterConfig::DEFAULT_REQUEST_SEND_BYTE = 128 * 1024;                              // 128KB
const uint64_t SwiftWriterConfig::DEFAULT_MAX_BUFFER_BYTE = 512 * 1024 * 1024;                         // 512M
const uint64_t SwiftWriterConfig::DEFAULT_MAX_KEEP_IN_BUFFER_TIME = 1000 * 1000;                       // 1s
const uint64_t SwiftWriterConfig::SEND_REQUEST_LOOP_INTERVAL = 10 * 1000;                              // 10ms
const uint64_t SwiftWriterConfig::DEFAULT_WAIT_LOOP_INTERVAL = 10 * 1000;                              // 10ms
const uint64_t SwiftWriterConfig::BROKER_BUSY_WAIT_INTERVAL_MIN = 1000 * 1000;                         // 1s
const uint64_t SwiftWriterConfig::BROKER_BUSY_WAIT_INTERVAL_MAX = 20 * 1000 * 1000;                    // 20s
const uint64_t SwiftWriterConfig::DEFAULT_WAIT_FINISHED_WRITER_TIME = 3 * 1000 * 1000;                 // 3s
const uint64_t SwiftWriterConfig::DEFAULT_BROKER_COMMIT_PROGRESS_DETECTION_INTERVAL = 1 * 1000 * 1000; // 1s
const uint64_t SwiftWriterConfig::DEFAULT_COMPRESS_THRESHOLD_IN_BYTES = numeric_limits<uint64_t>::max();
const uint64_t SwiftWriterConfig::DEFAULT_SYNC_TIMEOUT_INTERVAL = 3 * 1000;   // 3s
const uint64_t SwiftWriterConfig::DEFAULT_ASYNC_TIMEOUT_INTERVAL = 30 * 1000; // 30s
const uint32_t SwiftWriterConfig::DEFAULT_MESSAGE_FORMAT = 1;                 // FB_FORMAT
const uint64_t SwiftWriterConfig::DEFAULT_MERGE_THRESHOLD_IN_COUNT = 64;
const uint64_t SwiftWriterConfig::MAX_MERGE_THRESHOLD_IN_COUNT = 1023;
const uint64_t SwiftWriterConfig::DEFAULT_MERGE_THRESHOLD_IN_SIZE = 512 * 1024;           // 512kb
const int32_t SwiftWriterConfig::DEFAULT_SCHEMA_VERSION = numeric_limits<int32_t>::max(); // invalid schema

SwiftWriterConfig::SwiftWriterConfig()
    : isSynchronous(false)
    , retryTimes(DEFAULT_RETRY_TIMES)
    , retryTimeInterval(DEFAULT_RETRY_TIME_INTERVAL)
    , oneRequestSendByteCount(DEFAULT_REQUEST_SEND_BYTE)
    , maxBufferSize(DEFAULT_MAX_BUFFER_BYTE)
    , maxKeepInBufferTime(DEFAULT_MAX_KEEP_IN_BUFFER_TIME)
    , sendRequestLoopInterval(SEND_REQUEST_LOOP_INTERVAL)
    , waitLoopInterval(DEFAULT_WAIT_LOOP_INTERVAL)
    , brokerBusyWaitIntervalMin(BROKER_BUSY_WAIT_INTERVAL_MIN)
    , brokerBusyWaitIntervalMax(BROKER_BUSY_WAIT_INTERVAL_MAX)
    , waitFinishedWriterTime(DEFAULT_WAIT_FINISHED_WRITER_TIME)
    , compressThresholdInBytes(DEFAULT_COMPRESS_THRESHOLD_IN_BYTES)
    , commitDetectionInterval(DEFAULT_BROKER_COMMIT_PROGRESS_DETECTION_INTERVAL)
    , compress(false)
    , compressMsg(false)
    , compressMsgInBroker(false)
    , safeMode(true)
    , syncSendTimeout(DEFAULT_SYNC_TIMEOUT_INTERVAL)
    , asyncSendTimeout(DEFAULT_ASYNC_TIMEOUT_INTERVAL)
    , messageFormat(DEFAULT_MESSAGE_FORMAT)
    , mergeMsg(false)
    , needTimestamp(false)
    , mergeThresholdInCount(DEFAULT_MERGE_THRESHOLD_IN_COUNT)
    , mergeThresholdInSize(DEFAULT_MERGE_THRESHOLD_IN_SIZE)
    , schemaVersion(DEFAULT_SCHEMA_VERSION)
    , majorVersion(0)
    , minorVersion(0) {}

SwiftWriterConfig::~SwiftWriterConfig() {}

bool SwiftWriterConfig::isValidate() const {
    return (brokerBusyWaitIntervalMax >= brokerBusyWaitIntervalMin) && !topicName.empty() &&
           mergeThresholdInCount <= MAX_MERGE_THRESHOLD_IN_COUNT;
}

void SwiftWriterConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize(WRITER_TOPIC_NAME, topicName, std::string());
    json.Jsonize(WRITER_CONFIG_RETRY_TIMES, retryTimes, retryTimes);
    json.Jsonize(WRITER_CONFIG_RETRY_TIME_INTERVAL, retryTimeInterval, retryTimeInterval);
    json.Jsonize(WRITER_CONFIG_REQUEST_SEND_BYTE, oneRequestSendByteCount, oneRequestSendByteCount);
    json.Jsonize(WRITER_CONFIG_MAX_BUFFER_BYTE, maxBufferSize, maxBufferSize);
    json.Jsonize(WRITER_CONFIG_MAX_KEEP_IN_BUFFER_TIME, maxKeepInBufferTime, maxKeepInBufferTime);
    json.Jsonize(WRITER_CONFIG_SEND_REQUEST_LOOP_INTERVAL, sendRequestLoopInterval, sendRequestLoopInterval);
    json.Jsonize(WRITER_CONFIG_WAIT_LOOP_INTERVAL, waitLoopInterval, waitLoopInterval);
    json.Jsonize(WRITER_CONFIG_BROKER_BUSY_WAIT_INTERVAL_MIN, brokerBusyWaitIntervalMin, brokerBusyWaitIntervalMin);
    json.Jsonize(WRITER_CONFIG_BROKER_BUSY_WAIT_INTERVAL_MAX, brokerBusyWaitIntervalMax, brokerBusyWaitIntervalMax);
    json.Jsonize(WRITER_CONFIG_FUNCTION_CHAIN, functionChain, std::string());
    json.Jsonize(WRITER_CONFIG_WAIT_FINISHED_WRITER_TIME, waitFinishedWriterTime, waitFinishedWriterTime);
    json.Jsonize(WRITER_CONFIG_REQUEST_COMPRESS, compress, false);
    json.Jsonize(WRITER_CONFIG_MESSAGE_COMPRESS, compressMsg, false);
    json.Jsonize(WRITER_CONFIG_MESSAGE_COMPRESS_IN_BROKER, compressMsgInBroker, false);
    json.Jsonize(WRITER_CONFIG_COMPRESS_THRESHOLD_IN_BYTES, compressThresholdInBytes, compressThresholdInBytes);
    json.Jsonize(
        WRITER_CONFIG_BROKER_COMMIT_PROGRESS_DETECTION_INTERVAL, commitDetectionInterval, commitDetectionInterval);
    json.Jsonize(WRITER_CONFIG_SYNC_SEND_TIMEOUT, syncSendTimeout, syncSendTimeout);
    json.Jsonize(WRITER_CONFIG_ASYNC_SEND_TIMEOUT, asyncSendTimeout, asyncSendTimeout);
    json.Jsonize(WRITER_CONFIG_MESSAGE_FORMAT, messageFormat, messageFormat);
    json.Jsonize(WRITER_CONFIG_ZK_PATH, zkPath, zkPath);
    json.Jsonize(WRITER_CONFIG_MERGE_MESSAGE, mergeMsg, false);
    json.Jsonize(WRITER_CONFIG_NEED_TIMESTAMP, needTimestamp, false);
    json.Jsonize(WRITER_CONFIG_MERGE_THRESHOLD_IN_COUNT, mergeThresholdInCount, mergeThresholdInCount);
    json.Jsonize(WRITER_CONFIG_MERGE_THRESHOLD_IN_SIZE, mergeThresholdInSize, mergeThresholdInSize);
    json.Jsonize(WRITER_CONFIG_SCHEMA_VERSION, schemaVersion, schemaVersion);
    json.Jsonize(WRITER_CONFIG_IDENTITY, writerIdentity, writerIdentity);
    json.Jsonize(WRITER_CONFIG_ACCESS_ID, accessId, accessId);
    json.Jsonize(WRITER_CONFIG_ACCESS_KEY, accessKey, accessKey);
    json.Jsonize(WRITER_CONFIG_WRITER_NAME, writerName, writerName);
    json.Jsonize(WRITER_CONFIG_MAJOR_VERSION, majorVersion, majorVersion);
    json.Jsonize(WRITER_CONFIG_MINOR_VERSION, minorVersion, minorVersion);

    if (json.GetMode() == FROM_JSON) {
        string modeStr;
        json.Jsonize(WRITER_CONFIG_MODE, modeStr, std::string("async"));
        parseMode(modeStr);
        if (compressThresholdInBytes == DEFAULT_COMPRESS_THRESHOLD_IN_BYTES && compressMsg) {
            compressThresholdInBytes = 0; // compress all
        }
    } else if (json.GetMode() == TO_JSON) {
        json.Jsonize("sync_mode", isSynchronous, isSynchronous);
        json.Jsonize("safe_mode", safeMode, safeMode);
    }
    if (mergeThresholdInCount > MAX_MERGE_THRESHOLD_IN_COUNT) {
        mergeThresholdInCount = MAX_MERGE_THRESHOLD_IN_COUNT;
    }
}

bool SwiftWriterConfig::parseFromString(const string &configStr) {
    try {
        autil::legacy::FromJsonString(*this, configStr);
        return true;
    } catch (const autil::legacy::ExceptionBase &e) {}

#define PARSE_SWIFT_WRITER_CONFIG(keyName, valueName, valueType, parseFunc)                                            \
    else if (key == keyName) {                                                                                         \
        valueType value;                                                                                               \
        if (!parseFunc(valueStr.c_str(), value)) {                                                                     \
            AUTIL_LOG(ERROR,                                                                                           \
                      "Invalid swift writer config [%s], needed type[%s], actually[%s].",                              \
                      keyName,                                                                                         \
                      #valueType,                                                                                      \
                      valueStr.c_str());                                                                               \
            return false;                                                                                              \
        }                                                                                                              \
        valueName = value;                                                                                             \
    }

    StringTokenizer st(
        configStr, WRITER_CONFIG_SEPERATOR, StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        StringTokenizer st2(*it, WRITER_CONFIG_KV_SEPERATOR, StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_TRIM);
        if (st2.getNumTokens() != 2) {
            AUTIL_LOG(ERROR, "Invalid config item: [%s]", (*it).c_str());
            return false;
        }
        string key = st2[0];
        string valueStr = st2[1];
        if (key == WRITER_TOPIC_NAME) {
            topicName = valueStr;
        } else if (key == WRITER_CONFIG_MODE) {
            if (!parseMode(valueStr)) {
                return false;
            }
        } else if (key == WRITER_CONFIG_REQUEST_COMPRESS) {
            if (valueStr == "true") {
                compress = true;
            } else {
                compress = false;
            }
        } else if (key == WRITER_CONFIG_MESSAGE_COMPRESS) {
            if (valueStr == "true") {
                compressMsg = true;
            } else {
                compressMsg = false;
            }
        } else if (key == WRITER_CONFIG_MESSAGE_COMPRESS_IN_BROKER) {
            if (valueStr == "true") {
                compressMsgInBroker = true;
            } else {
                compressMsgInBroker = false;
            }
        } else if (key == WRITER_CONFIG_MERGE_MESSAGE) {
            if (valueStr == "true") {
                mergeMsg = true;
            } else {
                mergeMsg = false;
            }
        } else if (key == WRITER_CONFIG_NEED_TIMESTAMP) {
            if (valueStr == "true") {
                needTimestamp = true;
            } else {
                needTimestamp = false;
            }
        } else if (key == WRITER_CONFIG_WRITER_NAME) {
            writerName = valueStr;
        }
        PARSE_SWIFT_WRITER_CONFIG(WRITER_CONFIG_RETRY_TIMES, retryTimes, uint32_t, StringUtil::strToUInt32)
        PARSE_SWIFT_WRITER_CONFIG(
            WRITER_CONFIG_RETRY_TIME_INTERVAL, retryTimeInterval, uint64_t, StringUtil::strToUInt64)
        PARSE_SWIFT_WRITER_CONFIG(
            WRITER_CONFIG_REQUEST_SEND_BYTE, oneRequestSendByteCount, uint64_t, StringUtil::strToUInt64)
        PARSE_SWIFT_WRITER_CONFIG(WRITER_CONFIG_MAX_BUFFER_BYTE, maxBufferSize, uint64_t, StringUtil::strToUInt64)
        PARSE_SWIFT_WRITER_CONFIG(
            WRITER_CONFIG_MAX_KEEP_IN_BUFFER_TIME, maxKeepInBufferTime, uint64_t, StringUtil::strToUInt64)
        PARSE_SWIFT_WRITER_CONFIG(
            WRITER_CONFIG_SEND_REQUEST_LOOP_INTERVAL, sendRequestLoopInterval, uint64_t, StringUtil::strToUInt64)
        PARSE_SWIFT_WRITER_CONFIG(WRITER_CONFIG_WAIT_LOOP_INTERVAL, waitLoopInterval, uint64_t, StringUtil::strToUInt64)
        PARSE_SWIFT_WRITER_CONFIG(
            WRITER_CONFIG_BROKER_BUSY_WAIT_INTERVAL_MIN, brokerBusyWaitIntervalMin, uint64_t, StringUtil::strToUInt64)
        PARSE_SWIFT_WRITER_CONFIG(
            WRITER_CONFIG_BROKER_BUSY_WAIT_INTERVAL_MAX, brokerBusyWaitIntervalMax, uint64_t, StringUtil::strToUInt64)
        PARSE_SWIFT_WRITER_CONFIG(
            WRITER_CONFIG_WAIT_FINISHED_WRITER_TIME, waitFinishedWriterTime, uint64_t, StringUtil::strToUInt64)
        PARSE_SWIFT_WRITER_CONFIG(
            WRITER_CONFIG_COMPRESS_THRESHOLD_IN_BYTES, compressThresholdInBytes, uint64_t, StringUtil::strToUInt64)
        PARSE_SWIFT_WRITER_CONFIG(WRITER_CONFIG_BROKER_COMMIT_PROGRESS_DETECTION_INTERVAL,
                                  commitDetectionInterval,
                                  uint64_t,
                                  StringUtil::strToUInt64)
        PARSE_SWIFT_WRITER_CONFIG(WRITER_CONFIG_SYNC_SEND_TIMEOUT, syncSendTimeout, uint64_t, StringUtil::strToUInt64)
        PARSE_SWIFT_WRITER_CONFIG(WRITER_CONFIG_ASYNC_SEND_TIMEOUT, asyncSendTimeout, uint64_t, StringUtil::strToUInt64)
        PARSE_SWIFT_WRITER_CONFIG(WRITER_CONFIG_MESSAGE_FORMAT, messageFormat, uint32_t, StringUtil::strToUInt32)
        PARSE_SWIFT_WRITER_CONFIG(
            WRITER_CONFIG_MERGE_THRESHOLD_IN_COUNT, mergeThresholdInCount, uint16_t, StringUtil::strToUInt16)
        PARSE_SWIFT_WRITER_CONFIG(
            WRITER_CONFIG_MERGE_THRESHOLD_IN_SIZE, mergeThresholdInSize, uint64_t, StringUtil::strToUInt64)
        PARSE_SWIFT_WRITER_CONFIG(WRITER_CONFIG_SCHEMA_VERSION, schemaVersion, int32_t, StringUtil::strToInt32)
        PARSE_SWIFT_WRITER_CONFIG(WRITER_CONFIG_MAJOR_VERSION, majorVersion, uint32_t, StringUtil::strToUInt32)
        PARSE_SWIFT_WRITER_CONFIG(WRITER_CONFIG_MINOR_VERSION, minorVersion, uint32_t, StringUtil::strToUInt32)
        else if (key == WRITER_CONFIG_FUNCTION_CHAIN) {
            functionChain = valueStr;
        }
        else if (key == WRITER_CONFIG_ZK_PATH) {
            zkPath = valueStr;
        }
        else if (key == WRITER_CONFIG_IDENTITY) {
            writerIdentity = valueStr;
        }
        else if (key == WRITER_CONFIG_ACCESS_ID) {
            accessId = valueStr;
        }
        else if (key == WRITER_CONFIG_ACCESS_KEY) {
            accessKey = valueStr;
        }
        else {
            AUTIL_LOG(WARN, "Unknown config key : [%s]", key.c_str());
        }
    }
#undef PARSE_SWIFT_WRITER_CONFIG
    if (compressThresholdInBytes == DEFAULT_COMPRESS_THRESHOLD_IN_BYTES && compressMsg) {
        compressThresholdInBytes = 0; // compress all
    }
    if (mergeThresholdInCount > MAX_MERGE_THRESHOLD_IN_COUNT) {
        mergeThresholdInCount = MAX_MERGE_THRESHOLD_IN_COUNT;
    }
    if (writerIdentity.empty()) {
        writerIdentity = StringUtil::toString(TimeUtility::currentTime());
    }

    return true;
}

bool SwiftWriterConfig::parseMode(const string &modeStr) {
    vector<string> strVec = StringUtil::split(modeStr, "|", true);
    if (strVec.size() == 0) {
        return false;
    }
    string syncStr = strVec[0];
    if (syncStr != "sync" && syncStr != "async") {
        AUTIL_LOG(ERROR, "Invalid swift writer mode(sync/async): [%s]", syncStr.c_str());
        return false;
    }
    isSynchronous = syncStr == "sync";
    if (strVec.size() > 1) {
        if (isSynchronous && strVec[1] == "safe") {
            AUTIL_LOG(WARN, "unsupport sync|safe mode [%s]", syncStr.c_str());
        }
        safeMode = strVec[1] == "safe";
    }
    if (isSynchronous) {
        safeMode = false;
    }
    return true;
}

} // namespace client
} // namespace swift
