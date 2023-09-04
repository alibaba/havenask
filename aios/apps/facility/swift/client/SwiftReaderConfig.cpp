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
#include "swift/client/SwiftReaderConfig.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "swift/protocol/SwiftMessage.pb.h"

using namespace std;
using namespace autil;

namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, SwiftReaderConfig);

const uint64_t SwiftReaderConfig::DEFAULT_FATAL_ERROR_TIME_LIMIT = 60 * 1000 * 1000;     // 1min
const uint64_t SwiftReaderConfig::DEFAULT_FATAL_ERROR_REPORT_INTERVAL = 3 * 1000 * 1000; // 3 s
const uint64_t SwiftReaderConfig::DEFAULT_CONSUMPTIONREQUEST_BYTES_LIMIT = 512 * 1024;   // 512 kb
const uint64_t SwiftReaderConfig::ONE_REQUEST_READ_COUNT = 1 << 15;                      // 32767
const uint64_t SwiftReaderConfig::READ_BUFFER_SIZE = 2048;
const uint32_t SwiftReaderConfig::DEFAULT_RETRY_TIMES = 3;
const uint64_t SwiftReaderConfig::RETRY_GET_MESSAGE_INTERVAL = 100 * 1000;             // 100ms
const uint64_t SwiftReaderConfig::PARTITION_STATUS_REFRESH_INTERVAL = 5 * 1000 * 1000; // 5s
const uint32_t SwiftReaderConfig::DEFAULT_MESSAGE_FORMAT = 1;                          // MF_FB
const uint32_t SwiftReaderConfig::DEFAULT_MERGE_MESSAGE = 1;                           // support merge
const uint32_t SwiftReaderConfig::DEFAULT_BATCH_READ_COUNT = 256;
const uint64_t SwiftReaderConfig::CHECK_PARTITION_LOOP_INTERVAL = 1000 * 1000;            // 1s
const uint64_t SwiftReaderConfig::DEFAULT_TOPIC_SWITCH_INTERVAL = 100 * 1000;             // 100ms
const uint64_t SwiftReaderConfig::DEFAULT_CHECKPOINT_REFRESH_TIMESTAMP_OFFSET = 10000000; // 10s
SwiftReaderConfig::SwiftReaderConfig()
    : fatalErrorTimeLimit(DEFAULT_FATAL_ERROR_TIME_LIMIT)
    , fatalErrorReportInterval(DEFAULT_FATAL_ERROR_REPORT_INTERVAL)
    , consumptionRequestBytesLimit(DEFAULT_CONSUMPTIONREQUEST_BYTES_LIMIT)
    , oneRequestReadCount(ONE_REQUEST_READ_COUNT)
    , readBufferSize(READ_BUFFER_SIZE)
    , retryTimes(DEFAULT_RETRY_TIMES)
    , retryGetMsgInterval(RETRY_GET_MESSAGE_INTERVAL)
    , partitionStatusRefreshInterval(PARTITION_STATUS_REFRESH_INTERVAL)
    , needCompress(false)
    , canDecompress(true)
    , messageFormat(DEFAULT_MESSAGE_FORMAT)
    , batchReadCount(DEFAULT_BATCH_READ_COUNT)
    , readAll(false)
    , mergeMessage(DEFAULT_MERGE_MESSAGE)
    , checkpointRefreshTimestampOffset(DEFAULT_CHECKPOINT_REFRESH_TIMESTAMP_OFFSET) {}

SwiftReaderConfig::~SwiftReaderConfig() {}

void SwiftReaderConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize(READER_CONFIG_TOPIC_NAME, topicName, topicName);
    json.Jsonize(READER_CONFIG_PARTITIONS, readPartVec, readPartVec);
    json.Jsonize(READER_CONFIG_FATAL_ERROR_TIME_LIMIT, fatalErrorTimeLimit, fatalErrorTimeLimit);
    json.Jsonize(READER_CONFIG_FATAL_ERROR_REPORT_INTERVAL, fatalErrorReportInterval, fatalErrorReportInterval);
    json.Jsonize(
        READER_CONFIG_CONSUMPTIONREQUEST_BYTES_LIMIT, consumptionRequestBytesLimit, consumptionRequestBytesLimit);
    json.Jsonize(READER_CONFIG_ONE_REQUEST_READ_COUNT, oneRequestReadCount, oneRequestReadCount);
    json.Jsonize(READER_CONFIG_READ_BUFFER_SIZE, readBufferSize, readBufferSize);
    json.Jsonize(READER_CONFIG_RETRY_TIMES, retryTimes, retryTimes);
    json.Jsonize(READER_CONFIG_RETRY_GET_MESSAGE_INTERVAL, retryGetMsgInterval, retryGetMsgInterval);
    json.Jsonize(READER_CONFIG_PARTITION_STATUS_REFRESH_INTERVAL,
                 partitionStatusRefreshInterval,
                 partitionStatusRefreshInterval);
    json.Jsonize(READER_CONFIG_READER_IDENTITY, readerIdentity, readerIdentity);
    json.Jsonize(READER_CONFIG_ACCESS_ID, accessId, accessId);
    json.Jsonize(READER_CONFIG_ACCESS_KEY, accessKey, accessKey);

    if (json.GetMode() == FROM_JSON) {
        uint16_t from = 0;
        uint16_t to = 65535;
        uint8_t uint8FilterMask = 0;
        uint8_t uint8MaskResult = 0;
        json.Jsonize(READER_CONFIG_FROM, from, from);
        json.Jsonize(READER_CONFIG_TO, to, to);
        json.Jsonize(READER_CONFIG_MASK, uint8FilterMask, uint8FilterMask);
        json.Jsonize(READER_CONFIG_MASKRESULT, uint8MaskResult, uint8MaskResult);
        swiftFilter.set_from(from);
        swiftFilter.set_to(to);
        swiftFilter.set_uint8filtermask(uint8FilterMask);
        swiftFilter.set_uint8maskresult(uint8MaskResult);
    } else if (json.GetMode() == TO_JSON) {
        uint16_t from = swiftFilter.from();
        uint16_t to = swiftFilter.to();
        uint8_t uint8FilterMask = swiftFilter.uint8filtermask();
        uint8_t uint8MaskResult = swiftFilter.uint8maskresult();
        json.Jsonize(READER_CONFIG_FROM, from, from);
        json.Jsonize(READER_CONFIG_TO, to, to);
        json.Jsonize(READER_CONFIG_MASK, uint8FilterMask, uint8FilterMask);
        json.Jsonize(READER_CONFIG_MASKRESULT, uint8MaskResult, uint8MaskResult);
    }
    json.Jsonize(READER_CONFIG_READ_ALL, readAll, readAll);
    json.Jsonize(READER_CONFIG_REQUEST_COMPRESS, needCompress, needCompress);
    json.Jsonize(READER_CONFIG_DECOMPRESS_MESSAGE_IN_CLIENT, canDecompress, canDecompress);
    json.Jsonize(READER_CONFIG_REQUIRED_FIELDS, requiredFields, requiredFields);
    json.Jsonize(READER_CONFIG_FIELD_FILTER_DESC, fieldFilterDesc, fieldFilterDesc);
    json.Jsonize(READER_CONFIG_MESSAGE_FORMAT, messageFormat, messageFormat);
    json.Jsonize(READER_CONFIG_MERGE_MESSAGE, mergeMessage, mergeMessage);
    json.Jsonize(READER_CONFIG_BATCH_READ_COUNT, batchReadCount, batchReadCount);
    json.Jsonize(READER_CONFIG_ZK_PATH, zkPath, zkPath);
    json.Jsonize(READER_CONFIG_MULTI_READER_ORDER, multiReaderOrder, multiReaderOrder);
    json.Jsonize(READER_CONFIG_CHECKPOINT_MODE, checkpointMode, checkpointMode);
    json.Jsonize(READER_CONFIG_CHECKPOINT_REFRESH_TIMESTAMP_OFFSET,
                 checkpointRefreshTimestampOffset,
                 checkpointRefreshTimestampOffset);
}

bool SwiftReaderConfig::parseFromString(const std::string &configStr) {
    try {
        autil::legacy::FromJsonString(*this, configStr);
        return true;
    } catch (const autil::legacy::ExceptionBase &e) {}

#define PARSE_SWIFT_READER_CONFIG_WITH_OPERATION(keyName, valueType, parseFunc, op)                                    \
    else if (key == keyName) {                                                                                         \
        valueType value;                                                                                               \
        if (!parseFunc(valueStr.c_str(), value)) {                                                                     \
            AUTIL_LOG(ERROR,                                                                                           \
                      "Invalid swift reader config [%s], needed type[%s], actually[%s].",                              \
                      keyName,                                                                                         \
                      #valueType,                                                                                      \
                      valueStr.c_str());                                                                               \
            return false;                                                                                              \
        }                                                                                                              \
        op;                                                                                                            \
    }
#define PARSE_SWIFT_READER_CONFIG(keyName, valueName, valueType, parseFunc)                                            \
    PARSE_SWIFT_READER_CONFIG_WITH_OPERATION(keyName, valueType, parseFunc, valueName = value)

    StringTokenizer st(
        configStr, READER_CONFIG_SEPERATOR, StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        StringTokenizer st2(*it, READER_CONFIG_KV_SEPERATOR, StringTokenizer::TOKEN_TRIM);
        if (st2.getNumTokens() != 2) {
            AUTIL_LOG(ERROR, "Invalid config item: [%s]", (*it).c_str());
            return false;
        }
        string key = st2[0];
        string valueStr = st2[1];

        if (false) {}
        PARSE_SWIFT_READER_CONFIG_WITH_OPERATION(
            READER_CONFIG_FROM, uint16_t, StringUtil::strToUInt16, swiftFilter.set_from(value))
        PARSE_SWIFT_READER_CONFIG_WITH_OPERATION(
            READER_CONFIG_TO, uint16_t, StringUtil::strToUInt16, swiftFilter.set_to(value))
        PARSE_SWIFT_READER_CONFIG_WITH_OPERATION(
            READER_CONFIG_MASK, uint8_t, StringUtil::strToUInt8, swiftFilter.set_uint8filtermask(value))
        PARSE_SWIFT_READER_CONFIG_WITH_OPERATION(
            READER_CONFIG_MASKRESULT, uint8_t, StringUtil::strToUInt8, swiftFilter.set_uint8maskresult(value))
        PARSE_SWIFT_READER_CONFIG(
            READER_CONFIG_FATAL_ERROR_TIME_LIMIT, fatalErrorTimeLimit, uint64_t, StringUtil::strToUInt64)
        PARSE_SWIFT_READER_CONFIG(
            READER_CONFIG_FATAL_ERROR_REPORT_INTERVAL, fatalErrorReportInterval, uint64_t, StringUtil::strToUInt64)
        PARSE_SWIFT_READER_CONFIG(READER_CONFIG_CONSUMPTIONREQUEST_BYTES_LIMIT,
                                  consumptionRequestBytesLimit,
                                  uint64_t,
                                  StringUtil::strToUInt64)
        PARSE_SWIFT_READER_CONFIG(
            READER_CONFIG_ONE_REQUEST_READ_COUNT, oneRequestReadCount, uint64_t, StringUtil::strToUInt64)
        PARSE_SWIFT_READER_CONFIG(READER_CONFIG_READ_BUFFER_SIZE, readBufferSize, uint64_t, StringUtil::strToUInt64)
        PARSE_SWIFT_READER_CONFIG(READER_CONFIG_RETRY_TIMES, retryTimes, uint32_t, StringUtil::strToUInt32)
        PARSE_SWIFT_READER_CONFIG(
            READER_CONFIG_RETRY_GET_MESSAGE_INTERVAL, retryGetMsgInterval, uint64_t, StringUtil::strToUInt64)
        PARSE_SWIFT_READER_CONFIG(READER_CONFIG_PARTITION_STATUS_REFRESH_INTERVAL,
                                  partitionStatusRefreshInterval,
                                  uint64_t,
                                  StringUtil::strToUInt64)
        PARSE_SWIFT_READER_CONFIG(READER_CONFIG_MESSAGE_FORMAT, messageFormat, uint32_t, StringUtil::strToUInt32)
        PARSE_SWIFT_READER_CONFIG(READER_CONFIG_MERGE_MESSAGE, mergeMessage, uint32_t, StringUtil::strToUInt32)
        PARSE_SWIFT_READER_CONFIG(READER_CONFIG_BATCH_READ_COUNT, batchReadCount, uint32_t, StringUtil::strToUInt32)
        PARSE_SWIFT_READER_CONFIG(READER_CONFIG_CHECKPOINT_REFRESH_TIMESTAMP_OFFSET,
                                  checkpointRefreshTimestampOffset,
                                  uint64_t,
                                  StringUtil::strToUInt64)
        else if (key == READER_CONFIG_REQUEST_COMPRESS) {
            needCompress = (valueStr == "true") ? true : false;
        }
        else if (key == READER_CONFIG_DECOMPRESS_MESSAGE_IN_CLIENT) {
            canDecompress = (valueStr == "true") ? true : false;
        }
        else if (key == READER_CONFIG_READ_ALL) {
            readAll = (valueStr == "true") ? true : false;
        }
        else if (key == READER_CONFIG_TOPIC_NAME) {
            topicName = valueStr;
        }
        else if (key == READER_CONFIG_PARTITIONS) {
            vector<string> partStrVec = StringUtil::split(valueStr, ",");
            for (size_t i = 0; i < partStrVec.size(); i++) {
                uint32_t pid;
                if (!StringUtil::strToUInt32(partStrVec[i].c_str(), pid)) {
                    return false;
                }
                readPartVec.push_back(pid);
            }
        }
        else if (key == READER_CONFIG_FIELD_FILTER_DESC) {
            fieldFilterDesc = valueStr;
        }
        else if (key == READER_CONFIG_ZK_PATH) {
            zkPath = valueStr;
        }
        else if (key == READER_CONFIG_REQUIRED_FIELDS) {
            requiredFields = StringUtil::split(valueStr, ",");
        }
        else if (key == READER_CONFIG_READER_IDENTITY) {
            readerIdentity = valueStr;
        }
        else if (key == READER_CONFIG_ACCESS_ID) {
            accessId = valueStr;
        }
        else if (key == READER_CONFIG_ACCESS_KEY) {
            accessKey = valueStr;
        }
        else if (key == READER_CONFIG_MULTI_READER_ORDER) {
            multiReaderOrder = valueStr;
        }
        else if (key == READER_CONFIG_CHECKPOINT_MODE) {
            checkpointMode = valueStr;
        }
        else {
            AUTIL_LOG(ERROR, "Unknown config key : [%s]", key.c_str());
            return false;
        }
    }
#undef PARSE_SWIFT_READER_CONFIG
#undef PARSE_SWIFT_READER_CONFIG_WITH_OPERATION

    if (readerIdentity.empty()) {
        readerIdentity = StringUtil::toString(TimeUtility::currentTime());
    }
    return true;
}

} // namespace client
} // namespace swift
