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
#pragma once

#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

namespace swift {
namespace client {
constexpr char READER_CONFIG_SEPERATOR[] = ";";
constexpr char READER_CONFIG_KV_SEPERATOR[] = "=";
constexpr char READER_CONFIG_TOPIC_NAME[] = "topicName";
constexpr char READER_CONFIG_PARTITIONS[] = "partitions";
constexpr char READER_CONFIG_REQUIRED_FIELDS[] = "requiredFields";
constexpr char READER_CONFIG_FIELD_FILTER_DESC[] = "fieldFilterDesc";
constexpr char READER_CONFIG_FROM[] = "from";
constexpr char READER_CONFIG_TO[] = "to";
constexpr char READER_CONFIG_READ_ALL[] = "readAll";
constexpr char READER_CONFIG_MASK[] = "uint8FilterMask";
constexpr char READER_CONFIG_MASKRESULT[] = "uint8MaskResult";
constexpr char READER_CONFIG_FATAL_ERROR_TIME_LIMIT[] = "fatalErrorTimeLimit";
constexpr char READER_CONFIG_FATAL_ERROR_REPORT_INTERVAL[] = "fatalErrorReportInterval";
constexpr char READER_CONFIG_CONSUMPTIONREQUEST_BYTES_LIMIT[] = "consumptionRequestBytesLimit";
constexpr char READER_CONFIG_ONE_REQUEST_READ_COUNT[] = "oneRequestReadCount";
constexpr char READER_CONFIG_READ_BUFFER_SIZE[] = "readBufferSize";
constexpr char READER_CONFIG_RETRY_TIMES[] = "retryTimes";
constexpr char READER_CONFIG_RETRY_GET_MESSAGE_INTERVAL[] = "retryGetMsgInterval";
constexpr char READER_CONFIG_PARTITION_STATUS_REFRESH_INTERVAL[] = "partitionStatusRefreshInterval";
constexpr char READER_CONFIG_REQUEST_COMPRESS[] = "compress";
constexpr char READER_CONFIG_DECOMPRESS_MESSAGE_IN_CLIENT[] = "decompress";
constexpr char READER_CONFIG_MESSAGE_FORMAT[] = "messageFormat"; // for test
constexpr char READER_CONFIG_MERGE_MESSAGE[] = "mergeMessage";   // for test
constexpr char READER_CONFIG_BATCH_READ_COUNT[] = "batchReadCount";
constexpr char READER_CONFIG_ZK_PATH[] = "zkPath";
constexpr char READER_CONFIG_READER_IDENTITY[] = "readerIdentity";
constexpr char READER_CONFIG_ACCESS_ID[] = "accessId";
constexpr char READER_CONFIG_ACCESS_KEY[] = "accessKey";
constexpr char READER_CONFIG_MULTI_READER_ORDER[] = "multiReaderOrder"; // sequence, default
constexpr char READER_CONFIG_CHECKPOINT_MODE[] = "checkpointMode";
constexpr char READER_CONFIG_CHECKPOINT_REFRESH_TIMESTAMP_OFFSET[] = "checkpointRefreshTimestampOffset";
constexpr char READER_CHECKPOINT_REFRESH_MODE[] = "refresh";
constexpr char READER_CHECKPOINT_READED_MODE[] = "readed";
class SwiftReaderConfig : public autil::legacy::Jsonizable {
public:
    static const uint64_t DEFAULT_FATAL_ERROR_TIME_LIMIT;
    static const uint64_t DEFAULT_FATAL_ERROR_REPORT_INTERVAL;
    static const uint64_t DEFAULT_CONSUMPTIONREQUEST_BYTES_LIMIT;
    static const uint64_t ONE_REQUEST_READ_COUNT;
    static const uint64_t READ_BUFFER_SIZE;
    static const uint32_t DEFAULT_RETRY_TIMES;
    static const uint64_t RETRY_GET_MESSAGE_INTERVAL;
    static const uint64_t PARTITION_STATUS_REFRESH_INTERVAL;
    static const uint32_t DEFAULT_MESSAGE_FORMAT;
    static const uint32_t DEFAULT_BATCH_READ_COUNT;
    static const uint64_t CHECK_PARTITION_LOOP_INTERVAL;
    static const uint32_t DEFAULT_MERGE_MESSAGE;
    static const uint64_t DEFAULT_TOPIC_SWITCH_INTERVAL;
    static const uint64_t DEFAULT_CHECKPOINT_REFRESH_TIMESTAMP_OFFSET;

public:
    SwiftReaderConfig();
    ~SwiftReaderConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);

    bool parseFromString(const std::string &configStr);

public:
    bool isAccessAuthEmpty() const { return accessKey.empty() || accessId.empty(); }
    protocol::AccessAuthInfo getAccessAuthInfo() const {
        protocol::AccessAuthInfo authInfo;
        if (!isAccessAuthEmpty()) {
            authInfo.set_accessid(accessId);
            authInfo.set_accesskey(accessKey);
        }
        return authInfo;
    }

public:
    std::string topicName;
    std::string readerIdentity;
    std::string accessId;  // 订阅id
    std::string accessKey; // 订阅key
    std::vector<uint32_t> readPartVec;
    uint64_t fatalErrorTimeLimit;
    uint64_t fatalErrorReportInterval;
    uint64_t consumptionRequestBytesLimit;
    uint64_t oneRequestReadCount;
    uint64_t readBufferSize;
    uint32_t retryTimes;
    uint64_t retryGetMsgInterval;
    uint64_t partitionStatusRefreshInterval;
    protocol::Filter swiftFilter;
    bool needCompress;
    bool canDecompress;
    std::vector<std::string> requiredFields;
    std::string fieldFilterDesc;
    uint32_t messageFormat;
    uint32_t batchReadCount;
    std::string zkPath;
    bool readAll;
    uint32_t mergeMessage;
    std::string multiReaderOrder;
    std::string checkpointMode;
    uint64_t checkpointRefreshTimestampOffset;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftReaderConfig);

} // namespace client
} // namespace swift
