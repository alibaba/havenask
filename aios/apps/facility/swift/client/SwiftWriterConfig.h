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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace client {
constexpr char WRITER_CONFIG_SEPERATOR[] = ";";
constexpr char WRITER_CONFIG_KV_SEPERATOR[] = "=";
constexpr char WRITER_CONFIG_MODE[] = "mode";
constexpr char WRITER_CONFIG_RETRY_TIMES[] = "retryTimes";
constexpr char WRITER_CONFIG_RETRY_TIME_INTERVAL[] = "retryTimeInterval";
constexpr char WRITER_CONFIG_REQUEST_SEND_BYTE[] = "oneRequestSendByteCount";
constexpr char WRITER_CONFIG_MAX_BUFFER_BYTE[] = "maxBufferSize";
constexpr char WRITER_CONFIG_MAX_KEEP_IN_BUFFER_TIME[] = "maxKeepInBufferTime";
constexpr char WRITER_CONFIG_SEND_REQUEST_LOOP_INTERVAL[] = "sendRequestLoopInterval";
constexpr char WRITER_CONFIG_BROKER_BUSY_WAIT_INTERVAL_MIN[] = "brokerBusyWaitIntervalMin";
constexpr char WRITER_CONFIG_BROKER_BUSY_WAIT_INTERVAL_MAX[] = "brokerBusyWaitIntervalMax";
constexpr char WRITER_CONFIG_FUNCTION_CHAIN[] = "functionChain";
constexpr char WRITER_CONFIG_WAIT_FINISHED_WRITER_TIME[] = "waitFinishedWriterTime";
constexpr char WRITER_CONFIG_REQUEST_COMPRESS[] = "compress";
constexpr char WRITER_CONFIG_MESSAGE_COMPRESS[] = "compressMsg";
constexpr char WRITER_CONFIG_NEED_TIMESTAMP[] = "needTimestamp";
constexpr char WRITER_CONFIG_MESSAGE_COMPRESS_IN_BROKER[] = "compressMsgInBroker";
constexpr char WRITER_CONFIG_COMPRESS_THRESHOLD_IN_BYTES[] = "compressThresholdInBytes";
constexpr char WRITER_CONFIG_BROKER_COMMIT_PROGRESS_DETECTION_INTERVAL[] = "commitDetectionInterval";
constexpr char WRITER_TOPIC_NAME[] = "topicName";
constexpr char WRITER_CONFIG_SYNC_SEND_TIMEOUT[] = "syncSendTimeout";
constexpr char WRITER_CONFIG_ASYNC_SEND_TIMEOUT[] = "asyncSendTimeout";
constexpr char WRITER_CONFIG_ZK_PATH[] = "zkPath";
constexpr char WRITER_CONFIG_MESSAGE_FORMAT[] = "messageFormat"; // for test
constexpr char WRITER_CONFIG_WAIT_LOOP_INTERVAL[] = "waitLoopInterval";
constexpr char WRITER_CONFIG_MERGE_MESSAGE[] = "mergeMessage";
constexpr char WRITER_CONFIG_MERGE_THRESHOLD_IN_COUNT[] = "mergeThresholdInCount";
constexpr char WRITER_CONFIG_MERGE_THRESHOLD_IN_SIZE[] = "mergeThresholdInSize";
constexpr char WRITER_CONFIG_SCHEMA_VERSION[] = "schemaVersion";
constexpr char WRITER_CONFIG_IDENTITY[] = "writerIdentity";
constexpr char WRITER_CONFIG_ACCESS_ID[] = "accessId";
constexpr char WRITER_CONFIG_ACCESS_KEY[] = "accessKey";
constexpr char WRITER_CONFIG_WRITER_NAME[] = "writerName";
constexpr char WRITER_CONFIG_MAJOR_VERSION[] = "majorVersion";
constexpr char WRITER_CONFIG_MINOR_VERSION[] = "minorVersion";

class SwiftWriterConfig : public autil::legacy::Jsonizable {
public:
    static const uint32_t DEFAULT_RETRY_TIMES;
    static const uint64_t DEFAULT_RETRY_TIME_INTERVAL;
    static const uint64_t DEFAULT_REQUEST_SEND_BYTE;
    static const uint64_t DEFAULT_MAX_BUFFER_BYTE;
    static const uint64_t DEFAULT_SINGLE_BUFFER_BYTE;
    static const uint64_t DEFAULT_MAX_KEEP_IN_BUFFER_TIME;
    static const uint64_t SEND_REQUEST_LOOP_INTERVAL;
    static const uint64_t BROKER_BUSY_WAIT_INTERVAL_MIN;
    static const uint64_t BROKER_BUSY_WAIT_INTERVAL_MAX;
    static const uint64_t DEFAULT_WAIT_FINISHED_WRITER_TIME;
    static const uint64_t DEFAULT_COMPRESS_THRESHOLD_IN_BYTES;
    static const uint64_t DEFAULT_BROKER_COMMIT_PROGRESS_DETECTION_INTERVAL;
    static const uint64_t DEFAULT_SYNC_TIMEOUT_INTERVAL;
    static const uint64_t DEFAULT_ASYNC_TIMEOUT_INTERVAL;
    static const uint32_t DEFAULT_MESSAGE_FORMAT;
    static const uint64_t DEFAULT_WAIT_LOOP_INTERVAL;
    static const uint64_t DEFAULT_MERGE_THRESHOLD_IN_COUNT;
    static const uint64_t MAX_MERGE_THRESHOLD_IN_COUNT;
    static const uint64_t DEFAULT_MERGE_THRESHOLD_IN_SIZE;
    static const int32_t DEFAULT_SCHEMA_VERSION;

public:
    SwiftWriterConfig();
    ~SwiftWriterConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);

    bool parseFromString(const std::string &configStr);
    bool isValidate() const;

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

private:
    bool parseMode(const std::string &modeStr);

public:
    std::string topicName;
    bool isSynchronous;
    uint32_t retryTimes;
    uint64_t retryTimeInterval;
    uint64_t oneRequestSendByteCount;
    uint64_t maxBufferSize;
    uint64_t maxKeepInBufferTime;
    uint64_t sendRequestLoopInterval;
    uint64_t waitLoopInterval;
    uint64_t brokerBusyWaitIntervalMin;
    uint64_t brokerBusyWaitIntervalMax;
    std::string functionChain;
    uint64_t waitFinishedWriterTime;
    uint64_t compressThresholdInBytes;
    uint64_t commitDetectionInterval;
    bool compress;
    bool compressMsg;
    bool compressMsgInBroker;
    bool safeMode;
    uint64_t syncSendTimeout;
    uint64_t asyncSendTimeout;
    uint32_t messageFormat;
    std::string zkPath;
    bool mergeMsg;
    bool needTimestamp;
    uint16_t mergeThresholdInCount; // only use 10 bit, other 6 bit is reserved
    uint64_t mergeThresholdInSize;
    int32_t schemaVersion;
    std::string writerIdentity;
    std::string accessId;
    std::string accessKey;
    std::string writerName;
    uint32_t majorVersion;
    uint32_t minorVersion;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftWriterConfig);

} // namespace client
} // namespace swift
