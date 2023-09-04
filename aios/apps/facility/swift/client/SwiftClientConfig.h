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

namespace swift {
namespace client {

constexpr char CLIENT_CONFIG_SEPERATOR[] = ";";
constexpr char CLIENT_CONFIG_KV_SEPERATOR[] = "=";
constexpr char CLIENT_CONFIG_ZK_PATH[] = "zkPath";
constexpr char CLIENT_CONFIG_LOG_CONFIG_FILE[] = "logConfigFile";
constexpr char CLIENT_CONFIG_RETRY_TIME[] = "retryTime";
constexpr char CLIENT_CONFIG_RETRY_TIME_INTERVAL[] = "retryInterval";
constexpr char CLIENT_CONFIG_USE_FOLLOWER_ADMIN[] = "useFollowerAdmin";
constexpr char CLIENT_CONFIG_TRACING_MSG_LEVEL[] = "tracingMsgLevel";
constexpr char CLIENT_CONFIG_ADMIN_TIMEOUT[] = "adminTimeout";
constexpr char CLIENT_CONFIG_ADMIN_REFRESH_TIME[] = "refreshTime";
constexpr char CLIENT_CONFIG_MAX_WRITE_BUFFER_SIZE_MB[] = "maxWriteBufferSizeMb";
constexpr char CLIENT_CONFIG_TEMP_WRITE_BUFFER_PERCENT[] = "tempWriteBufferPercent";
constexpr char CLIENT_CONFIG_BUFFER_BLOCK_SIZE_KB[] = "bufferBlockSizeKb";
constexpr char CLIENT_CONFIG_FB_BUFFER_RECYCLE_SIZE_KB[] = "fbWriterRecycleSizeKb";
constexpr char CLIENT_CONFIG_MERGE_MESSAGE_THREAD_NUM[] = "mergeMessageThreadNum";
constexpr char CLIENT_CONFIG_CLIENT_IDENTITY[] = "clientIdentity";
constexpr char CLIENT_CONFIG_IO_THREAD_NUM[] = "ioThreadNum";
constexpr char CLIENT_CONFIG_RPC_SEND_QUEUE_SIZE[] = "rpcSendQueueSize";
constexpr char CLIENT_CONFIG_RPC_TIMEOUT[] = "rpcTimeout";
constexpr char CLIENT_CONFIG_LATELY_ERROR_TIME_MIN_INTERVAL_US[] = "latelyErrorTimeMinIntervalUs";
constexpr char CLIENT_CONFIG_LATELY_ERROR_TIME_MAX_INTERVAL_US[] = "latelyErrorTimeMaxIntervalUs";
constexpr char CLIENT_CONFIG_AUTHENTICATION_USERNAME[] = "username";
constexpr char CLIENT_CONFIG_AUTHENTICATION_PASSWD[] = "passwd";

class SwiftClientConfig : public autil::legacy::Jsonizable {
public:
    static const uint32_t DEFAULT_TRY_TIMES;
    static const int64_t DEFAULT_RETRY_TIME_INTERVAL;
    static const uint32_t MIN_TRY_TIMES;
    static const int64_t MIN_RETRY_TIME_INTERVAL;
    static const int64_t DEFAULT_ADMIN_TIMEOUT;
    static const int64_t DEFAULT_ADMIN_REFRESH_TIME;
    static const int64_t DEFAULT_MAX_WRITE_BUFFER_SIZE_MB;
    static const double DEFAULT_TEMP_WRITE_BUFFER_PERCENT;
    static const int64_t DEFAULT_BUFFER_BLOCK_SIZE_KB;
    static const int64_t DEFAULT_FB_WRITER_RECYCLE_SIZE_KB;
    static const uint32_t DEFAULT_MERGE_MESSAGE_THREAD_NUM;
    static const uint32_t DEFAULT_IO_THREAD_NUM;
    static const uint32_t DEFAULT_RPC_SEND_QUEUE_SIZE;
    static const uint32_t DEFAULT_RPC_TIMEOUT;
    static const std::string CLIENT_CONFIG_MULTI_SEPERATOR;
    static const int64_t DEFAULT_LATELY_ERROR_TIME_MIN_INTERVAL_US;
    static const int64_t DEFAULT_LATELY_ERROR_TIME_MAX_INTERVAL_US;

public:
    SwiftClientConfig(uint32_t retryTimes_ = DEFAULT_TRY_TIMES,
                      int64_t retryTimeInterval_ = DEFAULT_RETRY_TIME_INTERVAL,
                      int64_t maxWriteBufferSizeMb_ = DEFAULT_MAX_WRITE_BUFFER_SIZE_MB,
                      int64_t bufferBlockSizeKb_ = DEFAULT_BUFFER_BLOCK_SIZE_KB);
    ~SwiftClientConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool parseFromString(const std::string &configStr);

private:
    AUTIL_LOG_DECLARE();

public:
    uint32_t retryTimes;
    int64_t retryTimeInterval;
    std::string zkPath;
    std::string logConfigFile;
    std::string clientIdentity;
    bool useFollowerAdmin;
    uint32_t tracingMsgLevel;
    int64_t adminTimeout;
    int64_t refreshTime;
    int64_t maxWriteBufferSizeMb;
    double tempWriteBufferPercent;
    int64_t bufferBlockSizeKb;
    uint32_t mergeMessageThreadNum;
    uint32_t ioThreadNum;
    uint32_t rpcSendQueueSize;
    uint32_t rpcTimeout;
    int64_t fbWriterRecycleSizeKb;
    int64_t latelyErrorTimeMinIntervalUs;
    int64_t latelyErrorTimeMaxIntervalUs;
    std::string username;
    std::string passwd;
};

SWIFT_TYPEDEF_PTR(SwiftClientConfig);

} // namespace client
} // namespace swift
