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
#include "swift/client/SwiftClientConfig.h"

#include <cstddef>
#include <cstdint>
#include <vector>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"

namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, SwiftClientConfig);
using namespace autil;
using namespace std;

const uint32_t SwiftClientConfig::DEFAULT_TRY_TIMES = 3;
const int64_t SwiftClientConfig::DEFAULT_RETRY_TIME_INTERVAL = 3 * 1000 * 1000; // 3s
const uint32_t SwiftClientConfig::MIN_TRY_TIMES = 1;
const int64_t SwiftClientConfig::MIN_RETRY_TIME_INTERVAL = 100 * 1000;         // 100ms
const int64_t SwiftClientConfig::DEFAULT_ADMIN_TIMEOUT = 3 * 1000;             // 3s
const int64_t SwiftClientConfig::DEFAULT_ADMIN_REFRESH_TIME = 3 * 1000 * 1000; // 3s
const int64_t SwiftClientConfig::DEFAULT_MAX_WRITE_BUFFER_SIZE_MB = -1;        // compatible old behavior
const double SwiftClientConfig::DEFAULT_TEMP_WRITE_BUFFER_PERCENT =
    256; // compatible old behavior 256M, if < 1.0, use max_buffer_size * percent
const int64_t SwiftClientConfig::DEFAULT_FB_WRITER_RECYCLE_SIZE_KB = 10 * 1024; // 10 mb
const int64_t SwiftClientConfig::DEFAULT_BUFFER_BLOCK_SIZE_KB = 64;
const uint32_t SwiftClientConfig::DEFAULT_MERGE_MESSAGE_THREAD_NUM = 3;
const uint32_t SwiftClientConfig::DEFAULT_IO_THREAD_NUM = 1;
const uint32_t SwiftClientConfig::DEFAULT_RPC_SEND_QUEUE_SIZE = 200;
const uint32_t SwiftClientConfig::DEFAULT_RPC_TIMEOUT = 5000; // 5s
const std::string SwiftClientConfig::CLIENT_CONFIG_MULTI_SEPERATOR = "##";
const int64_t SwiftClientConfig::DEFAULT_LATELY_ERROR_TIME_MIN_INTERVAL_US = 300 * 1000;   // 0.3 s
const int64_t SwiftClientConfig::DEFAULT_LATELY_ERROR_TIME_MAX_INTERVAL_US = 10000 * 1000; // 10 s

SwiftClientConfig::SwiftClientConfig(uint32_t retryTimes_,
                                     int64_t retryInterval_,
                                     int64_t maxBufferSize_,
                                     int64_t bufferBlockSize_)
    : retryTimes(retryTimes_)
    , retryTimeInterval(retryInterval_)
    , useFollowerAdmin(true)
    , tracingMsgLevel(0)
    , adminTimeout(DEFAULT_ADMIN_TIMEOUT)
    , refreshTime(DEFAULT_ADMIN_REFRESH_TIME)
    , maxWriteBufferSizeMb(maxBufferSize_)
    , tempWriteBufferPercent(DEFAULT_TEMP_WRITE_BUFFER_PERCENT)
    , bufferBlockSizeKb(bufferBlockSize_)
    , mergeMessageThreadNum(DEFAULT_MERGE_MESSAGE_THREAD_NUM)
    , ioThreadNum(DEFAULT_IO_THREAD_NUM)
    , rpcSendQueueSize(DEFAULT_RPC_SEND_QUEUE_SIZE)
    , rpcTimeout(DEFAULT_RPC_TIMEOUT)
    , fbWriterRecycleSizeKb(DEFAULT_FB_WRITER_RECYCLE_SIZE_KB)
    , latelyErrorTimeMinIntervalUs(DEFAULT_LATELY_ERROR_TIME_MIN_INTERVAL_US)
    , latelyErrorTimeMaxIntervalUs(DEFAULT_LATELY_ERROR_TIME_MAX_INTERVAL_US) {}

SwiftClientConfig::~SwiftClientConfig() {}

bool SwiftClientConfig::parseFromString(const std::string &configStr) {
    try {
        autil::legacy::FromJsonString(*this, configStr);
        return !zkPath.empty();
    } catch (const autil::legacy::ExceptionBase &e) {}
    StringTokenizer st(
        configStr, CLIENT_CONFIG_SEPERATOR, StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        size_t pos = it->find(CLIENT_CONFIG_KV_SEPERATOR);
        if (pos == string::npos) {
            AUTIL_LOG(ERROR, "Invalid config item: [%s]", (*it).c_str());
            return false;
        }
        string key = it->substr(0, pos);
        string valueStr = it->substr(pos + 1);
        if (key == CLIENT_CONFIG_RETRY_TIME) {
            if (!StringUtil::strToUInt32(valueStr.c_str(), retryTimes)) {
                return false;
            }
        } else if (key == CLIENT_CONFIG_RETRY_TIME_INTERVAL) {
            if (!StringUtil::strToInt64(valueStr.c_str(), retryTimeInterval)) {
                return false;
            }
        } else if (key == CLIENT_CONFIG_MAX_WRITE_BUFFER_SIZE_MB) {
            if (!StringUtil::strToInt64(valueStr.c_str(), maxWriteBufferSizeMb)) {
                return false;
            }
        } else if (key == CLIENT_CONFIG_TEMP_WRITE_BUFFER_PERCENT) {
            if (!StringUtil::strToDouble(valueStr.c_str(), tempWriteBufferPercent)) {
                return false;
            }
        } else if (key == CLIENT_CONFIG_BUFFER_BLOCK_SIZE_KB) {
            if (!StringUtil::strToInt64(valueStr.c_str(), bufferBlockSizeKb)) {
                return false;
            }
        } else if (key == CLIENT_CONFIG_LATELY_ERROR_TIME_MIN_INTERVAL_US) {
            if (!StringUtil::strToInt64(valueStr.c_str(), latelyErrorTimeMinIntervalUs)) {
                return false;
            }
        } else if (key == CLIENT_CONFIG_LATELY_ERROR_TIME_MAX_INTERVAL_US) {
            if (!StringUtil::strToInt64(valueStr.c_str(), latelyErrorTimeMaxIntervalUs)) {
                return false;
            }
        } else if (key == CLIENT_CONFIG_FB_BUFFER_RECYCLE_SIZE_KB) {
            if (!StringUtil::strToInt64(valueStr.c_str(), fbWriterRecycleSizeKb)) {
                return false;
            }
        } else if (key == CLIENT_CONFIG_MERGE_MESSAGE_THREAD_NUM) {
            if (!StringUtil::strToUInt32(valueStr.c_str(), mergeMessageThreadNum)) {
                return false;
            }
        } else if (key == CLIENT_CONFIG_IO_THREAD_NUM) {
            if (!StringUtil::strToUInt32(valueStr.c_str(), ioThreadNum)) {
                return false;
            }
        } else if (key == CLIENT_CONFIG_RPC_SEND_QUEUE_SIZE) {
            if (!StringUtil::strToUInt32(valueStr.c_str(), rpcSendQueueSize)) {
                return false;
            }
        } else if (key == CLIENT_CONFIG_RPC_TIMEOUT) {
            if (!StringUtil::strToUInt32(valueStr.c_str(), rpcTimeout)) {
                return false;
            }
        } else if (key == CLIENT_CONFIG_ZK_PATH) {
            zkPath = valueStr;
        } else if (key == CLIENT_CONFIG_LOG_CONFIG_FILE) {
            logConfigFile = valueStr;
        } else if (key == CLIENT_CONFIG_USE_FOLLOWER_ADMIN) {
            bool ret = true;
            if (StringUtil::parseTrueFalse(valueStr, ret)) {
                useFollowerAdmin = ret;
            }
        } else if (key == CLIENT_CONFIG_TRACING_MSG_LEVEL) {
            if (!StringUtil::strToUInt32(valueStr.c_str(), tracingMsgLevel)) {
                return false;
            }
        } else if (key == CLIENT_CONFIG_CLIENT_IDENTITY) {
            clientIdentity = valueStr;
        } else if (key == CLIENT_CONFIG_ADMIN_TIMEOUT) {
            if (!StringUtil::strToInt64(valueStr.c_str(), adminTimeout)) {
                return false;
            }
        } else if (key == CLIENT_CONFIG_ADMIN_REFRESH_TIME) {
            if (!StringUtil::strToInt64(valueStr.c_str(), refreshTime)) {
                return false;
            }
        } else if (key == CLIENT_CONFIG_AUTHENTICATION_USERNAME) {
            username = valueStr;
        } else if (key == CLIENT_CONFIG_AUTHENTICATION_PASSWD) {
            passwd = valueStr;
        } else {
            AUTIL_LOG(ERROR, "Unknown config key : [%s]", key.c_str());
            return false;
        }
    }
    if (latelyErrorTimeMaxIntervalUs < latelyErrorTimeMinIntervalUs) {
        AUTIL_LOG(ERROR,
                  "error max [%ld] interval small than min [%ld]",
                  latelyErrorTimeMaxIntervalUs,
                  latelyErrorTimeMinIntervalUs);
        return false;
    }
    return !zkPath.empty();
}

void SwiftClientConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize(CLIENT_CONFIG_RETRY_TIME, retryTimes, retryTimes);
    json.Jsonize(CLIENT_CONFIG_RETRY_TIME_INTERVAL, retryTimeInterval, retryTimeInterval);
    json.Jsonize(CLIENT_CONFIG_ZK_PATH, zkPath);
    json.Jsonize(CLIENT_CONFIG_LOG_CONFIG_FILE, logConfigFile, logConfigFile);
    json.Jsonize(CLIENT_CONFIG_USE_FOLLOWER_ADMIN, useFollowerAdmin, useFollowerAdmin);
    json.Jsonize(CLIENT_CONFIG_TRACING_MSG_LEVEL, tracingMsgLevel, tracingMsgLevel);
    json.Jsonize(CLIENT_CONFIG_ADMIN_TIMEOUT, adminTimeout, adminTimeout);
    json.Jsonize(CLIENT_CONFIG_ADMIN_REFRESH_TIME, refreshTime, refreshTime);
    json.Jsonize(CLIENT_CONFIG_MAX_WRITE_BUFFER_SIZE_MB, maxWriteBufferSizeMb, maxWriteBufferSizeMb);
    json.Jsonize(CLIENT_CONFIG_BUFFER_BLOCK_SIZE_KB, bufferBlockSizeKb, bufferBlockSizeKb);
    json.Jsonize(CLIENT_CONFIG_FB_BUFFER_RECYCLE_SIZE_KB, fbWriterRecycleSizeKb, fbWriterRecycleSizeKb);
    json.Jsonize(CLIENT_CONFIG_MERGE_MESSAGE_THREAD_NUM, mergeMessageThreadNum, mergeMessageThreadNum);
    json.Jsonize(CLIENT_CONFIG_TEMP_WRITE_BUFFER_PERCENT, tempWriteBufferPercent, tempWriteBufferPercent);
    json.Jsonize(CLIENT_CONFIG_IO_THREAD_NUM, ioThreadNum, ioThreadNum);
    json.Jsonize(CLIENT_CONFIG_RPC_TIMEOUT, rpcTimeout, rpcTimeout);
    json.Jsonize(CLIENT_CONFIG_RPC_SEND_QUEUE_SIZE, rpcSendQueueSize, rpcSendQueueSize);
    json.Jsonize(CLIENT_CONFIG_CLIENT_IDENTITY, clientIdentity, clientIdentity);
    json.Jsonize(
        CLIENT_CONFIG_LATELY_ERROR_TIME_MAX_INTERVAL_US, latelyErrorTimeMaxIntervalUs, latelyErrorTimeMaxIntervalUs);
    json.Jsonize(
        CLIENT_CONFIG_LATELY_ERROR_TIME_MIN_INTERVAL_US, latelyErrorTimeMinIntervalUs, latelyErrorTimeMinIntervalUs);
    json.Jsonize(CLIENT_CONFIG_AUTHENTICATION_USERNAME, username, username);
    json.Jsonize(CLIENT_CONFIG_AUTHENTICATION_PASSWD, passwd, passwd);
}

} // namespace client
} // namespace swift
