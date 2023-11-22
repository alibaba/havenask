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

#include <map>
#include <memory>
#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "swift/common/Common.h"

namespace anet {
class Transport;
} // namespace anet
namespace arpc {
class ANetRPCChannel;
class ANetRPCChannelManager;
} // namespace arpc

namespace swift {
namespace network {

typedef std::shared_ptr<arpc::ANetRPCChannel> ANetRpcChannelPtr;
struct CacheChannelItem {
    CacheChannelItem() : lastAccessTime(0), timeoutErrorCount(0) {}
    ANetRpcChannelPtr channel;
    int64_t lastAccessTime;
    int64_t timeoutErrorCount;
};

class SwiftRpcChannelManager {
public:
    SwiftRpcChannelManager(uint32_t ioThreadNum = 1, uint32_t rpcSendQueueSize = 200, uint32_t rpcTimeout = 5000);
    ~SwiftRpcChannelManager();

private:
    SwiftRpcChannelManager(const SwiftRpcChannelManager &);
    SwiftRpcChannelManager &operator=(const SwiftRpcChannelManager &);

public:
    bool init(arpc::ANetRPCChannelManager *channelManager);
    ANetRpcChannelPtr openChannel(const std::string &address, bool block = false, bool autoReconn = true);
    void channelTimeout(const std::string &address);

private:
    void clearUselessChannel();
    void cleanChannel();

private:
    autil::ThreadMutex _mutex;
    std::map<std::string, CacheChannelItem> _channelCache;
    bool _ownChannelManager;
    arpc::ANetRPCChannelManager *_channelManager;
    anet::Transport *_transport;
    int64_t _reserveTime;
    autil::LoopThreadPtr _cleanThreadPtr;
    uint32_t _ioThreadNum;
    uint32_t _rpcSendQueueSize;
    uint32_t _rpcTimeout;

private:
    constexpr static int32_t TIMEOUT_ERROR_GAP_US = 5000000; // 5s
    constexpr static int32_t MAX_TIMEOUT_ERROR_COUNT = 3;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftRpcChannelManager);

} // namespace network
} // namespace swift
