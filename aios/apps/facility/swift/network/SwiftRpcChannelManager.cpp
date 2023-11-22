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
#include "swift/network/SwiftRpcChannelManager.h"

#include <cstddef>
#include <functional>
#include <utility>

#include "aios/network/anet/transport.h"
#include "arpc/ANetRPCChannel.h"
#include "arpc/ANetRPCChannelManager.h"
#include "arpc/CommonMacros.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;
using namespace arpc;
namespace swift {
namespace network {
AUTIL_LOG_SETUP(swift, SwiftRpcChannelManager);

SwiftRpcChannelManager::SwiftRpcChannelManager(uint32_t ioThreadNum, uint32_t rpcSendQueueSize, uint32_t rpcTimeout)
    : _ownChannelManager(false)
    , _channelManager(NULL)
    , _transport(NULL)
    , _reserveTime(60 * 10 * 1000 * 1000) // 600s
    , _ioThreadNum(ioThreadNum)
    , _rpcSendQueueSize(rpcSendQueueSize)
    , _rpcTimeout(rpcTimeout) {}

SwiftRpcChannelManager::~SwiftRpcChannelManager() {
    _cleanThreadPtr.reset();
    cleanChannel();
    if (_ownChannelManager) {
        _transport->stop();
        _transport->wait();
        SWIFT_DELETE(_channelManager);
        SWIFT_DELETE(_transport);
        _ownChannelManager = false;
    }
}

bool SwiftRpcChannelManager::init(arpc::ANetRPCChannelManager *channelManager) {
    _cleanThreadPtr = autil::LoopThread::createLoopThread(
        std::bind(&SwiftRpcChannelManager::clearUselessChannel, this), 120 * 1000 * 1000, "clear_loop"); // 120s
    if (!_cleanThreadPtr) {
        AUTIL_LOG(ERROR, "failed to start clean channel  thread.");
        return false;
    }
    _channelManager = channelManager;
    _ownChannelManager = false;
    if (_channelManager == NULL) {
        _transport = new anet::Transport(_ioThreadNum);
        _channelManager = new arpc::ANetRPCChannelManager(_transport);
        _transport->start();
        _transport->setName("Swift");
        _ownChannelManager = true;
    }
    return true;
}

ANetRpcChannelPtr SwiftRpcChannelManager::openChannel(const string &address, bool block, bool autoReconn) {
    ScopedLock lock(_mutex);
    std::map<std::string, CacheChannelItem>::iterator iter = _channelCache.find(address);
    if (iter != _channelCache.end()) {
        if (iter->second.channel != NULL) {
            if (!iter->second.channel->ChannelBroken()) {
                iter->second.lastAccessTime = TimeUtility::currentTime();
                AUTIL_LOG(INFO, "reuse channel on [%s] success", address.c_str());
                return iter->second.channel;
            } else if (TimeUtility::currentTime() - iter->second.lastAccessTime < 500000) {
                AUTIL_LOG(INFO, "channel on [%s] is broken, retry later", address.c_str());
                return ANetRpcChannelPtr();
            }
        }
    }
    RPCChannel *rpcChannel =
        _channelManager->OpenChannel(address.c_str(), block, _rpcSendQueueSize, _rpcTimeout, autoReconn);
    arpc::ANetRPCChannel *anetRpcChannel = dynamic_cast<ANetRPCChannel *>(rpcChannel);
    if (anetRpcChannel == NULL) {
        AUTIL_LOG(ERROR, "open channel on [%s] failed", address.c_str());
        return ANetRpcChannelPtr();
    }
    AUTIL_LOG(INFO, "open channel on [%s] success", address.c_str());
    ANetRpcChannelPtr channel(anetRpcChannel);
    CacheChannelItem item;
    item.lastAccessTime = TimeUtility::currentTime();
    item.timeoutErrorCount = 0;
    item.channel = channel;
    _channelCache[address] = item;
    return channel;
}

void SwiftRpcChannelManager::channelTimeout(const std::string &address) {
    ScopedLock lock(_mutex);
    auto iter = _channelCache.find(address);
    if (iter == _channelCache.end()) {
        return;
    }
    auto &channelItem = iter->second;
    if (TimeUtility::currentTime() - channelItem.lastAccessTime <= TIMEOUT_ERROR_GAP_US) {
        if (++channelItem.timeoutErrorCount >= MAX_TIMEOUT_ERROR_COUNT) {
            _channelCache.erase(iter);
        }
    }
}

void SwiftRpcChannelManager::cleanChannel() {
    ScopedLock lock(_mutex);
    std::map<std::string, CacheChannelItem>::iterator iter = _channelCache.begin();
    for (; iter != _channelCache.end(); iter++) {
        if (iter->second.channel.use_count() != 1) {
            AUTIL_LOG(
                WARN, "channel on [%s] use count [%d]", iter->first.c_str(), (int)iter->second.channel.use_count());
        }
    }
    _channelCache.clear();
}

void SwiftRpcChannelManager::clearUselessChannel() {
    ScopedLock lock(_mutex);
    std::map<std::string, CacheChannelItem>::iterator iter = _channelCache.begin();
    int64_t currentTime = TimeUtility::currentTime();
    for (; iter != _channelCache.end();) {
        AUTIL_LOG(DEBUG, "[%s] use count [%d].", iter->first.c_str(), (int)iter->second.channel.use_count());
        if (iter->second.channel.use_count() == 1 && currentTime - iter->second.lastAccessTime > _reserveTime) {
            AUTIL_LOG(INFO, "clear channel on [%s].", iter->first.c_str());
            iter = _channelCache.erase(iter);
        } else {
            iter++;
        }
    }
}

} // namespace network
} // namespace swift
