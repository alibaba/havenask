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

#include "arpc/PooledChannelManager.h"

#include "arpc/ANetRPCChannel.h"
#include "arpc/ANetRPCChannelManager.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace arpc {

AUTIL_LOG_SETUP(arpc, PooledChannelManager);

PooledChannelManager::PooledChannelManager(int ioThreadNum) {
    _transport = make_unique<anet::Transport>(ioThreadNum);
    _transport->start();
    _rpcChannelManager = make_unique<ANetRPCChannelManager>(_transport.get());
}

PooledChannelManager::~PooledChannelManager() {
    _loopThreadPtr.reset();
    _rpcChannelPool.clear();
    _transport->stop();
    _transport->wait();
}

shared_ptr<ANetRPCChannel> PooledChannelManager::getRpcChannel(const std::string &addr) {
    unique_lock<mutex> lock(_mu);
    if (_loopThreadPtr.get() == NULL) {
        _loopThreadPtr = LoopThread::createLoopThread(
            bind(&PooledChannelManager::cleanBrokenChannelLoop, this), 10 * 1000 * 1000, "cleanChannel");
    }
    auto it = _rpcChannelPool.find(addr);
    if (it != _rpcChannelPool.end()) {
        auto ch = it->second.get();
        if (ch->ChannelBroken()) {
            _rpcChannelPool.erase(it);
            return nullptr;
        } else {
            return it->second;
        }
    }

    shared_ptr<ANetRPCChannel> rpcChannel = createRpcChannel(addr);
    if (rpcChannel) {
        _rpcChannelPool[addr] = rpcChannel;
    }

    return rpcChannel;
}

void PooledChannelManager::cleanBrokenChannelLoop() {
    unique_lock<mutex> lock(_mu);
    for (auto it = _rpcChannelPool.begin(); it != _rpcChannelPool.end();) {
        auto ch = it->second.get();
        if (ch->ChannelBroken()) {
            _rpcChannelPool.erase(it++);
        } else {
            it++;
        }
    }
}

shared_ptr<ANetRPCChannel> PooledChannelManager::createRpcChannel(const string &addr) {
    string spec = convertToSpec(addr);
    bool block = false;
    size_t queueSize = 50ul;
    bool autoReconnect = false;
    shared_ptr<ANetRPCChannel> rpcChannel(
        (ANetRPCChannel *)_rpcChannelManager->OpenChannel(spec, block, queueSize, _timeout, autoReconnect));
    if (rpcChannel == NULL) {
        AUTIL_LOG(ERROR, "open channel on [%s] failed", spec.c_str());
    }
    return rpcChannel;
}

string PooledChannelManager::convertToSpec(const string &addr) {
    if (StringUtil::startsWith(addr, "tcp:") || StringUtil::startsWith(addr, "udp:") ||
        StringUtil::startsWith(addr, "http:")) {
        return addr;
    } else {
        return "tcp:" + addr;
    }
}

} // namespace arpc
