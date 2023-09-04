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

#include <memory>
#include <mutex>

#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "autil/NoCopyable.h"

namespace anet {
class Transport;
}

namespace arpc {

class ANetRPCChannelManager;
class ANetRPCChannel;

class PooledChannelManager : public autil::NoCopyable {
public:
    PooledChannelManager(int ioThreadNum = 1);
    virtual ~PooledChannelManager();

private:
    PooledChannelManager(const PooledChannelManager &);
    PooledChannelManager &operator=(const PooledChannelManager &);

public:
    virtual std::shared_ptr<ANetRPCChannel> getRpcChannel(const std::string &addr);
    void setTimeout(int timeout) { _timeout = timeout; }

private:
    std::shared_ptr<ANetRPCChannel> createRpcChannel(const std::string &spec);
    void cleanBrokenChannelLoop();

private:
    static std::string convertToSpec(const std::string &addr);

private:
    std::mutex _mu;
    int _timeout = 5000;
    std::unique_ptr<anet::Transport> _transport;
    std::unique_ptr<arpc::ANetRPCChannelManager> _rpcChannelManager;
    std::map<std::string, std::shared_ptr<ANetRPCChannel>> _rpcChannelPool;
    autil::LoopThreadPtr _loopThreadPtr;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace arpc
