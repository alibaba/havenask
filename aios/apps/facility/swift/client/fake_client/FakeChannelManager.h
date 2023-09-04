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

#include <stddef.h>
#include <string>

#include "aios/network/anet/connectionpriority.h"
#include "arpc/ANetRPCChannelManager.h"
#include "arpc/CommonMacros.h"
#include "arpc/anet/ANetApp.h"
#include "autil/Log.h"
#include "autil/ThreadPool.h"
#include "swift/client/fake_client/FakeRpcChannel.h"
#include "swift/common/Common.h"

namespace swift {
namespace client {

class FakeChannelManager : public arpc::ANetRPCChannelManager {
public:
    FakeChannelManager();
    ~FakeChannelManager();

private:
    FakeChannelManager(const FakeChannelManager &);
    FakeChannelManager &operator=(const FakeChannelManager &);

public:
    virtual RPCChannel *OpenChannel(const std::string &address,
                                    bool block = false,
                                    size_t queueSize = 50ul,
                                    int timeout = 5000,
                                    bool autoReconn = true,
                                    anet::CONNPRIORITY prio = anet::ANET_PRIORITY_NORMAL) {
        (void)address;
        (void)block;
        (void)queueSize;
        (void)timeout;
        (void)autoReconn;
        (void)prio;
        std::string address1 = "tcp:0.0.0.0:0";
        return new FakeRpcChannel(&_threadPool, _anetApp->Connect(address1, true, anet::ANET_PRIORITY_NORMAL));
    }
    void stop() { _threadPool.stop(); }

private:
    autil::ThreadPool _threadPool;
    arpc::ANetApp *_anetApp;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FakeChannelManager);
} // namespace client
} // namespace swift
