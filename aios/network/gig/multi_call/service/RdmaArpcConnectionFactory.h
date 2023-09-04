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

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/service/ConnectionFactory.h"
#include "aios/network/gig/multi_call/service/RdmaArpcConnection.h"
#include "aios/network/rdma/arpc/RdmaRPCChannelManager.h"

namespace multi_call {

class RdmaArpcConnectionFactory : public ConnectionFactory
{
public:
    RdmaArpcConnectionFactory(const std::shared_ptr<arpc::RdmaRPCChannelManager> &channelManager,
                              size_t queueSize)
        : ConnectionFactory(queueSize)
        , _rpcChannelManager(channelManager) {
    }
    ~RdmaArpcConnectionFactory() {
    }

public:
    ConnectionPtr createConnection(const std::string &spec) override {
        return ConnectionPtr(new RdmaArpcConnection(_rpcChannelManager, spec, _queueSize));
    }

private:
    std::shared_ptr<arpc::RdmaRPCChannelManager> _rpcChannelManager;
};

} // namespace multi_call