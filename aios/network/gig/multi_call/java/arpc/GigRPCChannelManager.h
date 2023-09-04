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
#ifndef ISEARCH_MULTI_CALL_GIGRPCCHANNELMANAGER_H
#define ISEARCH_MULTI_CALL_GIGRPCCHANNELMANAGER_H

#include "aios/network/arpc/arpc/ANetRPCChannelManager.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/java/arpc/GigRPCChannel.h"

namespace multi_call {

class GigRPCChannelManager : public arpc::ANetRPCChannelManager
{
public:
    GigRPCChannelManager(anet::Transport *transport = NULL);
    virtual ~GigRPCChannelManager();

private:
    GigRPCChannelManager(const GigRPCChannelManager &);
    GigRPCChannelManager &operator=(const GigRPCChannelManager &);

public:
    // queueSize : default queue size for rpcchannel
    // timout    : default timeout for rpcchannel(miliseconds)
    RPCChannel *OpenChannel(const std::string &address, bool block = false, size_t queueSize = 50ul,
                            int timeout = 5000, bool autoReconn = true,
                            anet::CONNPRIORITY prio = anet::ANET_PRIORITY_NORMAL) override;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigRPCChannelManager);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGRPCCHANNELMANAGER_H
