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
#include "aios/network/gig/multi_call/java/arpc/GigRPCChannelManager.h"

using namespace std;
using namespace arpc;
using namespace anet;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigRPCChannelManager);

GigRPCChannelManager::GigRPCChannelManager(Transport *transport)
    : ANetRPCChannelManager(transport) {
}

GigRPCChannelManager::~GigRPCChannelManager() {
}

RPCChannel *GigRPCChannelManager::OpenChannel(const std::string &address, bool block,
                                              size_t queueSize, int timeout, bool autoReconn,
                                              anet::CONNPRIORITY prio) {
    Connection *pConn = Connect(address, block, queueSize, timeout, autoReconn, prio);

    if (pConn == NULL) {
        AUTIL_LOG(ERROR, "connection is NULL, address: [%s]", address.c_str());
        return NULL;
    }

    GigRPCMessageCodec *gigMessageCodec = new GigRPCMessageCodec(_anetApp.GetPacketFactory());
    GigRPCChannel *pChannel = new GigRPCChannel(pConn, gigMessageCodec, block);
    return pChannel;
}

} // namespace multi_call
