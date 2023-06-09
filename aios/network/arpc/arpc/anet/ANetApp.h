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
#ifndef ARPC_ANET_APP_H
#define ARPC_ANET_APP_H
#include <iosfwd>
#include <string>
#include <vector>

#include "aios/network/anet/anet.h"
#include "aios/network/arpc/arpc/util/Log.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/anet/connectionpriority.h"
#include "aios/network/anet/defaultpacketstreamer.h"

ARPC_BEGIN_NAMESPACE(arpc);

class ANetApp
{
public:
    ANetApp(anet::Transport *transport);
    ~ANetApp();
public:
    anet::IOComponent *Listen(const std::string &address,
                              anet::IServerAdapter *serverAdapter,
                              int timeout,
                              int maxIdleTime,
                              int backlog);

    anet::Connection *Connect(const std::string &address,
                              bool autoReconn, anet::CONNPRIORITY prio);

    bool OwnTransport()
    {
        return _ownTransport;
    }

    bool StartPrivateTransport();
    bool StartPrivateTransport(const std::string &name);
    bool StopPrivateTransport();
    anet::IPacketFactory *GetPacketFactory()
    {
        return &_factory;
    }

    anet::Connection *Connect(const std::string &address, const std::string &bindAddr,
                              bool autoReconn, anet::CONNPRIORITY prio);
    void dump(std::ostringstream &out);
private:
    bool _ownTransport;
    anet::Transport *_transport;
    anet::AdvanceDelayDecodePacketFactory _factory;
    anet::DefaultPacketStreamer _streamer;
};

ARPC_END_NAMESPACE(arpc);

#endif //ARPC_ANET_APP_H
