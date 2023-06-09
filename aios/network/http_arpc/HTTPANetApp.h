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
#ifndef HTTP_ARPC_HTTPANETAPP_H
#define HTTP_ARPC_HTTPANETAPP_H

#include <string>
#include "aios/network/anet/anet.h"
#if !defined ANET_VERSION || ANET_VERSION < 010300
#error "From this version, HTTP_ARPC requires ANET >= 1.3.0. Please check your ANET version."
#endif

namespace http_arpc {

class HTTPANetApp
{
public:
    HTTPANetApp(anet::Transport *transport);
    ~HTTPANetApp();
public:
    anet::IOComponent* Listen(const std::string &address,
                              anet::IServerAdapter *serverAdapter,
                              int timeout,
                              int maxIdleTime,
                              int backlog);

    bool OwnTransport() {
        return _ownTransport;
    }

    bool StartPrivateTransport();
    bool StopPrivateTransport();

private:
    bool _ownTransport;
    anet::Transport *_transport;
    anet::HTTPPacketFactory _factory;
    anet::HTTPStreamer _streamer;
};

}

#endif // HTTP_ARPC_HTTPANETAPP_H
