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
#include "HTTPANetApp.h"

#include "Log.h"

using namespace std;
using namespace anet;

namespace http_arpc {

HTTP_ARPC_DECLARE_AND_SETUP_LOGGER(HTTPANetApp);

HTTPANetApp::HTTPANetApp(Transport *transport) : _streamer(&_factory) {
    _ownTransport = false;

    if (transport == NULL) {
        _transport = new Transport;
        _ownTransport = true;
    } else {
        _transport = transport;
    }
}

HTTPANetApp::~HTTPANetApp() {
    if (_ownTransport) {
        StopPrivateTransport();
        delete _transport;
        _transport = NULL;
    }
}

IOComponent *HTTPANetApp::Listen(
    const std::string &address, IServerAdapter *serverAdapter, int timeout, int maxIdleTime, int backlog) {
    return _transport->listen(address.c_str(), &_streamer, serverAdapter, timeout, maxIdleTime, backlog);
}

bool HTTPANetApp::StartPrivateTransport() {
    if (!_ownTransport) {
        return false;
    }

    _transport->start();
    _transport->setName("HttpArpc");
    return true;
}

bool HTTPANetApp::StopPrivateTransport() {
    if (!_ownTransport) {
        return false;
    }
    _transport->stop();
    _transport->wait();

    return true;
}

} // namespace http_arpc
