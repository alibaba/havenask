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
#include "aios/network/arpc/arpc/anet/ANetApp.h"

#include <cstddef>
#include <iosfwd>
#include <string>
#include <vector>

#include "aios/network/anet/anet.h"
#include "aios/network/anet/connectionpriority.h"
#include "aios/network/anet/defaultpacketstreamer.h"
#include "aios/network/anet/runnable.h"
#include "aios/network/anet/transport.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/util/Log.h"

using namespace std;
using namespace anet;

ARPC_BEGIN_NAMESPACE(arpc);
ARPC_DECLARE_AND_SETUP_LOGGER(ANetApp);

ANetApp::ANetApp(Transport *transport) : _streamer(&_factory) {
    _ownTransport = false;

    if (transport == NULL) {
        _transport = new Transport();
        _ownTransport = true;
    } else {
        _transport = transport;
    }
}

ANetApp::~ANetApp() {
    if (_ownTransport) {
        StopPrivateTransport();
        delete _transport;
        _transport = NULL;
    }
}

IOComponent *
ANetApp::Listen(const std::string &address, IServerAdapter *serverAdapter, int timeout, int maxIdleTime, int backlog) {
    return _transport->listen(address.c_str(), &_streamer, serverAdapter, timeout, maxIdleTime, backlog);
}

Connection *ANetApp::Connect(const std::string &address, bool autoReconn, anet::CONNPRIORITY prio) {
    return _transport->connect(address.c_str(), &_streamer, autoReconn, prio);
}

Connection *
ANetApp::Connect(const std::string &address, const std::string &bindAddr, bool autoReconn, anet::CONNPRIORITY prio) {
    return _transport->connectWithAddr(bindAddr.c_str(), address.c_str(), &_streamer, autoReconn, prio);
}

bool ANetApp::StartPrivateTransport() { return StartPrivateTransport("Arpc"); }

bool ANetApp::StartPrivateTransport(const std::string &name) {
    if (!_ownTransport) {
        return false;
    }

    _transport->start();
    _transport->setName(name);
    return true;
}

bool ANetApp::StopPrivateTransport() {
    if (!_ownTransport) {
        return false;
    }

    _transport->stop();
    _transport->wait();

    return true;
}

void ANetApp::dump(std::ostringstream &out) {
    if (_transport) {
        _transport->dump(out);
    }
}

ARPC_END_NAMESPACE(arpc);
