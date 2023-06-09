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
#include <assert.h>
#include <google/protobuf/message.h>
#include <stdint.h>
#include <cstddef>
#include <memory>
#include <new>
#include <string>
#include <unordered_map>
#include <utility>

#include "aios/network/anet/delaydecodepacket.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/packet.h"
#include "aios/network/anet/runnable.h"
#include "aios/network/anet/globalflags.h"
#include "aios/network/anet/iocomponent.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/common/Exception.h"
#include "aios/autil/autil/Lock.h"
#include "aios/network/arpc/arpc/util/Log.h"
#include "aios/network/arpc/arpc/MessageCodec.h"
#include "aios/network/arpc/arpc/RPCServer.h"
#include "aios/network/arpc/arpc/RPCServerClosure.h"
#include "aios/network/arpc/arpc/Tracer.h"
#include "aios/network/arpc/arpc/ANetRPCServer.h"
#include "aios/network/arpc/arpc/MessageSerializable.h"
#include "aios/network/arpc/arpc/RPCServerAdapter.h"
#include "aios/network/arpc/arpc/PacketArg.h"
#include "aios/network/arpc/arpc/anet/ANetApp.h"
#include "aios/network/arpc/arpc/anet/ANetRPCMessageCodec.h"
#include "aios/network/arpc/arpc/anet/ANetRPCServerAdapter.h"

using namespace std;
using namespace anet;

ARPC_BEGIN_NAMESPACE(arpc);
ARPC_DECLARE_AND_SETUP_LOGGER(ANetRPCServer);

using namespace common;

ANetRPCServer::ANetRPCServer(Transport *transport,
                             size_t threadNum,
                             size_t queueSize)
    : RPCServer(threadNum, queueSize)
    , _anetApp(transport)
{
    ANetRPCMessageCodec *codec = new ANetRPCMessageCodec(_anetApp.GetPacketFactory());
    _messageCodec = codec;
    _serverAdapter = new ANetRPCServerAdapter(this, codec);
}

ANetRPCServer::~ANetRPCServer()
{
}

bool ANetRPCServer::Listen(const std::string &address,
                           int timeout, int maxIdleTime,
                           int backlog)
{
    IOComponent *ioComponent = _anetApp.Listen(address, (ANetRPCServerAdapter *)_serverAdapter,
                          timeout, maxIdleTime, backlog);

    if (NULL == ioComponent) {
        ARPC_LOG(ERROR, "listen on %s failed", address.c_str());
        return false;
    }

    autil::ScopedLock lock(_ioComponentMutex);
    _ioComponentList.push_back(ioComponent);
    return true;
}

bool ANetRPCServer::Close() {
    Exception e;
    e.Init("", "", 0);
    ARPC_LOG(DEBUG, "[%p] RPCServer::Close: %s", this, e.what());

    {
        autil::ScopedLock lock(_ioComponentMutex);
        list<IOComponent *>::iterator it = _ioComponentList.begin();

        for(; it != _ioComponentList.end(); ++it) {
            (*it)->close();
            (*it)->subRef();
        }

        _ioComponentList.clear();
    }

    return true;
}

void ANetRPCServer::dump(std::ostringstream &out) {
    RPCServer::dump(out);
    // dump iocomponent
    {
        autil::ScopedLock lock(_ioComponentMutex);
        out << "IOComponent Count: " << _ioComponentList.size() << endl;

        for (list<IOComponent *>::iterator it = _ioComponentList.begin();
                it != _ioComponentList.end(); ++it) {
            (*it)->dump(out);
        }
    }
}

int ANetRPCServer::GetListenPort() {
    if (_ioComponentList.empty()) {
        ARPC_LOG(ERROR, "io component list is empty");
        return -1;
    }
    anet::IOComponent *ioComponent = _ioComponentList.front();
    Socket *socket = ioComponent->getSocket();
    if (!socket) {
        ARPC_LOG(ERROR, "get socket failed");
        return -1;
    }
    return socket->getPort();
}


ARPC_END_NAMESPACE(arpc)
