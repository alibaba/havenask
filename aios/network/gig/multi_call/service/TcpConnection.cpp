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
#include "aios/network/gig/multi_call/service/TcpConnection.h"

#include "aios/network/anet/connection.h"
#include "aios/network/anet/defaultpacketfactory.h"
#include "aios/network/anet/defaultpacketstreamer.h"
#include "aios/network/anet/transport.h"
#include "aios/network/gig/multi_call/interface/TcpRequest.h"
#include "aios/network/gig/multi_call/service/ProtocolCallBack.h"
#include "aios/network/gig/multi_call/service/SearchServiceResource.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, TcpConnection);

TcpConnection::TcpConnection(anet::Transport *transport, const string &spec, size_t queueSize)
    : Connection(spec, MC_PROTOCOL_TCP, queueSize)
    , _transport(transport)
    , _connection(NULL) {
}

TcpConnection::~TcpConnection() {
    ScopedReadWriteLock lock(_connectLock, 'w');
    if (_connection) {
        _connection->close();
        _connection->subRef();
        _connection = NULL;
    }
}

anet::Connection *TcpConnection::createAnetConnection() {
    static anet::DefaultPacketFactory packetFactory;
    static anet::DefaultPacketStreamer packetStreamer(&packetFactory);
    ScopedReadWriteLock lock(_connectLock, 'w');
    if (_connection) {
        if (!_connection->isClosed()) {
            _connection->addRef();
            return _connection;
        } else {
            _connection->subRef();
            _connection = NULL;
        }
    }
    auto con = _transport->connect(_spec.c_str(), &packetStreamer);
    if (!con) {
        AUTIL_LOG(ERROR, "connection to %s failed", _spec.c_str());
        return NULL;
    }
    con->setQueueLimit(_queueSize);
    con->setQueueTimeout(RPC_CONNECTION_TIMEOUT);
    _connection = con;
    _connection->addRef();
    return _connection;
}

anet::Connection *TcpConnection::getAnetConnection() {
    {
        ScopedReadWriteLock lock(_connectLock, 'r');
        if (_connection && !_connection->isClosed()) {
            _connection->addRef();
            return _connection;
        }
    }
    return createAnetConnection();
}

void TcpConnection::post(const RequestPtr &request, const CallBackPtr &callBack) {
    assert(callBack);
    const auto &resource = callBack->getResource();
    auto tcpRequest = dynamic_cast<TcpRequest *>(request.get());
    if (!tcpRequest) {
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_REQUEST, string(), string());
        return;
    }
    auto con = getAnetConnection();
    if (!con) {
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_CONNECTION, string(), string());
        return;
    }

    tcpRequest->setRequestType(resource->getRequestType());
    anet::DefaultPacket *packet = tcpRequest->makeTcpPacket();
    if (!packet) {
        con->subRef();
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_PROTOCOL_MESSAGE, string(), string());
        return;
    }

    if (resource->isNormalRequest()) {
        startChildRpc(request, callBack);
    }

    packet->setExpireTime(resource->getTimeout());
    auto tcpCallBack = new TcpCallBack(callBack, _callBackThreadPool);
    callBack->rpcBegin();
    if (!con->postPacket(packet, tcpCallBack)) {
        AUTIL_LOG(ERROR, "post packet failed, [%s]", _spec.c_str());
        con->subRef();
        packet->free();
        delete tcpCallBack;
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_POST, string(), string());
        return;
    }
    con->subRef();
    return;
}

} // namespace multi_call
