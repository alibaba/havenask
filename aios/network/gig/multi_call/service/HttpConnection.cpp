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
#include "aios/network/gig/multi_call/service/HttpConnection.h"

#include "aios/network/gig/multi_call/interface/HttpRequest.h"
#include "aios/network/gig/multi_call/service/ProtocolCallBack.h"
#include "aios/network/gig/multi_call/service/SearchServiceResource.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, HttpConnection);

HttpConnection::HttpConnection(anet::Transport *transport, const std::string &spec,
                               size_t queueSize)
    : Connection(spec, MC_PROTOCOL_HTTP, queueSize)
    , _transport(transport) {
}

HttpConnection::~HttpConnection() {
    ScopedReadWriteLock lock(_listLock, 'w');
    for (const auto con : _connectionList) {
        con->close();
        con->subRef();
    }
    _connectionList.clear();
}

void HttpConnection::recycleConnection(anet::Connection *con) {
    if (con->isClosed()) {
        con->subRef();
        AUTIL_LOG(INFO, "http anet connection [0x%p] to [%s] has closed", con, _spec.c_str());
        return;
    }
    ScopedReadWriteLock lock(_listLock, 'w');
    _connectionList.push_back(con);
}

void HttpConnection::closeConnection(anet::Connection *con) const {
    con->close();
    con->subRef();
}

anet::Connection *HttpConnection::getAnetConnection() {
    std::list<anet::Connection *> removeList;
    anet::Connection *retCon = NULL;
    {
        ScopedReadWriteLock lock(_listLock, 'w');
        while (!_connectionList.empty()) {
            auto tmp = _connectionList.back();
            _connectionList.pop_back();
            if (!tmp->isClosed()) {
                retCon = tmp;
                break;
            } else {
                removeList.push_back(tmp);
            }
        }
    }
    for (const auto con : removeList) {
        con->subRef();
        AUTIL_LOG(INFO, "http anet connection [0x%p] to [%s] has closed", con, _spec.c_str());
    }
    return retCon ? retCon : createConnection();
}

anet::Connection *HttpConnection::createConnection() {
    static anet::HTTPPacketFactory packetFactory;
    static anet::HTTPStreamer packetStreamer(&packetFactory);
    auto connection = _transport->connect(_spec.c_str(), &packetStreamer);
    if (!connection) {
        AUTIL_LOG(ERROR, "connect to %s failed", _spec.c_str());
        return NULL;
    }
    connection->setQueueLimit(1);
    connection->setQueueTimeout(RPC_CONNECTION_TIMEOUT * 4);
    AUTIL_LOG(INFO, "create http anet connection to [%s] success, con [0x%p]", _spec.c_str(),
              connection);
    return connection;
}

void HttpConnection::post(const RequestPtr &request, const CallBackPtr &callBack) {
    assert(callBack);
    const auto &resource = callBack->getResource();
    auto httpRequest = dynamic_cast<HttpRequest *>(request.get());
    if (!httpRequest) {
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_REQUEST, string(), string());
        return;
    }
    auto con = getAnetConnection();
    if (!con) {
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_CONNECTION, string(), string());
        return;
    }
    httpRequest->setHost(getHost());
    httpRequest->setRequestType(resource->getRequestType());
    auto packet = httpRequest->makeHttpPacket();
    if (!packet) {
        recycleConnection(con);
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_PROTOCOL_MESSAGE, string(), string());
        return;
    }

    if (resource->isNormalRequest()) {
        startChildRpc(request, callBack);
    }

    packet->setExpireTime(resource->getTimeout());
    auto httpCallBack = new HttpCallBack(callBack, shared_from_this(), con, _callBackThreadPool);
    callBack->rpcBegin();
    if (!con->postPacket(packet, httpCallBack)) {
        AUTIL_LOG(ERROR, "post packet failed, [%s]", _spec.c_str());
        packet->free();
        delete httpCallBack;
        recycleConnection(con);
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_POST, string(), string());
    }
}

} // namespace multi_call
