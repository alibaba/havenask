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
#include "master/HttpConnection.h"

using namespace std;
using namespace autil;

BEGIN_CARBON_NAMESPACE(master);
CARBON_LOG_SETUP(master, HttpConnection);
static const uint32_t RPC_CONNECTION_TIMEOUT = 5000;    //ms

HttpConnection::HttpConnection(anet::Transport *transport,
                               const std::string &spec)
{
    _transport = transport;
    _spec = spec;
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
        return;
    }
    ScopedReadWriteLock lock(_listLock, 'w');
    _connectionList.push_back(con);
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
        CARBON_LOG(DEBUG, "http anet connection [0x%p] to [%s] closed",
                  con, _spec.c_str());
    }
    return retCon ? retCon : createConnection();
}

anet::Connection *HttpConnection::createConnection() {
    static anet::HTTPPacketFactory packetFactory;
    static anet::HTTPStreamer packetStreamer(&packetFactory);
    auto connection = _transport->connect(_spec.c_str(), &packetStreamer, true);
    if (!connection) {
        CARBON_LOG(ERROR, "connect to %s failed", _spec.c_str());
        return NULL;
    }
    connection->setQueueLimit(1);
    connection->setQueueTimeout(RPC_CONNECTION_TIMEOUT * 4);
    CARBON_LOG(DEBUG, "create http anet connection to [%s] success, con [0x%p]",
              _spec.c_str(), connection);
    return connection;
}

anet::Packet *HttpConnection::sendPacket(Packet *packet, bool block, bool longConn){
    anet::Packet *ret_packet = NULL;
    anet::Connection * con = NULL;
    if (longConn) {
        con = getAnetConnection();
    } else {
        con = createConnection();
    }
    if (!con) {
        return ret_packet;
    }
    ret_packet = con->sendPacket(packet, block);
    if (longConn) {
        recycleConnection(con);
    } else {
        con->close();
        con->subRef();
    }
    return ret_packet;
}

END_CARBON_NAMESPACE(master);
