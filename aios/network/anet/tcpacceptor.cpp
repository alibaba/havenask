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
#include "aios/network/anet/tcpacceptor.h"

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include "aios/network/anet/connection.h"
#include "aios/network/anet/controlpacket.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/iocomponent.h"
#include "aios/network/anet/ioworker.h"
#include "aios/network/anet/iserveradapter.h"
#include "aios/network/anet/log.h"
#include "aios/network/anet/socket.h"
#include "aios/network/anet/socketevent.h"
#include "aios/network/anet/tcpcomponent.h"
#include "aios/network/anet/threadmutex.h"
#include "aios/network/anet/transport.h"

namespace anet {
class IPacketStreamer;

TCPAcceptor::TCPAcceptor(Transport *owner,
                         Socket *socket,
                         IPacketStreamer *streamer,
                         IServerAdapter *serverAdapter,
                         int timeout,
                         int maxIdleTimeInMillseconds,
                         int backlog)
    : IOComponent(owner, socket) {
    _streamer = streamer;
    _serverAdapter = serverAdapter;
    _timeout = timeout;
    _maxIdleTimeInMillseconds = maxIdleTimeInMillseconds;
    _backlog = backlog;
    _lastAcceptedComponent = NULL;
    _type = IOC_TCPACCEPTOR;
}

bool TCPAcceptor::init(bool isServer) {
    _socket->setSoBlocking(false);
    bool rc = ((Socket *)_socket)->listen(_backlog);
    if (rc) {
        initialize();
        setState(ANET_CONNECTED); // may be need new state
        {
            MutexGuard lock(&_socketMutex);
            _isSocketInEpoll = true;
            rc = _socketEvent->addEvent(_socket, true, false);
            if (!rc)
                return rc;
        }
        _belongedWorker->postCommand(Transport::TC_ADD_IOC, this);
    }
    return rc;
}

bool TCPAcceptor::handleReadEvent() {
    lock();
    Socket *socket;
    while ((socket = ((Socket *)_socket)->accept()) != NULL) {
        ANET_LOG(DEBUG, "New connection coming. fd=%d", socket->getSocketHandle());
        TCPComponent *component = new TCPComponent(_owner, socket);
        assert(component);
        component->setMaxIdleTime(_maxIdleTimeInMillseconds);
        if (!component->init(true)) {
            delete component; /**@TODO: may coredump?*/
            unlock();
            return true;
        }
        Connection *conn = component->createConnection(_streamer, _serverAdapter);
        conn->setQueueTimeout(_timeout);
        _serverAdapter->handlePacket(conn, &ControlPacket::ReceiveNewConnectionPacket);

        _lastAcceptedComponent = component;
        _owner->addToCheckingList(component);

        // transport's Read Write Thread and Timeout thread will have their
        // own reference of component, so we need to subRef() the initial one
        component->subRef();
    }
    unlock();
    return true;
}

bool TCPAcceptor::handleErrorEvent() {
    close();
    return false;
}

void TCPAcceptor::close() {
    lock();
    if (getState() != ANET_CLOSED) {
        closeSocketNoLock();
        setState(ANET_CLOSED);
    }
    _belongedWorker->postCommand(Transport::TC_REMOVE_IOC, this);
    unlock();
}

/*
 * 超时检查
 *
 * @param    now 当前时间(单位us)
 */
bool TCPAcceptor::checkTimeout(int64_t now) { return true; }
} // namespace anet
