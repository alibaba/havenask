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
#include "aios/network/anet/tcpcomponent.h"

#include <assert.h>
#include <errno.h>
#include <ostream>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <sys/socket.h>

#include "aios/network/anet/common.h"
#include "aios/network/anet/connection.h"
#include "aios/network/anet/debug.h"
#include "aios/network/anet/directpacketstreamer.h"
#include "aios/network/anet/directtcpconnection.h"
#include "aios/network/anet/globalflags.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/iocomponent.h"
#include "aios/network/anet/ioworker.h"
#include "aios/network/anet/ipacketstreamer.h"
#include "aios/network/anet/log.h"
#include "aios/network/anet/socket.h"
#include "aios/network/anet/socketevent.h"
#include "aios/network/anet/tcpconnection.h"
#include "aios/network/anet/threadmutex.h"
#include "aios/network/anet/timeutil.h"
#include "aios/network/anet/transport.h"

namespace anet {
class IServerAdapter;

TCPComponent::TCPComponent(Transport *owner, Socket *socket) : IOComponent(owner, socket) {
    _startConnectTime = 0;
    _isServer = false;
    _connection = NULL;
    // useless, socketfd is -1 before init()
    // setSocketEvent(_owner->getSocketEvent(this));
    // assert(_socketEvent);
    _shrinkCount = SHRINK_COUNTDOWN;
    _type = IOC_TCPCOMPONENT;
}

TCPComponent::~TCPComponent() {
    if (_connection) {
        ANET_LOG(DEBUG, "delete connection(%p)", _connection);
        delete _connection;
        _connection = NULL;
    }
}

bool TCPComponent::init(bool isServer) {
    _socket->setSoLinger(false, 0);
    _socket->setIntOption(SO_KEEPALIVE, 1);
    _socket->setTcpNoDelay(true);
    if (!isServer) {
        if (!socketConnect()) {
            return false;
        }
        _enableWrite = true;
        setState(ANET_CONNECTING);
    } else {
        _socket->setSoBlocking(false);
        _enableWrite = false;
        setState(ANET_CONNECTED);
    }
    int64_t curTime = TimeUtil::getTime();
    updateUseTime(curTime);
    // add assignment statement for _startConnectTime when init TCPComponent for Ticket #23 by wanggf 200810162042
    _startConnectTime = curTime;
    _enableRead = true;
    _isServer = isServer;

    initialize();

    return true;
}

Connection *TCPComponent::createConnection(IPacketStreamer *streamer, IServerAdapter *adapter) {
    if (NULL == streamer) {
        return NULL;
    }
    if (_isServer && NULL == adapter) {
        return NULL;
    }

    auto directSteamer = dynamic_cast<DirectPacketStreamer *>(streamer);
    if (nullptr == directSteamer) {
        _connection = new TCPConnection(_socket, streamer, adapter);
    } else {
        _connection = new DirectTCPConnection(_socket, streamer, adapter);
    }

    assert(_connection);
    _connection->setIOComponent(this);
    _connection->setServer(_isServer);

    lock();

    {
        MutexGuard lock(&_socketMutex);
        _isSocketInEpoll = true;
        bool rc = _socketEvent->addEvent(_socket, _enableRead, _enableWrite);
        if (!rc) {
            ANET_LOG(ERROR, "(IOC:%p) Add event fail", this);
        }
    }

    _belongedWorker->postCommand(Transport::TC_ADD_IOC, this);

    unlock();

    return _connection;
}

/* 0 means all ok, < 0 means not connect at all. */
RECONNErr TCPComponent::reconnect() {
    RECONNErr rc = ANET_RECONN_CLOSED;

    lock();

    /* we don't want to mix manual connect with auto reconnect. */
    if (_autoReconn) {
        rc = ANET_RECONN_CONFLICT;
    } else if (getState() <= ANET_CONNECTED) {
        char tmp[32];
        ANET_LOG(INFO,
                 "connection is being/already established while manual connect is invoked: "
                 "ioc state: %d, target: %s. ",
                 getState(),
                 _socket->getAddr(tmp, 32));
        rc = ANET_RECONN_CONNECTED;
    } else if (getState() == ANET_CLOSING) {
        int64_t now = TimeUtil::getTime();
        if (socketReconnect(now)) {
            setState(ANET_CONNECTING);
            /* reenable connection to send packet. */
            _connection->openHook();
            rc = ANET_RECONN_OK;
        } else {
            rc = ANET_RECONN_SOCKFAILED;
        }
    } else if (getState() == ANET_CLOSED) {
        /* for close connection, we have nothing to do. */
        rc = ANET_RECONN_CLOSED;
    } else {
        ANET_LOG(ERROR, "unknown connection state when reconnect, state %d", getState());
    }

    unlock();
    ANET_LOG(DEBUG, "reconnect returns rc: %d, current ioc state: %d", rc, getState());
    return rc;
}

bool TCPComponent::socketConnect() {
    if (!_socket->isAddressValid()) {
        return false;
    }
    _socket->setSoBlocking(false);
    if (!_socket->connect()) {
        char tmp[32];
        int error = Socket::getLastError();
        if (error != EINPROGRESS && error != EWOULDBLOCK) {
            _socket->close();
            ERRSTR(error);
            ANET_LOG(ERROR, "failed to connect to %s, %s(%d)", _socket->getAddr(tmp, 32), errStr, error);
            return false;
        }
    }

    return true;
}

/**
 * @todo: redefine close()
 */
void TCPComponent::close() { closeConnection(_connection); }

void TCPComponent::closeConnection(Connection *conn) {
    assert(_connection == conn);
    lock();
    closeSocketNoLock();
    if (setState(ANET_CLOSING)) {
        _belongedWorker->postCommand(Transport::TC_REMOVE_IOC, this);
    }
    unlock();
}

bool TCPComponent::handleWriteEvent() {
    lock();
    bool rc = true;
    ANET_LOG(DEBUG, "(IOC:%p), WriteEvent", this);

    if (getState() == ANET_CONNECTING) {
        ANET_LOG(SPAM, "Write ANET_CONNECTING(IOC:%p)", this);
        int64_t now = TimeUtil::getTime();
        // log warn for slow connect, cost time more than 1s.
        if (now - _startConnectTime > 1000000) {
            char spec[32];
            ANET_LOG(WARN,
                     "slow connect to %s, cost time:%d us",
                     _socket->getAddr(spec, 32),
                     (int)(now - _startConnectTime));
        }
        setState(ANET_CONNECTED);
        setAutoReconn(isAutoReconn());
        _connection->resetContext();
        _connection->setQosGroup();
    }

    if (getState() == ANET_CONNECTED) {
        ANET_LOG(SPAM, "Write ANET_CONNECTED (IOC:%p)", this);
        rc = _connection->writeData();
        if (!rc) {
            closeAndSetState();
        }
    } else {
        rc = false;
        ANET_LOG(WARN, "State(%d) changed by other thread!", getState());
    }
    unlock();
    return rc;
}

bool TCPComponent::handleErrorEvent() {
    lock();
    ANET_LOG(DEBUG, "(IOC:%p)", this);
    // add stat ANET_CONNECTING when handle error for Ticket #23 by wanggf 200810162037
    if ((getState() == ANET_CONNECTED) || (getState() == ANET_CONNECTING)) {
        ANET_LOG(WARN, "Detect error event from (IOC:%p), state:%d, error:%d", this, getState(), _socket->getSoError());
        closeAndSetState();
    }
    unlock();
    return false;
}

bool TCPComponent::handleReadEvent() {
    lock();
    ANET_LOG(DEBUG, "(IOC:%p) ReadEvent", this);
    bool rc = true;

    if (getState() == ANET_CONNECTED) {
        ANET_LOG(DEBUG, "Read ANET_CONNECTED (IOC:%p)", this);
        rc = _connection->readData();
        if (!rc) {
            closeAndSetState();
        }
    } else {
        rc = false;
        ANET_LOG(WARN, "State(%d) changed by other thread!", getState());
    }
    unlock();
    return rc;
}

/**
 * checking time out
 *
 * @param    now
 * @return if return false, we should remove this iocomponent
 * from timeout thread's checking list
 */
bool TCPComponent::checkTimeout(int64_t now) {
    lock();
    char tmp[32];
    bool rc = true;

    TCPConnection *tcpconn = static_cast<TCPConnection *>(_connection);
    assert(tcpconn);

    ANET_LOG(DEBUG, "ioc is checking timeout: %p, state: %d, ref: %d.", this, getState(), getRef());
    if (getState() < ANET_CLOSING) {
        _connection->checkTimeout(now);
    } else {
        tcpconn->clearOutputBuffer();
        _connection->closeHook();
    }

    if (--_shrinkCount <= 0) {
        /*
         * Shrink Condition:
         *  1) user's callback is not running (only applied to input buffer because R/W thread
         *     will release the lock when it invokes user's callback)
         *  2) in the recent shrink interval, max packet size less than a half of current buffer size
         */
        if (LIKELY(tcpconn->getRWCBState() != Connection::RW_CB_RUNNING)) {
            if (tcpconn->getMaxRecvPacketSize() < (tcpconn->getInputBufferSpaceAllocated() >> 1)) {
                tcpconn->shrinkInputBuffer();
            }
        }
        if (tcpconn->getMaxSendPacketSize() < (tcpconn->getOutputBufferSpaceAllocated() >> 1)) {
            tcpconn->shrinkOutputBuffer();
        }
        tcpconn->clearMaxRecvPacketSize();
        tcpconn->clearMaxSendPacketSize();
        _shrinkCount = SHRINK_COUNTDOWN;
    }

    ANET_LOG(DEBUG, "isServer: %d, idle: %ld", _isServer, now - _lastUseTime);
    if (getState() == ANET_CONNECTING) {
        if (_startConnectTime > 0 && _startConnectTime < (now - flags::getConnectTimeout())) {
            ANET_LOG(ERROR, "Connecting to %s timeout.", _socket->getAddr(tmp, 32));
            closeAndSetState();
            _startConnectTime = now;
        }
    } else if (getState() == ANET_CONNECTED && _isServer) {
        int64_t idle = now - _lastUseTime;
        if (idle > _maxIdleTimeInMicroseconds) {
            _autoReconn = 0;
            closeAndSetState();
            ANET_LOG(WARN,
                     "Closing idle connection. %s (idle time: %ld secs IOC:%p).",
                     _socket->getAddr(tmp, 32),
                     (idle / static_cast<int64_t>(1000000)),
                     this);
        }
    } else if (getState() == ANET_TO_BE_CONNECTING && _autoReconn && _startConnectTime < now - RECONNECTING_INTERVAL) {
        ANET_LOG(INFO, "Reconnecting: %s", _socket->getAddr(tmp, 32));
        _autoReconn--;

        if (socketReconnect(now)) {
            setState(ANET_CONNECTING);
        } else if (!_autoReconn) {
            ANET_LOG(ERROR, "exceeding MAX_RECONNECTING_TIMES!");
            setState(ANET_CLOSING);
        }
    } else if (getState() == ANET_CLOSING && getRef() == 1) {
        setState(ANET_CLOSED);
        rc = false;
    }

    unlock();
    return rc;
}

bool TCPComponent::setState(IOCState state) {
    ANET_LOG(SPAM, "start setting IOC(%p) state from: %d to %d", this, _state, state);
    if (_state < ANET_CLOSING) {
        _state = state;
        if (_connection && _state >= ANET_CLOSING) {
            _connection->closeHook();
        }
        ANET_LOG(SPAM, "set IOC(%p) state: %d", this, _state);
        return true;
    }
    /* For manual reconnecting */
    else if (_state == ANET_CLOSING) {
        if (state == ANET_CONNECTING) {
            _state = state;
            ANET_LOG(SPAM, "set IOC(%p) state: %d", this, _state);
            return true;
        }
    }
    if (state > _state) {
        _state = state;
        ANET_LOG(SPAM, "set IOC(%p) state: %d", this, _state);
        return true;
    }
    ANET_LOG(SPAM, "set IOC(%p) state: %d(NOT CHANGED), target: %d", this, _state, state);
    return false;
}

void TCPComponent::closeAndSetState() {
    closeSocketNoLock();
    setState(_autoReconn ? ANET_TO_BE_CONNECTING : ANET_CLOSING);
    _belongedWorker->postCommand(Transport::TC_REMOVE_IOC, this);
}

bool TCPComponent::socketReconnect(int64_t now) {
    bool rc = true;
    _startConnectTime = now;

    if (socketConnect()) {
        assert(_socketEvent);
        _enableRead = _enableWrite = true;

        {
            MutexGuard lock(&_socketMutex);
            _isSocketInEpoll = true;
            bool rc = _socketEvent->addEvent(_socket, _enableRead, _enableWrite);
            if (!rc)
                ANET_LOG(ERROR, "reconnectSocket IOC(%p) Add event fail", this);
        }

        _belongedWorker->postCommand(Transport::TC_ADD_IOC, this);
        rc = true;
    } else {
        ANET_LOG(ERROR, "can not reconnect to target, ioc %p, _autoReconn %d!", this, _autoReconn);
        rc = false;
    }

    return rc;
}

void TCPComponent::dump(std::ostringstream &buf) {
    char timebuf[32];
    TimeUtil::getTimeStr(_startConnectTime, timebuf);
    IOComponent::dump(buf);
    buf << "Type: TCPComponent"
        << "\t"
        << "Start Conn: " << _startConnectTime << "-" << std::string(timebuf);
    _connection->dump("   ", buf);
}

} // namespace anet
