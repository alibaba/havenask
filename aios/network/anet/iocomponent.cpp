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
#include "aios/network/anet/timeutil.h"
#include "aios/network/anet/socket.h"
#include "aios/network/anet/socketevent.h"
#include "aios/network/anet/log.h"
#include "aios/network/anet/iocomponent.h"
#include "aios/network/anet/connection.h"
#include "aios/network/anet/transport.h"
#include "aios/network/anet/ioworker.h"
#include <assert.h>
#include <stddef.h>

#include "aios/network/anet/atomic.h"
#include "aios/network/anet/debug.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/threadmutex.h"

namespace anet {

/*
 * ����芥�
 */
IOComponent::IOComponent(Transport *owner, Socket *socket) {
    assert(socket);
    assert(owner);
    _owner = owner;
    _socket = socket;
    _socket->setIOComponent(this);
    _socketEvent = NULL;
    atomic_set(&_refcount, 1);
    _state = ANET_TO_BE_CONNECTING; // 姝ｅ�杩��
    _autoReconn = 0; // 涓��������
    _prev = _next = NULL;
    _lastUseTime = TimeUtil::getTime();
    _enableRead = true;
    _enableWrite = false;
    _refByRreadWriteThread = false;
    _maxIdleTimeInMicroseconds = MAX_IDLE_TIME_IN_MICROSECONDS;
    _isSocketInEpoll = false;
    _type = IOC_BASE;
    _belongedWorker = NULL;
}

/*
 * ����芥�
 */
IOComponent::~IOComponent() {
    if (_socket) {
        _socket->close();
        delete _socket;
        _socket = NULL;
    }
}

void IOComponent::setSocketEvent(SocketEvent *socketEvent) {
    _socketEvent = socketEvent;
}

void IOComponent::enableRead(bool on) {
    _enableRead = on;
    bool read = _enableRead;
    bool write = _enableWrite;
    if (_socketEvent) {
        ANET_LOG(SPAM,"setEvent(R:%d,W:%d). IOC(%p)", read, write, this);
        MutexGuard lock(&_socketMutex);
        if (!_isSocketInEpoll) return;
        bool rc = _socketEvent->setEvent(_socket, read, write);
        if (!rc)
            ANET_LOG(ERROR,"setEvent(R:%d,W:%d). IOC(%p) fail", read, write, this);
    }
}

void IOComponent::enableWrite(bool on) {
    _enableWrite = on;
    bool read = _enableRead;
    bool write = _enableWrite;
    if (_socketEvent) {
        ANET_LOG(SPAM,"setEvent(R:%d,W:%d). IOC(%p)", read, write, this);
        MutexGuard lock(&_socketMutex);
        if (!_isSocketInEpoll) return;
        bool rc = _socketEvent->setEvent(_socket, read, write);
        if (!rc)
            ANET_LOG(ERROR,"setEvent(R:%d,W:%d). IOC(%p) fail", read, write, this);
    }
}

void IOComponent::addRef() {
    ANET_LOG(SPAM,"IOC(%p)->addRef(), [%d]", this, _refcount.counter);
    atomic_add(1, &_refcount);
}

void IOComponent::subRef() {
    ANET_LOG(SPAM,"IOC(%p)->subRef(), [%d]", this, _refcount.counter);
    int ref = atomic_dec_return(&_refcount);
    if (!ref) {
        ANET_LOG(SPAM,"Deleting this IOC(%p)", this);
        delete this;
    }
}

void IOComponent::closeConnection(Connection *conn) {
    conn->closeHook();
}

int IOComponent::getRef() {
    return atomic_read(&_refcount);
}

void IOComponent::lock() {
    _mutex.lock();
}

void IOComponent::unlock() {
    _mutex.unlock();
}

bool IOComponent::setState(IOCState state) {
    _state = state;
    return true;
}

void IOComponent::closeSocketNoLock() {
   if (_socket) {
       {
           MutexGuard lock(&_socketMutex);
           if (!_isSocketInEpoll) return;
           bool rc = _socketEvent->removeEvent(_socket);
           if (!rc)
           {
               ANET_LOG(ERROR,"IOC(%p)Remove event error", this);
           }
           _isSocketInEpoll = false;
       }
       _socket->close();
    }
}

void IOComponent::closeSocket() {
    lock();
    closeSocketNoLock();
    unlock();
}

RECONNErr IOComponent::reconnect() {
    /* we should never arrive here for now. */
    DBGASSERT(false);
    return ANET_RECONN_OK;
}

char* IOComponent::getLocalAddrStr(char *dest, int len) {
    return _socket->getAddr(dest, len, true);
}

void IOComponent::initialize() {
    _belongedWorker = _owner->getBelongedWorker(this);
    _socketEvent = _belongedWorker->getSocketEvent();
    assert(_socketEvent);
}

void IOComponent::shutdownSocket() {
    if (_socket) {
        _socket->shutdown();
    }
}

}
