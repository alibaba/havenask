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
#include "aios/network/anet/transport.h"

#include <assert.h>
#include <list>
#include <ostream>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#include "aios/network/anet/connectionpriority.h"
#include "aios/network/anet/debug.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/iocomponent.h"
#include "aios/network/anet/ioworker.h"
#include "aios/network/anet/log.h"
#include "aios/network/anet/socket.h"
#include "aios/network/anet/tcpacceptor.h"
#include "aios/network/anet/tcpcomponent.h"
#include "aios/network/anet/thread.h"
#include "aios/network/anet/threadmutex.h"
#include "aios/network/anet/timeutil.h"
#include "aios/network/anet/transportlist.h"

namespace anet {
class Connection;
class IPacketStreamer;
class IServerAdapter;
struct ConnStat;
} // namespace anet

using namespace std;
namespace anet {

namespace {
const int MAX_IO_THREAD_NUM = 32;
} // anonymous namespace

static int SimpleHashStrategy(int fd, int ioThreadNum) { return (fd == -1) ? 0 : (fd % ioThreadNum); }

static int PrioritySeparateStrategy(int ioType, int priority, int fd, int ioThreadNum) {
    if (ioType == IOC_TCPACCEPTOR || priority == ANET_PRIORITY_HIGH) {
        return ioThreadNum;
    } else
        return SimpleHashStrategy(fd, ioThreadNum);
}

Transport::Transport() : _listenFdThreadMode(SHARE_THREAD), _listenThreadNum(0) {
    _ioThreadNum = 1;

    initialize();
}

Transport::Transport(int ioThreadNum, ListenFdThreadModeEnum mode) : _listenFdThreadMode(mode) {
    if (ioThreadNum < 1) {
        ioThreadNum = 1;
    } else if (ioThreadNum > MAX_IO_THREAD_NUM) {
        ioThreadNum = MAX_IO_THREAD_NUM;
    }
    _ioThreadNum = ioThreadNum;
    if (_listenFdThreadMode == EXCLUSIVE_LISTEN_THREAD) {
        _listenThreadNum = 1;
    } else {
        _listenThreadNum = 0;
    }

    initialize();
}

void Transport::initialize() {
    _ioWorkers = new IoWorker[_ioThreadNum + _listenThreadNum];
    _stop = false;
    _started = false;
    _promotePriority = false;
    _nextCheckTime = 0;
    _timeoutLoopInterval = 100000; // default 100ms
    /* Register the object into the global list. */
    addTransportToList(this);
}

Transport::~Transport() {
    closeComponents();
    delTransportFromList(this);
    delete[] _ioWorkers;
}

/**
 * start two threads, one for read/write, one for timeout checking
 *
 * @return true - OK, false - Fail
 */
bool Transport::start(bool promotePriority) {
    MutexGuard guard(&_stopMutex);
    if (_started) {
        return false;
    }
    _started = true;
    _promotePriority = promotePriority;
    signal(SIGPIPE, SIG_IGN);
    for (int k = 0; k < _ioThreadNum + _listenThreadNum; ++k) {
        _ioWorkers[k].start(_promotePriority);
    }
    _timeoutThread.start(this, NULL);
    return true;
}

/**
 * set stop flag for read/write thread and timeout checking thread.
 *
 * @return true - OK, false - Fail
 */
bool Transport::stop() {
    MutexGuard guard(&_stopMutex);
    _stop = true;
    for (int k = 0; k < _ioThreadNum + _listenThreadNum; ++k) {
        _ioWorkers[k].stop();
    }
    return true;
}

/**
 * waiting for all threads exit
 *
 * @return true - OK, false - Fail
 */
bool Transport::wait() {
    for (int k = 0; k < _ioThreadNum + _listenThreadNum; ++k) {
        _ioWorkers[k].wait();
    }

    _timeoutCond.signal();
    _timeoutThread.join();
    closeComponents();
    return true;
}

void Transport::eventIteration(int64_t &now) {
    now = TimeUtil::getTime();
    for (int k = 0; k < _ioThreadNum + _listenThreadNum; ++k) {
        _ioWorkers[k].step(now);
    }
}

/**
 * calling timeoutIteration() every 100ms
 */
void Transport::timeoutLoop() {
    while (!_stop) {
        auto waitTime = _timeoutLoopInterval / 1000;
        if (0 == waitTime) {
            waitTime = 100;
        }
        _timeoutCond.wait(waitTime);
        int64_t now = TimeUtil::getTime();
        timeoutIteration(now);
    }
}

void Transport::setTimeoutLoopInterval(int64_t ms) {
    // min 5ms, max 10000ms
    if (ms < 5) {
        ANET_LOG(WARN, "timeoutLoopInterval %ldms is too short", ms);
    } else if (ms > 10000) {
        ANET_LOG(WARN, "timeoutLoopInterval %ldms is too long", ms);
    }

    _timeoutLoopInterval = ms * 1000; // ms -> us
}

void Transport::closeComponents() {
    collectNewComponets();
    for (list<IOComponent *>::iterator it = _checkingList.begin(); it != _checkingList.end();) {
        IOComponent *ioc = *it;
        ANET_LOG(DEBUG, "IOC(%p)->subRef(), [%d]", ioc, ioc->getRef());
        ioc->close();
        ioc->subRef();
        it = _checkingList.erase(it);
    }
    _checkingList.clear();

    for (int k = 0; k < _ioThreadNum + _listenThreadNum; ++k) {
        _ioWorkers[k].closeComponents();
    }
}

void Transport::collectNewComponets() {
    MutexGuard guard(&_mutex);
    _checkingList.insert(_checkingList.end(), _newCheckingList.begin(), _newCheckingList.end());
    _newCheckingList.clear();
}

void Transport::timeoutIteration(int64_t now) {
    collectNewComponets();
    list<IOComponent *>::iterator it = _checkingList.begin();
    while (it != _checkingList.end()) {
        IOComponent *ioc = *it;
        if (ioc->checkTimeout(now)) {
            ++it;
        } else {
            ioc->subRef();
            it = _checkingList.erase(it);
            ANET_LOG(DEBUG, "checkTimeout remove ioc: %p from checkingList", ioc);
        }
    }
    _nextCheckTime = now + _timeoutLoopInterval;
}

void Transport::run(Thread *thread, void *arg) {
    assert(thread == &_timeoutThread);
    timeoutLoop();
}

void Transport::runIteration(int64_t &now) {
    eventIteration(now);
    if (now >= _nextCheckTime) {
        timeoutIteration(now);
    }
}

void Transport::run() {
    int64_t now;
    while (!_stop) {
        runIteration(now);
    }
}

void Transport::stopRun() { _stop = true; }

IOComponent *Transport::listen(const char *spec,
                               IPacketStreamer *streamer,
                               IServerAdapter *serverAdapter,
                               int postPacketTimeout,
                               int maxIdleTime,
                               int backlog) {
    MutexGuard guard(&_stopMutex);
    if (_stop) {
        ANET_LOG(SPAM, "Transport(%p) Stoped!", this);
        return NULL;
    }
    if (NULL == spec || NULL == streamer || NULL == serverAdapter) {
        ANET_LOG(WARN, "Invalid parameters for listen(%p,%p,%p)", spec, streamer, serverAdapter);
        return NULL;
    }

    // Server Socket
    Socket *socket = new Socket();
    DBGASSERT(socket);
    socket->setAddrSpec(spec);
    if (socket->getProtocolType() == (int)SOCK_STREAM) {
        // TCPAcceptor
        TCPAcceptor *acceptor =
            new TCPAcceptor(this, socket, streamer, serverAdapter, postPacketTimeout, maxIdleTime, backlog);
        DBGASSERT(acceptor);
        if (!acceptor->init()) {
            delete acceptor;
            return NULL;
        }
        return acceptor;
    } else {
        ANET_LOG(WARN, "SOCK_DGRAM server does not support yet, spec %s", spec);
        delete socket;
    }

    return NULL;
}

Connection *Transport::connect(const char *spec, IPacketStreamer *streamer, bool autoReconn, CONNPRIORITY prio) {
    return doConnect(NULL, spec, streamer, autoReconn, prio);
}

IoWorker *Transport::getBelongedWorker(const IOComponent *ioc) {
    int chunkId = getChunkId(ioc);
    return &_ioWorkers[chunkId];
}

void Transport::addToCheckingList(IOComponent *ioc) {
    ioc->addRef();

    MutexGuard guard(&_mutex);
    _newCheckingList.push_back(ioc);
}

void Transport::lock() { _mutex.lock(); }

void Transport::unlock() { _mutex.unlock(); }

int Transport::getChunkId(const IOComponent *ioc) const {
    if (ioc == NULL) {
        return 0;
    }

    if (_listenFdThreadMode == EXCLUSIVE_LISTEN_THREAD) {
        assert(_listenThreadNum > 0);
        IOComponent *pioc = const_cast<IOComponent *>(ioc);
        return PrioritySeparateStrategy(
            ioc->getType(), pioc->getSocket()->getPriority(), pioc->getSocket()->getSocketHandle(), _ioThreadNum);
    } else {
        IOComponent *pioc = const_cast<IOComponent *>(ioc);
        return SimpleHashStrategy(pioc->getSocket()->getSocketHandle(), _ioThreadNum);
    }
}

void Transport::dump(ostringstream &buf) {
    char timestr[32];
    TimeUtil::getTimeStr(_nextCheckTime, timestr);

    int totalIOC = 0;
    buf << "------Transport State------\n";
    buf << "Address: " << this << "\tStarted: " << _started << "\tStopping: " << _stop << endl;
    buf << "\tTimeout interval: " << _timeoutLoopInterval << "us\t"
        << "Next check: " << _nextCheckTime << "-" << timestr << endl;

    for (int k = 0; k < _ioThreadNum + _listenThreadNum; ++k) {
        totalIOC += _ioWorkers[k].dump(buf);
    }
    buf << "\nTotally dumped IOC: " << totalIOC << endl;
}

void Transport::setName(const std::string &name) {
    _timeoutThread.setName((name + "ANetTo").c_str());
    for (int k = 0; k < _ioThreadNum + _listenThreadNum; ++k) {
        _ioWorkers[k].setName((name + "ANetIo").c_str());
    }
    _name = name;
}

Connection *Transport::connectWithAddr(
    const char *localAddr, const char *remoteSpec, IPacketStreamer *streamer, bool autoReconn, CONNPRIORITY prio) {
    if (localAddr == NULL) {
        ANET_LOG(WARN, "Invalid localspec for connect");
        return NULL;
    }
    return doConnect(localAddr, remoteSpec, streamer, autoReconn, prio);
}

Connection *Transport::doConnect(
    const char *localAddr, const char *remoteSpec, IPacketStreamer *streamer, bool autoReconn, CONNPRIORITY prio) {
    MutexGuard guard(&_stopMutex);
    if (_stop) {
        return NULL;
    }

    if (NULL == remoteSpec || NULL == streamer) {
        ANET_LOG(WARN, "Invalid parameters for connect(%s,%p,%d)", remoteSpec, streamer, autoReconn);
        return NULL;
    }

    Socket *socket = new Socket();
    assert(socket);

    if (socket->setAddrSpec(remoteSpec) == false) {
        delete socket;
        ANET_LOG(ERROR, "setAddress error: %s", remoteSpec);
        return NULL;
    }

    if (!socket->setReuseAddress(true)) {
        ANET_LOG(WARN, "set reuse address [%s] failed with localAddr [%s]", remoteSpec, localAddr);
    }
    if (socket->setPriority(prio)) {
        ANET_LOG(WARN, "set priority [%d] to addr [%s] failed", prio, remoteSpec);
    }
    if (localAddr && socket->getProtocolFamily() != AF_UNIX) {
        if (!bindLocalAddress(socket, localAddr)) {
            delete socket;
            ANET_LOG(WARN, "Bind localip [%s] to socket failed", localAddr);
            return NULL;
        }
    }

    /* Now we only support steam, instead of dgram type. */
    if (socket->getProtocolType() == (int)SOCK_STREAM) {
        TCPComponent *component = new TCPComponent(this, socket);
        assert(component);
        if (!component->init()) {
            delete component;
            ANET_LOG(ERROR, "Failed to init TCPComponent(%s).", remoteSpec);
            return NULL;
        }
        component->setAutoReconn(autoReconn);
        component->createConnection(streamer, NULL);
        ANET_LOG(INFO,
                 "socket(fd:%d) connected, connection %p, ioc %p, remote %s, local %s",
                 component->getSocket()->getSocketHandle(),
                 component->getConnection(),
                 component,
                 remoteSpec,
                 localAddr);
        addToCheckingList(component);
        return component->getConnection();
    } else {
        ANET_LOG(WARN, "SOCK_DGRAM does not support yet, spec: %s", remoteSpec);
        delete socket;
    }
    return NULL;
}

void Transport::getTcpConnStats(vector<ConnStat> &connStats) {
    for (int k = 0; k < _ioThreadNum + _listenThreadNum; ++k) {
        _ioWorkers[k].getTcpConnStats(connStats);
    }
}

bool Transport::bindLocalAddress(Socket *socket, const char *localAddr) {
    char ipStr[512];
    ipStr[0] = '\0';
    uint16_t port;
    parseLocalAddr(localAddr, ipStr, 512, port);
    return socket->bindLocalAddress(ipStr, port);
}

void Transport::parseLocalAddr(const char *localAddr, char *ipStr, int ipStrBufLen, uint16_t &port) {
    const char *sep = strchr(localAddr, ':');
    int ipLen = 0;
    if (sep != NULL) {
        ipLen = sep - localAddr;
    } else {
        ipLen = strlen(localAddr);
    }

    if (ipLen >= ipStrBufLen) {
        if (ipStrBufLen >= 1) {
            ipStr[0] = '\0';
        }
        return;
    }
    strncpy(ipStr, localAddr, ipLen);
    ipStr[ipLen] = '\0';

    port = 0;
    if (sep != NULL && ipLen < (int)strlen(localAddr)) {
        port = (uint16_t)atoi(sep + 1);
    }
}

} // namespace anet
