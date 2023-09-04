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
#include "aios/network/anet/ioworker.h"

#include <assert.h>
#include <cstddef>
#include <iosfwd>
#include <sched.h>
#include <signal.h>
#include <stdint.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "aios/network/anet/connection.h"
#include "aios/network/anet/epollsocketevent.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/iocomponent.h"
#include "aios/network/anet/log.h"
#include "aios/network/anet/socketevent.h"
#include "aios/network/anet/tcpcomponent.h"
#include "aios/network/anet/thread.h"
#include "aios/network/anet/threadmutex.h"
#include "aios/network/anet/timeutil.h"
#include "aios/network/anet/transport.h"
#include "autil/EnvUtil.h"

using namespace std;
namespace anet {

IoWorker::IoWorker() {
    _stop = false;
    _started = false;
    _promotePriority = false;
    _iocListHead = _iocListTail = NULL;
    _epollWaitTimeoutMs = autil::EnvUtil::getEnv("ANET_EPOLL_WAIT_TIMEOUT", _epollWaitTimeoutMs);
}

IoWorker::~IoWorker() {}

bool IoWorker::start(bool promotePriority) {
    MutexGuard guard(&_stopMutex);
    if (_started) {
        return false;
    }
    _started = true;
    _promotePriority = promotePriority;
    signal(SIGPIPE, SIG_IGN);
    _ioThread.start(this, &_socketEvent);
    _ioThread.setName("ANetIOWorker");
    return true;
}

bool IoWorker::stop() {
    {
        MutexGuard guard(&_stopMutex);
        _stop = true;
    }
    _socketEvent.wakeUp();
    return true;
}

bool IoWorker::wait() {
    _ioThread.join();
    return true;
}

void IoWorker::run(Thread *thread, void *arg) {
    if (_promotePriority) {
        thread->setPriority(1, SCHED_RR);
    }
    setPriorityByEnv();
    eventLoop();
}

void IoWorker::setPriorityByEnv() {
    int niceValue = autil::EnvUtil::getEnv("ANET_IO_WORKER_NICE_INC", 0);
    if (niceValue != 0) {
        int ret = nice(niceValue);
        if (ret == -1) {
            ANET_LOG(ERROR, "inc anet io worker nice value %d falied.", niceValue);
        } else {
            ANET_LOG(INFO, "inc anet io worker nice value %d success, new nice %d.", niceValue, ret);
        }
    }
}

void IoWorker::eventLoop() {
    while (!_stop) {
        int64_t now = TimeUtil::getTime();
        step(now);
    }
}

__thread int64_t IoWorker::_loopTime = 0;
void IoWorker::step(int64_t timeStamp) {
    IOEvent events[MAX_SOCKET_EVENTS];
    int cnt = _socketEvent.getEvents(_epollWaitTimeoutMs, events, MAX_SOCKET_EVENTS);
    _loopTime = TimeUtil::getTime();
    for (int k = 0; k < cnt; ++k) {
        IOComponent *ioc = events[k]._ioc;
        assert(ioc);
        bool rc = true;
        if (events[k]._errorOccurred) {
            rc = ioc->handleErrorEvent();
        }

        if (rc && events[k]._readOccurred) {
            rc = ioc->handleReadEvent();
        }
        if (rc && events[k]._writeOccurred) {
            rc = ioc->handleWriteEvent();
        }
        ioc->updateUseTime(timeStamp);
    }
    processCommands();
}

void IoWorker::postCommand(const Transport::CommandType type, IOComponent *ioc) {
    assert(ioc);

    Transport::TransportCommand tc;
    tc.type = type;
    tc.ioc = ioc;
    ioc->addRef();
    {
        MutexGuard guard(&_mutex);
        _commands.push_back(tc);
    }
}

void IoWorker::processCommands() {
    /**
     * Check if the commands vector is empty before try to lock the transport.
     * Otherwise, it may get blocked by admin server, dump command and we
     * do not want to see the io threads get blocked.
     * We do this without lock because this is just a hint and would not
     * hurt if the value is not accurate. At least next loop will handle
     * the commands.
     */
    if (_commands.empty())
        return;

    MutexGuard guard(&_mutex);
    for (std::vector<Transport::TransportCommand>::iterator it = _commands.begin(); it != _commands.end(); ++it) {
        switch (it->type) {
        case Transport::TC_ADD_IOC:
            addComponent(it->ioc);
            break;
        case Transport::TC_REMOVE_IOC:
            removeComponent(it->ioc);
            break;
        default:
            break;
        }
        it->ioc->subRef();
    }
    _commands.clear();
}

void IoWorker::addComponent(IOComponent *ioc) {
    assert(ioc);
    if (ioc->isReferencedByReadWriteThread()) {
        ANET_LOG(DEBUG, "Already referenced: %p", ioc);
        return;
    }

    ioc->_prev = _iocListTail;
    ioc->_next = NULL;
    if (_iocListTail == NULL) {
        _iocListHead = ioc;
    } else {
        _iocListTail->_next = ioc;
    }
    _iocListTail = ioc;
    ioc->addRef();
    ioc->referencedByReadWriteThread(true);
}

void IoWorker::removeComponent(IOComponent *ioc) {
    assert(ioc);
    if (!ioc->isReferencedByReadWriteThread()) {
        ANET_LOG(DEBUG, "Not referenced: %p", ioc);
        return;
    }
    // remove from iocList
    if (ioc == _iocListHead) { // head
        _iocListHead = ioc->_next;
    }
    if (ioc == _iocListTail) { // tail
        _iocListTail = ioc->_prev;
    }
    if (ioc->_prev != NULL)
        ioc->_prev->_next = ioc->_next;
    if (ioc->_next != NULL)
        ioc->_next->_prev = ioc->_prev;
    ioc->referencedByReadWriteThread(false);
    ioc->subRef();
}

void IoWorker::closeComponents() {
    IOComponent *list, *ioc;
    list = _iocListHead;
    while (list) {
        ioc = list;
        ANET_LOG(DEBUG, "IOC(%p)->subRef(), [%d]", ioc, ioc->getRef());
        list = list->_next;
        ioc->close();
        ioc->subRef();
    }
    _iocListHead = _iocListTail = NULL;

    for (std::vector<Transport::TransportCommand>::iterator it = _commands.begin(); it != _commands.end(); ++it) {
        ANET_LOG(DEBUG, "IOC(%p)->subRef(), [%d]", it->ioc, it->ioc->getRef());
        it->ioc->subRef();
    }
    _commands.clear();
}

int IoWorker::dump(ostringstream &buf) {
    MutexGuard guard(&_mutex);

    int totalIOC = 0;
    IOComponent *ioc;
    ioc = _iocListHead;
    while (ioc) {
        ioc->dump(buf);
        ioc = ioc->_next;
        totalIOC++;
    }

    return totalIOC;
}

void IoWorker::getTcpConnStats(vector<ConnStat> &connStats) {
    MutexGuard guard(&_mutex);
    IOComponent *ioc;
    ioc = _iocListHead;
    while (ioc) {
        TCPComponent *tcpIoc = dynamic_cast<TCPComponent *>(ioc);
        if (tcpIoc == NULL) {
            ioc = ioc->_next;
            continue;
        }
        Connection *conn = tcpIoc->getConnection();
        if (conn) {
            ConnStat stat = conn->getConnStat();
            connStats.push_back(stat);
        }
        ioc = ioc->_next;
    }
}

SocketEvent *IoWorker::getSocketEvent() { return &_socketEvent; }

void IoWorker::setName(const char *name) { _ioThread.setName(name); }

} // namespace anet
