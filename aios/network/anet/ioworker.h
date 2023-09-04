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
#ifndef ANET_IO_WORKER_H
#define ANET_IO_WORKER_H

#include <iosfwd>
#include <stdint.h>
#include <vector>

#include "aios/network/anet/epollsocketevent.h"
#include "aios/network/anet/iocomponent.h"
#include "aios/network/anet/runnable.h"
#include "aios/network/anet/socket.h"
#include "aios/network/anet/thread.h"
#include "aios/network/anet/threadmutex.h"
#include "aios/network/anet/transport.h"

namespace anet {

/**
 * Class IoWorker is response for watch on a set of file descriptor and
 * dispatch the events on them.
 * There are two mode to run an IoWorker instance. a) Run it with a standalone
 * thread. In this case, call IoWorker::start(), then a new thread will be
 * created to do the io staff work. b) Call IoWorker::step() directly. Then
 * the calling thread is response for calling the epoll functions and handle
 * the read/write events.
 */
class IoWorker : public Runnable {
    friend class TcpComponentIdleTimeTest_testClientIdleTime_Test;
    friend class TcpComponentIdleTimeTest_testDefaultIdleTime_Test;
    friend class TcpComponentIdleTimeTest_testIdleTimeSetViaListen_Test;
    friend class TcpComponentIdleTimeTest_testIdleTimeZero_Test;
    friend class TcpComponentIdleTimeTest_testIdleTimeNegative_Test;
    friend class TCPConnectionTest_testMemLeak_Test;
    friend class ConnectionTest_testHTTPHandler_Test;
    friend class ConnectionTest_testHTTPTimeoutBeforeSend_Test;
    friend class ConnectionTest_testHTTPTimeoutAfterSend_Test;
    friend class TransportTest;
    friend class TransportTest_testConstructor_Test;
    friend class IoWorkerTest_testAddComponent_Test;
    friend class IoWorkerTest_testInitEpollWaitTimeoutFromEnv_Test;

public:
    IoWorker();

    ~IoWorker();

    /**
     * @param promotePriority promote the proproty of ANET thread
     * @return Return true if start success, else return false.
     */
    bool start(bool promotePriority = false);

    /**
     * Stop ANET. It will stop all threads of ANET.
     * In this function, _stopMutex will be locked.
     *
     * @return Return true if stop success, else return false.
     */
    bool stop();

    /**
     * Waiting for read/write thread exit.
     *
     * @return Return true if wait success, else return false.
     */
    bool wait();

    /**
     * Start thread.
     *
     * @param arg: args for thread
     */
    void run(Thread *thread, void *arg);

    void eventLoop();

    /**
     * Run iteration in single thread. In this function,
     * eventIteration and timeoutIteration will be called.
     *
     * @param timeStamp: execute time
     */
    void step(int64_t timeStamp);

    void postCommand(const Transport::CommandType type, IOComponent *ioc);

    SocketEvent *getSocketEvent();
    void closeComponents();
    int dump(std::ostringstream &buf);
    void getTcpConnStats(std::vector<ConnStat> &connStats);

    void setName(const char *name);

private:
    /**
     * process Command in queue _commands;
     */
    void processCommands();
    void addComponent(IOComponent *ioc);
    void removeComponent(IOComponent *ioc);
    void setPriorityByEnv();

private:
    // mutex for _iocList and _commands
    ThreadMutex _mutex;
    ThreadMutex _stopMutex;
    EPollSocketEvent _socketEvent;
    Thread _ioThread;
    bool _stop; // stopping flag
    bool _started;
    bool _promotePriority;
    IOComponent *_iocListHead, *_iocListTail; // IOComponent list
    std::vector<Transport::TransportCommand> _commands;
    int _epollWaitTimeoutMs{100};

public:
    static __thread int64_t _loopTime;
};

} // namespace anet

#endif // ANET_IO_WORKER_H
