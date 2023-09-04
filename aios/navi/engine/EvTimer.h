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
#pragma once

#include <autil/Thread.h>
#include <autil/Lock.h>
#include "aios/network/arpc/arpc/common/LockFreeQueue.h"
#include "aios/network/anet/atomic.h"
#include "navi/common.h"

struct ev_loop;
struct ev_async;
struct ev_timer;

namespace navi {

class Timer;
typedef void (*TimerCallback)(void *data, bool isTimeout);

class EvTimer
{
public:
    EvTimer(const std::string &threadName);
    ~EvTimer();
    EvTimer(const EvTimer &) = delete;
    EvTimer &operator=(const EvTimer &) = delete;
public:
    bool start();
    void stop();
    Timer *addTimer(double timeoutSecond, TimerCallback callback, void *data);
    void stopTimer(Timer *timer);
    void updateTimer(Timer *timer, double timeoutSecond);
private:
    void loop();
    void setStopFlag();
    bool getStopFlag();
    void signal();
private:
    static void callbackInvoke(struct ev_loop *loop);
    static void callback(struct ev_loop *loop, ev_async *w, int revents);
private:
    std::string _threadName;
    struct ev_loop *_evLoop = nullptr;
    ev_async *_watcher = nullptr;
    autil::ThreadMutex _mutex;
    bool _stopped = false;
    autil::ThreadPtr _thread;
    arpc::common::LockFreeQueue<uint64_t> _timerQueue;
};

NAVI_TYPEDEF_PTR(EvTimer);

}

