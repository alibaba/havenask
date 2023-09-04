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
#include "navi/engine/EvTimer.h"
#include "autil/Diagnostic.h"
#include <ev.h>

namespace navi {

#define CANCEL_MASK (1lu << 63)
#define UPDATE_MASK (1lu << 62)
#define UN_MASK (~(CANCEL_MASK | UPDATE_MASK))

class Timer {
public:
    Timer(double timeoutSecond, TimerCallback callback, void *data)
        : _callback(callback)
        , _data(data)
        , _timeoutSecond(timeoutSecond)
    {
        atomic_set(&_refCount, 2);
        _watcher.data = this;
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wstrict-aliasing")
        ev_timer_init(&_watcher, ev_callback, _timeoutSecond, 0.0);
DIAGNOSTIC_POP
    }
    ~Timer() {
    }
public:
    void ref() {
        atomic_inc(&_refCount);
    }
    void unRef() {
        if (unlikely(atomic_dec_return(&_refCount) == 0)) {
            delete this;
        }
    }
    void process(bool isTimeout) {
        auto callback = __atomic_exchange_n(&_callback, 0, __ATOMIC_ACQUIRE);
        if (callback) {
            callback(_data, isTimeout);
        }
    }
    void start(struct ev_loop *loop) {
        auto timer = &_watcher;
        ev_timer_start(loop, timer);
    }
    void update(struct ev_loop *loop) {
        if (_finish) {
            return;
        }
        auto timer = &_watcher;
        ev_timer_stop (loop, timer);
        ev_timer_set (timer, _timeoutSecond, 0.0);
        ev_timer_start (loop, timer);
    }
    void stop(struct ev_loop *loop) {
        ev_timer_stop(loop, &_watcher);
        if (_callback) {
            process(false);
        }
        setFinish();
    }
    TimerCallback getCallback() const {
        return _callback;
    }
    void setFinish() {
        if (!_finish) {
            _finish = true;
            unRef();
        }
    }
    void updateTimeout(double timeoutSecond) {
        _timeoutSecond = timeoutSecond;
    }
private:
    static void ev_callback(struct ev_loop *loop, ev_timer *w, int revents) {
        auto timer = (Timer *)w->data;
        assert(!timer->_finish);
        timer->process(true);
        timer->setFinish();
    }
private:
    TimerCallback _callback = nullptr;
    void *_data = nullptr;
    atomic64_t _refCount;
    ev_timer _watcher;
    bool _finish = false;
    double _timeoutSecond;
};

EvTimer::EvTimer(const std::string &threadName)
    : _threadName(threadName)
{
}

EvTimer::~EvTimer() {
    stop();
    DELETE_AND_SET_NULL(_watcher);
}

bool EvTimer::start() {
    _evLoop = ev_loop_new(EVFLAG_AUTO);
    if (!_evLoop) {
        return false;
    }
    _watcher = new ev_async();
    ev_set_userdata(_evLoop, this);
    ev_set_invoke_pending_cb(_evLoop, EvTimer::callbackInvoke);
    ev_async_init(_watcher, EvTimer::callback);
    _watcher->data = this;
    ev_async_start(_evLoop, _watcher);
    _thread = autil::Thread::createThread(std::bind(&EvTimer::loop, this), _threadName);
    if (!_thread) {
        return false;
    }
    return true;
}

void EvTimer::stop() {
    if (!_evLoop) {
        return;
    }
    setStopFlag();
    signal();
    _thread.reset();
    ev_loop_destroy(_evLoop);
    _evLoop = nullptr;
}

void EvTimer::setStopFlag() {
    autil::ScopedLock lock(_mutex);
    _stopped = true;
}

bool EvTimer::getStopFlag() {
    autil::ScopedLock lock(_mutex);
    return _stopped;
}

void EvTimer::signal() {
    ev_async_send(_evLoop, _watcher);
}

void EvTimer::callbackInvoke(struct ev_loop *loop) {
    auto evTimer = (EvTimer *)ev_userdata(loop);
    ev_invoke_pending(loop);
    if (evTimer->getStopFlag()) {
        ev_break(EV_A_ EVBREAK_ALL);
    }
}

void EvTimer::callback(struct ev_loop *loop, ev_async *w, int revents) {
    auto evTimer = (EvTimer *)w->data;
    uint64_t ptr = 0;
    while (evTimer->_timerQueue.Pop(&ptr)) {
        auto timer = (Timer *)(ptr & UN_MASK);
        auto cancel = ptr & CANCEL_MASK;
        auto update = ptr & UPDATE_MASK;
        if (cancel) {
            timer->stop(loop);
            timer->unRef();
        } else if (update) {
            timer->update(loop);
            timer->unRef();
        } else {
            timer->start(loop);
        }
    }
}

void EvTimer::loop() {
    ev_run(_evLoop, 0);
}

Timer *EvTimer::addTimer(double timeoutSecond, TimerCallback callback, void *data) {
    if (!callback) {
        return nullptr;
    }
    auto timer = new Timer(timeoutSecond, callback, data);
    _timerQueue.Push((uint64_t)timer);
    signal();
    return timer;
}

void EvTimer::stopTimer(Timer *timer) {
    if (timer->getCallback()) {
        timer->ref();
        uint64_t ptr = (uint64_t)timer | CANCEL_MASK;
        _timerQueue.Push(ptr);
        signal();
    }
    timer->unRef();
}

void EvTimer::updateTimer(Timer *timer, double timeoutSecond) {
    if (timer->getCallback()) {
        timer->ref();
        timer->updateTimeout(timeoutSecond);
        uint64_t ptr = (uint64_t)timer | UPDATE_MASK;
        _timerQueue.Push(ptr);
        signal();
    }
}

}

