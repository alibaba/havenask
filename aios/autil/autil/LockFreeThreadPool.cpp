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
#include "autil/LockFreeThreadPool.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <errno.h>
#include <exception>
#include <functional>
#include <memory>
#include <new>
#include <stack>
#include <string>
#include <typeinfo>
#include <unistd.h>

#include "autil/Lock.h"
#include "autil/LockFree.h"
#include "autil/Log.h"
#include "autil/Thread.h"
#include "autil/ThreadAutoScaler.h"
#include "autil/TimeUtility.h"
#include "autil/WorkItem.h"

namespace autil {

AUTIL_LOG_SETUP(autil, LockFreeThreadPool);

LockFreeThreadPool::LockFreeThreadPool(const size_t threadNum,
                                       const size_t queueSize,
                                       autil::WorkItemQueueFactoryPtr factory,
                                       const std::string &name,
                                       bool stopIfHasException)
    : autil::ThreadPool(threadNum, queueSize, stopIfHasException, name, factory)
    , _curQueueSize(0)
    , _notifier(nullptr)
    , _notifyIndex(0)
    , _waitIndex(0)
    , _notifySlots(threadNum)
    , _autoScaler(nullptr) {
    if (factory == NULL) {
        _queue.reset(new (std::nothrow) LockFreeThreadPoolQueue());
    } else {
        _queue.reset(factory->create(threadNum, queueSize));
    }
}

LockFreeThreadPool::~LockFreeThreadPool() {
    stop();
    autil::WorkItem *item = NULL;
    if (_queue.get()) {
        while ((item = _queue->pop())) {
            item->destroy();
        }
    }
    delete[] _notifier;
    delete _autoScaler;
    _notifier = nullptr;
    _autoScaler = nullptr;
}

bool LockFreeThreadPool::initAutoScaler() {
    if (AUTO_SCALE_THREAD_NUM == _threadNum) {
        std::unique_ptr<ThreadAutoScaler> scaler(new ThreadAutoScaler());
        if (scaler->init(_threadName)) {
            _autoScaler = scaler.release();
            return true;
        } else {
            AUTIL_LOG(ERROR, "thread num auto scale init failed");
            return false;
        }
    }
    return true;
}

bool LockFreeThreadPool::doInit() {
    if (!initAutoScaler()) {
        return false;
    }
    if (_autoScaler) {
        _threadNum = _autoScaler->getMaxThreadNum();
        AUTIL_LOG(
            INFO, "auto scaler enabled, thread num min[%lu], max[%lu]", _autoScaler->getMinThreadNum(), _threadNum);
    }
    _notifySlots = _threadNum;
    _notifier = new (std::nothrow) autil::ThreadCountedCond[_notifySlots];
    if (_notifier == NULL)
        return false;
    return true;
}

bool LockFreeThreadPool::start(const std::string &name) {
    if (!doInit()) {
        return false;
    }
    autil::ScopedLock lock(_cond);
    return ThreadPool::start(name);
}

void LockFreeThreadPool::stop(STOP_TYPE stopType) {
    {
        autil::ScopedLock guard(_cond);
        if (!_run) {
            return;
        }
        _run = false;
        _push = false;
    }
    if (_autoScaler) {
        _autoScaler->stop();
    }
    join();
}

LockFreeThreadPool::ERROR_TYPE LockFreeThreadPool::pushWorkItem(autil::WorkItem *item, bool isBlocked) {
    if (!IsRunning()) {
        return ERROR_POOL_HAS_STOP;
    }

    if (__builtin_expect(_curQueueSize > _queueSize, 0)) {
        if (isBlocked) {
            while (_curQueueSize > _queueSize)
                usleep(100);
        } else {
            return ERROR_POOL_QUEUE_FULL;
        }
    }

    if (item) {
        AtomicIncrement(&_curQueueSize);
        do {
            if (_queue->push(item)) {
                item = NULL;
                break;
            }
            if (isBlocked == false) {
                break;
            }
            usleep(100);
        } while (1);

        if (item) { // push failure
            AtomicDecrement(&_curQueueSize);
            return ERROR_POOL_QUEUE_FULL;
        }
        size_t index = AtomicIncrement(&_notifyIndex) % _notifySlots;
        _notifier[index].signalCounted();
    }

    return ERROR_NONE;
}

size_t LockFreeThreadPool::getItemCount() const { return _curQueueSize; }

bool LockFreeThreadPool::IsRunning() const { return _run; }

int LockFreeThreadPool::WorkerRoutine(size_t id) {
    while (true) {
        if (_autoScaler) {
            _autoScaler->wait(id);
        }
        size_t index = AtomicIncrement(&_waitIndex) % _notifySlots;
        _notifier[index].waitCounted();

        if (!_run) {
            break;
        }

        autil::WorkItem *item;
        while (!(item = _queue->pop())) {
#if __x86_64__
            asm("pause");
#elif __aarch64__
#define __nops(n) ".rept " #n "\nnop\n.endr\n"
            asm volatile(__nops(1));
#endif
        }
        ++_activeThreadCount;
        consumeItem(item);
        --_activeThreadCount;
    }

    return 0;
}

void LockFreeThreadPool::consumeItem(autil::WorkItem *item) {
    int64_t end, start;
    AtomicDecrement(&_curQueueSize);
    start = autil::TimeUtility::monotonicTimeUs();
    ThreadPoolBase::consumeItem(item);
    end = autil::TimeUtility::monotonicTimeUs();
    if (end - start >= 2000000) {
        AUTIL_LOG(WARN, "process cost: %ld, _curQueueSize: %ld", end - start, _curQueueSize);
    }
}

bool LockFreeThreadPool::createThreads(const std::string &name) {
    size_t num = _threadNum;
    _threads.resize(num, NULL);
    while (num--) {
        _threads[num] =
            autil::Thread::createThread(std::bind(&LockFreeThreadPool::WorkerRoutine, this, num), _threadName);
        if (!_threads[num]) {
            AUTIL_LOG(ERROR, "Create WorkerLoop thread fail!");
            return false;
        }
    }
    return true;
}

void LockFreeThreadPool::join() {
    for (size_t i = 0; i < _notifySlots; ++i) {
        _notifier[i].signalCounted();
    }

    for (size_t i = 0; i < _threadNum; i++) {
        if (_threads[i]) {
            _threads[i]->join();
            _threads[i].reset();
        }
    }
}

void LockFreeThreadPool::clearQueue() {}

} // namespace autil
