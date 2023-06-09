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
#include "autil/ThreadPool.h"

#include <unistd.h>
#include <ostream>
#include <utility>

#include "autil/LambdaWorkItem.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/WorkItem.h"
#include "autil/WorkItemQueue.h"
#include "autil/legacy/exception.h"

using namespace std;

namespace autil {

AUTIL_LOG_SETUP(autil, ThreadPoolBase);
AUTIL_LOG_SETUP(autil, ThreadPool);

ThreadPoolBase::ThreadPoolBase(const size_t threadNum,
                               const size_t queueSize,
                               bool stopIfHasException,
                               const string &name)
    : _stopIfHasException(stopIfHasException)
    , _activeThreadCount(0)
    , _hasException(false)
    , _threadNum(threadNum)
    , _queueSize(queueSize)
    , _push(true)
    , _run(false)
    , _threadName(name)
{
    if (_threadNum == 0) {
        AUTIL_LOG(WARN, "Thread number is zero, use default value 4");
        _threadNum = DEFAULT_THREADNUM;
    }
    if (_queueSize == 0) {
        AUTIL_LOG(WARN, "Queue size is zero, use default value 32");
        _queueSize = DEFAULT_QUEUESIZE;
    }
}

ThreadPoolBase::~ThreadPoolBase() {}

ThreadPoolBase::ERROR_TYPE ThreadPoolBase::pushTask(Task task, bool isBlocked, bool executeWhenFail) {
    WorkItem *workItem = new autil::LambdaWorkItem(std::move(task));
    auto ret = pushWorkItem(workItem, isBlocked);
    if (ret != ERROR_NONE) {
        if (executeWhenFail) {
            workItem->process();
        }
        workItem->destroy();
    }
    return ret;
}

bool ThreadPoolBase::start(const std::string &name) {
    if (_run) {
        AUTIL_LOG(ERROR, "Thread Pool [%s] already started!", name.c_str());
        return false;
    }
    _push = true;
    _run = true;
    if (_threadName.empty() && !name.empty()) {
        _threadName = name;
    }
    if (createThreads(_threadName)) {
        return true;
    } else {
        AUTIL_LOG(ERROR, "Thread Pool [%s] failed!", _threadName.c_str());
        stop(STOP_THREAD_ONLY);
        return false;
    }
}

void ThreadPoolBase::consumeItem(WorkItem *item) {
    try {
        item->process();
        item->destroy();
    } catch (const std::exception &e) {
        autil::legacy::ExceptionBase wrapper(e.what());
        wrapper.Init(__FILE__, __FUNCTION__, __LINE__);
        AUTIL_LOG(ERROR, "thread [%s] exception: [%s], stack: [%s]",
                  _threadName.c_str(), e.what(), wrapper.GetStackTrace().c_str());
        destroyItemIgnoreException(item);
        {
            ScopedLock lock(_cond);
            if (!_hasException) {
                _hasException = true;
                _exception = e;
            }
        }
    }
}

void ThreadPoolBase::checkException() {
    ScopedLock lock(_cond);
    if (stopIfHasException()) {
        throw _exception;
    }
}

void ThreadPoolBase::destroyItemIgnoreException(WorkItem *item) {
    try {
        if (item) {
            item->destroy();
        }
    } catch (...) { AUTIL_LOG(ERROR, "Work item destroy exception ignore"); }
}

void ThreadPoolBase::dropItemIgnoreException(WorkItem *item) {
    try {
        if (item) {
            item->drop();
        }
    } catch (...) { AUTIL_LOG(ERROR, "Work item drop exception igonre"); }
}

void ThreadPoolBase::waitQueueEmpty() const {
    while (!stopIfHasException()) {
        if ((size_t)0 == getItemCount()) {
            break;
        }
        usleep(10000);
    }
}

const size_t ThreadPool::MAX_THREAD_NAME_LEN = 15;

ThreadPool::ThreadPool(const size_t threadNum, const size_t queueSize, bool stopIfHasException,
                       const std::string &name, WorkItemQueueFactoryPtr factory)
    : ThreadPoolBase(threadNum, queueSize, stopIfHasException, name)
    , _lastPopTime(0)
    {
        if (factory == nullptr) {
            _queue.reset(new CircularPoolQueue(_queueSize));
        } else {
            _queue.reset(factory->create(threadNum, queueSize));
        }
    }

ThreadPool::ThreadPool(const size_t threadNum, const size_t queueSize, WorkItemQueueFactoryPtr factory,
                       const std::string &name, bool stopIfHasException):
    ThreadPool(threadNum, queueSize, stopIfHasException, name, factory) {}

ThreadPool::~ThreadPool() { stop(STOP_AND_CLEAR_QUEUE_IGNORE_EXCEPTION); }

void ThreadPool::dump(std::string &leadingSpace, ostringstream &os) const {
    os << leadingSpace << "ThreadPool addr: 0x" << this << "\t"
       << "Max thread num: " << _threadNum << "\t"
       << "Current thread num: " << _threads.size() << endl;
    os << leadingSpace << "Max queue size: " << _queueSize << "\t"
       << "Current queue size: " << getItemCount() << endl;
}

bool ThreadPool::isFull() const {
    ScopedLock guard(_cond);
    return _queue->size() >= _queueSize;
}

ThreadPool::ERROR_TYPE ThreadPool::pushWorkItem(WorkItem *item, bool isBlocked) {
    if (!_push) {
        AUTIL_LOG(INFO, "thread pool [%s] has stopped", _threadName.c_str());
        return ERROR_POOL_HAS_STOP;
    }
    if (!item) {
        AUTIL_LOG(INFO, "thread pool [%s] WorkItem is NULL", _threadName.c_str());
        return ERROR_POOL_ITEM_IS_NULL;
    }
    if (stopIfHasException()) {
        stop(STOP_AND_CLEAR_QUEUE);
        AUTIL_LOG(INFO, "thread pool [%s] has stopped", _threadName.c_str());
        return ERROR_POOL_HAS_STOP;
    }

    ScopedLock lock(_cond);
    if (!isBlocked && _queue->size() >= _queueSize) {
        AUTIL_LOG(INFO,
                  "thread pool [%s] is full, queueSize [%lu], active thread [%d]",
                  _threadName.c_str(),
                  _queueSize,
                  _activeThreadCount.load());
        return ERROR_POOL_QUEUE_FULL;
    }

    while (_push && _queue->size() >= _queueSize) {
        _cond.producerWait();
        if (stopIfHasException()) {
            AUTIL_LOG(INFO, "thread pool [%s] has exception, now has stopped", _threadName.c_str());
            return ERROR_POOL_HAS_STOP;
        }
    }

    if (!_push) {
        AUTIL_LOG(INFO, "thread pool [%s] has stopped", _threadName.c_str());
        return ERROR_POOL_HAS_STOP;
    }

    do {
        if (_queue->push(item)) {
            break;
        } else {
            return ERROR_POOL_QUEUE_FULL;
        }
        if (isBlocked == false) {
            break;
        }
        usleep(100);
    } while (true);

    _cond.signalConsumer();
    return ERROR_NONE;
}

size_t ThreadPool::getItemCount() const {
    ScopedLock guard(_cond);
    return _queue->size();
}

void ThreadPool::stop(STOP_TYPE stopType) {
    if (stopType != STOP_THREAD_ONLY) {
        ScopedLock lock(_cond);
        _push = false;
        _cond.broadcastProducer();
    }

    if (stopType == STOP_AFTER_QUEUE_EMPTY) {
        waitQueueEmpty();
    }

    stopThread();

    if (stopType != STOP_THREAD_ONLY) {
        clearQueue();
    }

    if (stopType != STOP_AND_CLEAR_QUEUE_IGNORE_EXCEPTION) {
        checkException();
    }
}

void ThreadPool::clearQueue() {
    ScopedLock lock(_cond);
    while (_queue && _queue->size()) {
        auto item = _queue->pop();
        dropItemIgnoreException(item);
    }
    _cond.broadcastProducer();
}

void ThreadPool::waitFinish() {
    while (!stopIfHasException()) {
        {
            ScopedLock guard(_cond);
            if (_queue->size() == 0 && _activeThreadCount == 0) {
                break;
            }
        }
        usleep(10000);
    }
    if (stopIfHasException()) {
        stop(STOP_AND_CLEAR_QUEUE);
    }
}

void ThreadPool::workerLoop() {
    if (_threadStartHook) {
        _threadStartHook();
    }
    while (_run && !stopIfHasException()) {
        WorkItem *item = NULL;
        {
            ScopedLock lock(_cond);
            while (_run && !_queue->size()) {
                _cond.consumerWait();
            }
            if (!_run) {
                break;
            }
            item = _queue->pop();
            _activeThreadCount++;
            _cond.signalProducer();
        }
        consumeItem(item);
        _lastPopTime = TimeUtility::monotonicTimeUs();
        _activeThreadCount--;
    }
    if (stopIfHasException()) {
        ScopedLock lock(_cond);
        _cond.broadcastProducer();
    }
    if (_threadStopHook) {
        _threadStopHook();
    }
}

void ThreadPool::stopThread() {
    if (!_run) {
        return;
    }
    {
        ScopedLock lock(_cond);
        _run = false;
        _cond.broadcastConsumer();
    }
    _threads.clear();
}

bool ThreadPool::createThreads(const std::string &name) {
    std::string threadName = name.substr(0, std::min(name.size(), MAX_THREAD_NAME_LEN));
    size_t num = _threadNum;
    while (num--) {
        ThreadPtr tp = Thread::createThread(std::bind(&ThreadPool::workerLoop, this), threadName);
        _threads.push_back(tp);
        if (!tp) {
            AUTIL_LOG(ERROR, "Create WorkerLoop thread [%s] fail!", name.c_str());
            return false;
        }
    }
    return true;
}

void ThreadPool::asyncImpl(const std::function<void()> &fn) {
    // TODO(zhenyun.yzy): unify pushTask and async
    auto ec = pushTask(fn);
    if (ec != autil::ThreadPool::ERROR_NONE) {
        fn();
    }
}

void ThreadPool::stopTrace() {
    autil::legacy::ExceptionBase e;
    e.Init("", "", 0);
    _stopBackTrace = e.ToString();
    AUTIL_LOG(INFO, "StopThreadPool: %s", _stopBackTrace.c_str());
}

} // namespace autil
