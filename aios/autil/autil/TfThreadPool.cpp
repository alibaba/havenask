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
#include "autil/TfThreadPool.h"

#include <cassert>
#include <exception>
#include <functional>
#include <string>
#include <unistd.h>
#include <climits>
#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/WorkItemQueue.h"
namespace autil {

AUTIL_LOG_SETUP(autil, TfThreadPool);

TfThreadPool::TfThreadPool(const size_t threadNum,
                           const size_t queueSize,
                           bool stopIfHasException,
                           const std::string &name)
    : ThreadPoolBase(threadNum, queueSize, stopIfHasException, name),
      owned(true)
{
    if (!start()) {
        AUTIL_LOG(ERROR, "TfThreadPool %s failed to start!", _threadName.c_str());
    }
    if (_queueSize == std::numeric_limits<size_t>::max()) {
        AUTIL_LOG(WARN, "TfThreadPool %s start without queue size limitation", _threadName.c_str());
    }
}

TfThreadPool::~TfThreadPool() { stop(STOP_AND_CLEAR_QUEUE_IGNORE_EXCEPTION); }

TfThreadPool::TfThreadPool(ThreadPoolImpl *impl, size_t queueSize)
    : ThreadPoolBase(0, queueSize, false, ""), owned(false) {
    _impl = impl;
    _push = true;
    _run = true;
}
    
TfThreadPool TfThreadPool::from(ThreadPoolImpl *impl, size_t queueSize) {
    return TfThreadPool{impl, queueSize};
}
    
bool TfThreadPool::isFull() const { return getItemCount() >= _queueSize; }

void TfThreadPool::dump(std::string &leadingSpace, std::ostringstream &os) const {
    os << leadingSpace << "ThreadPool addr: 0x" << this << "\t"
       << "thread num: " << _threadNum << "\t"
       << "current workitem count: " << getItemCount() << std::endl;
}

TfThreadPool::ERROR_TYPE TfThreadPool::pushWorkItem(WorkItem *item, bool isBlocked) {
    if (!_push) {
        AUTIL_LOG(INFO, "ThreadPool [%s] doesn't accpet WorkItem anymore!", _threadName.c_str());
        return ERROR_POOL_HAS_STOP;
    }
    if (!item) {
        AUTIL_LOG(INFO, "Try to push a NULL WorkItem to ThreadPool [%s]", _threadName.c_str());
        return ERROR_POOL_ITEM_IS_NULL;
    }
    if (stopIfHasException()) {
        stop(STOP_AND_CLEAR_QUEUE);
        AUTIL_LOG(INFO, "ThreadPool [%s] has stopped", _threadName.c_str());
        return ERROR_POOL_HAS_STOP;
    }
    if (!isBlocked && isFull()) {
        AUTIL_LOG(INFO,
                  "ThreadPool [%s] is full, queueSize [%lu], active thread [%d]",
                  _threadName.c_str(),
                  _queueSize,
                  _activeThreadCount.load());
        return ERROR_POOL_QUEUE_FULL;
    }
    while (_push && isFull()) {
        ScopedLock guard(_cond);
        _cond.producerWait();
        if (stopIfHasException()) {
            AUTIL_LOG(INFO, "ThreadPool [%s] has exception, now has stopped", _threadName.c_str());
            return ERROR_POOL_HAS_STOP;
        }
    }
    if (!_push) {
        AUTIL_LOG(INFO, "ThreadPool [%s] has stopped", _threadName.c_str());
        return ERROR_POOL_HAS_STOP;
    }
    if (!_impl) {
        AUTIL_LOG(INFO, "Implementation of ThreadPool [%s] is null", _threadName.c_str());
        return ERROR_POOL_HAS_STOP;
    }
    _impl->ScheduleWithTaskDropper(std::bind(&TfThreadPool::scheduleFunc, this, item),
                    std::bind(&TfThreadPool::dropItemIgnoreException, item));
    return ERROR_NONE;
}

size_t TfThreadPool::getItemCount() const {
    if (!_impl) {
        return 0;
    }
    return _impl->GetItemCount(); 
}

bool TfThreadPool::start(const std::string &name) {
    if (!owned) {
        AUTIL_LOG(ERROR, "TF ThreadPool not owned cannot be startted");
        return false;
    }
    _threadName = _threadName.empty() ? name : _threadName;
    if (_run) {
        AUTIL_LOG(INFO, "ThreadPool [%s] has started!", _threadName.c_str());
        return true;
    }
    _run = true;
    _push = true;
    if (createThreads(_threadName)) {
        return true;
    } else {
        AUTIL_LOG(ERROR, "ThreadPool [%s] fails to start!", _threadName.c_str());
        stop(STOP_THREAD_ONLY);
        return false;
    }
    return true;
}

void TfThreadPool::stop(STOP_TYPE stopType) {
    if (!owned) {
        AUTIL_LOG(ERROR, "TF ThreadPool not owned cannot be stopped");
        return;
    }
    {
        ScopedLock lock(_cond);
        _push = false;
        _cond.broadcastProducer();
    }

    if (stopType == STOP_AFTER_QUEUE_EMPTY) {
        waitQueueEmpty();
    }
    clearQueue();

    delete _impl;
    _impl = nullptr;

    _run = false;
    if (stopType != STOP_AND_CLEAR_QUEUE_IGNORE_EXCEPTION) {
        checkException();
    }
}

void TfThreadPool::clearQueue() {
    if (_impl) {
        _impl->clearQueue();
    }
}

bool TfThreadPool::createThreads(const std::string &name) {
    try {
        _impl = new ThreadPoolImpl(tensorflow::Env::Default(), _threadName, _threadNum);
    } catch (const std::exception &e) {
        AUTIL_LOG(ERROR, "ThreadPool [%s] fails to create threads, exception: %s", _threadName.c_str(), e.what())
        if (!_hasException) {
            _hasException = true;
            _exception = e;
        }
        return false;
    }
    return true;
}

void TfThreadPool::asyncImpl(const std::function<void()> &fn) {
    if (_impl) {
        _impl->Schedule(fn);
    }
}

void TfThreadPool::scheduleFunc(WorkItem *item) {
    ++_activeThreadCount;
    _cond.signalProducer();
    consumeItem(item);
    --_activeThreadCount;
}

} // namespace autil
