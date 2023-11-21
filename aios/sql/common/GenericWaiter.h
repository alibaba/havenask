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

#include <assert.h>
#include <atomic>
#include <functional>
#include <list>
#include <memory>
#include <queue>
#include <stdint.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "autil/result/Errors.h"
#include "autil/result/Result.h"

namespace sql {

template <typename T>
class GenericWaiter {
public:
    using D = typename T::ClassT;
    using Status = typename T::StatusT;
    using CallbackParam = typename T::CallbackParamT;

private:
    struct CallbackItem {
    public:
        typedef std::function<void(autil::result::Result<CallbackParam>)> CallbackFunc;
        struct Payload {
            CallbackFunc callback;
            int64_t timeoutInUs = 0;
        };

    public:
        CallbackItem()
            : payload(std::make_shared<Payload>()) {}

    public:
        bool onDone(CallbackParam param);
        bool onTimeout(void *parent);

    public:
        int64_t targetTs = 0;
        int64_t expireTimeInUs = 0;
        std::shared_ptr<Payload> payload;
    };

    struct CallbackItemCmpTargetTs {
        bool operator()(const CallbackItem &lhs, const CallbackItem &rhs) {
            return lhs.targetTs > rhs.targetTs;
        }
    };

    struct CallbackItemCmpExpireTime {
        bool operator()(const CallbackItem &lhs, const CallbackItem &rhs) {
            return lhs.expireTimeInUs > rhs.expireTimeInUs;
        }
    };

public:
    GenericWaiter();
    virtual ~GenericWaiter();

private:
    GenericWaiter(const GenericWaiter &);
    GenericWaiter &operator=(const GenericWaiter &);

public:
    bool init();
    void wait(int64_t targetTs, typename CallbackItem::CallbackFunc callback, int64_t timeoutInUs);
    size_t getPendingItemCount() const;

protected:
    void notifyWorkerCheckLoop() {
        _notifier.notify();
    }

    void stop();

private:
    void workerCheckLoop();
    void checkCallback();
    virtual std::string getDesc() const = 0;
    virtual bool doInit() {
        return true;
    }
    virtual void doStop() {}
    virtual void onAwake() {}

private:
    Status getCurrentStatus() const {
        return static_cast<const D *>(this)->getCurrentStatusImpl();
    }
    int64_t transToKey(const Status &status) const {
        return static_cast<const D *>(this)->transToKeyImpl(status);
    }
    CallbackParam transToCallbackParam(const Status &status) const {
        return static_cast<const D *>(this)->transToCallbackParamImpl(status);
    }

public:
    static constexpr int64_t workerCheckLoopIntervalUs = 50 * 1000; // 50ms
    static constexpr int64_t timeoutValueLimit = 120 * 1000 * 1000; // 120s

private:
    bool _stopped = false;
    autil::Notifier _notifier;
    int64_t _watermark = 0;
    autil::ThreadPtr _workerThread;
    mutable autil::ThreadMutex _lock;
    std::atomic<size_t> _pendingItemCount = {0};
    std::priority_queue<CallbackItem, std::vector<CallbackItem>, CallbackItemCmpTargetTs>
        _targetTsQueue;
    std::priority_queue<CallbackItem, std::vector<CallbackItem>, CallbackItemCmpExpireTime>
        _timeoutQueue;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(sql, GenericWaiter, D);

template <typename T>
bool GenericWaiter<T>::CallbackItem::onDone(CallbackParam param) {
    auto callback_ = std::move(payload->callback);
    if (callback_) {
        callback_(std::move(param));
        return true;
    }
    return false;
}

template <typename T>
bool GenericWaiter<T>::CallbackItem::onTimeout(void *parent) {
    auto callback_ = std::move(payload->callback);
    if (callback_) {
        callback_(autil::result::RuntimeError::make(
            "[%p] GenericWaiter timeout, timeoutInUs %ld, targetTs %ld",
            parent,
            payload->timeoutInUs,
            targetTs));
        return true;
    }
    return false;
}

template <typename T>
GenericWaiter<T>::GenericWaiter()
    : _notifier(1) {}

template <typename T>
GenericWaiter<T>::~GenericWaiter() {
    assert(_stopped && "you must call stop while derived class destructed");
}

template <typename T>
void GenericWaiter<T>::stop() {
    _notifier.notifyExit();
    _workerThread.reset();
    _stopped = true;
    // no lock needed
    auto targetTsQueue = std::move(_targetTsQueue);
    auto timeoutQueue = std::move(_timeoutQueue);
    size_t timeoutCnt = 0;
    for (; !targetTsQueue.empty(); targetTsQueue.pop()) {
        if (auto top = targetTsQueue.top(); top.onTimeout(this)) {
            ++timeoutCnt;
        }
    }
    for (; !timeoutQueue.empty(); timeoutQueue.pop()) {
        if (auto top = timeoutQueue.top(); top.onTimeout(this)) {
            ++timeoutCnt;
        }
    }
    assert(_pendingItemCount.load() == timeoutCnt && "pending item count mismatch");
    _pendingItemCount.store(0, std::memory_order_release);
    doStop();
}

template <typename T>
bool GenericWaiter<T>::init() {
    if (!doInit()) {
        AUTIL_LOG(ERROR, "[%p] do init failed, desc[%s]", this, getDesc().c_str());
        return false;
    }
    _workerThread
        = autil::Thread::createThread(std::bind(&GenericWaiter::workerCheckLoop, this), "WmCb");
    if (!_workerThread) {
        AUTIL_LOG(
            ERROR, "[%p] init targetTs callback thread failed, desc[%s]", this, getDesc().c_str());
        return false;
    }
    return true;
}

template <typename T>
size_t GenericWaiter<T>::getPendingItemCount() const {
    return _pendingItemCount.load();
}

template <typename T>
void GenericWaiter<T>::wait(int64_t targetTs,
                            typename CallbackItem::CallbackFunc callback,
                            int64_t timeoutInUs) {
    assert(!_stopped);
    AUTIL_LOG(TRACE1, "[%p] start wait targetTs[%ld] timeout[%ld]", this, targetTs, timeoutInUs);
    timeoutInUs = std::min(timeoutInUs, D::timeoutValueLimit); // force truncate timeout value

    auto status = getCurrentStatus();
    if (targetTs < transToKey(status)) {
        callback(transToCallbackParam(status));
        return;
    }
    auto now = autil::TimeUtility::currentTime();
    CallbackItem item;
    item.payload->timeoutInUs = timeoutInUs;
    item.expireTimeInUs = autil::TIME_ADD(now, timeoutInUs);
    item.targetTs = targetTs;
    AUTIL_LOG(TRACE1, "[%p] start emplace targetTs[%ld]", this, targetTs);
    {
        autil::ScopedLock _(_lock);
        item.payload->callback = std::move(callback);
        _timeoutQueue.emplace(item); // DO NOT std::move
        _targetTsQueue.emplace(std::move(item));
    }
    auto pendingItemCount = _pendingItemCount.fetch_add(1, std::memory_order_release) + 1;
    if (pendingItemCount == 1) {
        onAwake();
    }
    AUTIL_LOG(
        TRACE1, "[%p] end emplace targetTs[%ld] pending[%lu]", this, targetTs, pendingItemCount);
}

template <typename T>
void GenericWaiter<T>::workerCheckLoop() {
    AUTIL_LOG(INFO, "[%p] worker loop started, desc[%s]", this, getDesc().c_str());
    while (true) {
        int status = _notifier.waitNotification(D::workerCheckLoopIntervalUs);
        if (status == autil::Notifier::EXITED) {
            break;
        }
        checkCallback();
    }
    AUTIL_LOG(INFO, "[%p] worker loop finished", this);
}

template <typename T>
void GenericWaiter<T>::checkCallback() {
    assert(!_stopped);
    if (getPendingItemCount() == 0) {
        return;
    }
    std::list<CallbackItem> timeoutList;
    std::list<CallbackItem> targetTsList;
    Status status = getCurrentStatus();
    int64_t checkpoint = transToKey(status);
    AUTIL_LOG(DEBUG, "[%p] start handle for checkpoint[%ld]", this, checkpoint);

    int64_t now = autil::TimeUtility::currentTime();
    {
        autil::ScopedLock _(_lock);
        for (; !_targetTsQueue.empty(); _targetTsQueue.pop()) {
            auto &top = _targetTsQueue.top();
            if (top.targetTs >= checkpoint) {
                break;
            }
            targetTsList.emplace_back(top);
        }
        for (; !_timeoutQueue.empty(); _timeoutQueue.pop()) {
            auto &top = _timeoutQueue.top();
            if (top.expireTimeInUs > now) {
                break;
            }
            timeoutList.emplace_back(top);
        }
    }
    // DO NOT callback in lock
    size_t doneCnt = 0, timeoutCnt = 0;
    auto param = transToCallbackParam(status);
    for (auto &item : targetTsList) {
        AUTIL_LOG(TRACE1,
                  "[%p] onDone for checkpoint[%ld] targetTs[%ld]",
                  this,
                  checkpoint,
                  item.targetTs);
        if (item.onDone(param)) {
            ++doneCnt;
        }
    }
    for (auto &item : timeoutList) {
        AUTIL_LOG(TRACE1,
                  "[%p] onTimeout for checkpoint[%ld] targetTs[%ld]",
                  this,
                  checkpoint,
                  item.targetTs);
        if (item.onTimeout(this)) {
            ++timeoutCnt;
        }
    }

    size_t finishedCount = doneCnt + timeoutCnt;
    size_t itemCount
        = _pendingItemCount.fetch_sub(finishedCount, std::memory_order_release) - finishedCount;
    AUTIL_LOG(DEBUG,
              "[%p] end handle for checkpoint[%ld] done[%lu] timeout[%lu] pending[%lu]",
              this,
              checkpoint,
              doneCnt,
              timeoutCnt,
              itemCount);
}

} // namespace sql
