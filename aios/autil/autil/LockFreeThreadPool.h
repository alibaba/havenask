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

#include <iosfwd>
#include <memory>
#include <semaphore.h>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "autil/LockFreeQueue.h"
#include "autil/Thread.h"
#include "autil/ThreadPool.h"
#include "autil/WorkItem.h"
#include "autil/WorkItemQueue.h"

namespace autil {

class ThreadAutoScaler;

class LockFreeThreadPoolQueue : public autil::WorkItemQueue {
public:
    LockFreeThreadPoolQueue() : WorkItemQueue(){};

    virtual bool push(autil::WorkItem *item) override {
        _queue.Push(item);
        return true;
    }
    virtual autil::WorkItem *pop() override {
        autil::WorkItem *item;
        if (_queue.Pop(&item))
            return item;
        return NULL;
    }
    virtual size_t size() const override { return _queue.Size(); }

private:
    LockFreeQueue<autil::WorkItem *> _queue;
};

// comment out the code to avoid -faligned-new warning
// class LockFreeThreadPoolQueueFactory : public WorkItemQueueFactory {
// public:
//     WorkItemQueue* Create(const size_t threadNum, const size_t queueSize) {
//         return new LockFreeThreadPoolQueue();
//     }
// };

class LockFreeThreadPool : public autil::ThreadPool {
public:
    LockFreeThreadPool(const size_t threadNum,
                       const size_t queueSize,
                       autil::WorkItemQueueFactoryPtr factory,
                       const std::string &name,
                       bool stopIfHasException = false);
    virtual ~LockFreeThreadPool();

private:
    LockFreeThreadPool(const LockFreeThreadPool &);
    LockFreeThreadPool &operator=(const LockFreeThreadPool &);

public:
    virtual ERROR_TYPE pushWorkItem(autil::WorkItem *item, bool isBlocked = true) override;
    virtual size_t getItemCount() const override;
    virtual bool start(const std::string &name = "") override;
    virtual void stop(STOP_TYPE stopType = STOP_AFTER_QUEUE_EMPTY) override;
    void join();

protected:
    virtual void clearQueue() override;
    void SetIdleThresholdInMs(int ms);
    virtual bool createThreads(const std::string &name = "") override;

private:
    bool doInit();
    bool initAutoScaler();
    bool IsRunning() const;
    int WorkerRoutine(size_t id);
    virtual void consumeItem(autil::WorkItem *item) override;

private:
    volatile uint64_t _curQueueSize;
    autil::ThreadCountedCond *_notifier;
    int64_t _notifyIndex;
    int64_t _waitIndex;
    uint64_t _notifySlots;
    ThreadAutoScaler *_autoScaler;

public:
    static constexpr size_t AUTO_SCALE_THREAD_NUM = (size_t)-1;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace autil
