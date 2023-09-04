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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <iosfwd>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "autil/Thread.h"
#include "autil/WorkItem.h"
#include "autil/WorkItemQueue.h"

namespace autil {

class ThreadPoolBase : public NoCopyable {
public:
    using Task = std::function<void()>;

    template <typename T = void>
    using Future = std::future<T>;

    enum {
        DEFAULT_THREADNUM = 4,
        DEFAULT_QUEUESIZE = 32,
    };

    enum STOP_TYPE {
        STOP_THREAD_ONLY,
        STOP_AND_CLEAR_QUEUE,
        STOP_AFTER_QUEUE_EMPTY,
        STOP_AND_CLEAR_QUEUE_IGNORE_EXCEPTION,
    };

    enum ERROR_TYPE {
        ERROR_NONE = 0,
        ERROR_POOL_HAS_STOP,
        ERROR_POOL_ITEM_IS_NULL,
        ERROR_POOL_QUEUE_FULL,
    };

public:
    ThreadPoolBase(const size_t threadNum = DEFAULT_THREADNUM,
                   const size_t queueSize = DEFAULT_QUEUESIZE,
                   bool stopIfHasException = false,
                   const std::string &name = "");
    virtual ~ThreadPoolBase();

public:
    virtual void dump(std::string &leadingSpace, std::ostringstream &os) const = 0;
    virtual bool isFull() const = 0;
    virtual ERROR_TYPE pushWorkItem(WorkItem *item, bool isBlocked = true) = 0;
    virtual size_t getItemCount() const = 0;
    virtual bool start(const std::string &name = "");
    virtual void stop(STOP_TYPE stopType = STOP_AFTER_QUEUE_EMPTY) = 0;

public:
    ERROR_TYPE pushTask(Task task, bool isBlocked = true, bool executeWhenFail = false);

    template <typename Lambda, typename Ret = std::invoke_result_t<Lambda>>
    Future<Ret> async(Lambda &&lambda);

    template <typename Lambda,
              typename = std::enable_if_t<std::is_same_v<bool, std::invoke_result_t<Lambda, int>>, void>>
    bool blockingParallel(int parallel_count, Lambda &&lambda);

public:
    inline const std::string &getName() const { return _threadName; }
    inline size_t getActiveThreadNum() const { return _activeThreadCount; };
    inline size_t getQueueSize() const { return _queueSize; };
    inline size_t getThreadNum() const { return _threadNum; };
    virtual void consumeItem(WorkItem *);
    void checkException();
    static void destroyItemIgnoreException(WorkItem *item);
    static void dropItemIgnoreException(WorkItem *item);

protected:
    virtual void clearQueue() = 0;
    virtual void waitQueueEmpty() const;
    virtual bool createThreads(const std::string &name) = 0;
    virtual void asyncImpl(const std::function<void()> &) = 0;
    inline bool stopIfHasException() const { return _hasException && _stopIfHasException; }
    bool _stopIfHasException;
    std::atomic<std::uint32_t> _activeThreadCount;
    std::atomic_bool _hasException;
    std::exception _exception;
    size_t _threadNum;
    size_t _queueSize;
    mutable ProducerConsumerCond _cond;
    std::atomic_bool _push;
    std::atomic_bool _run;
    std::string _threadName;

private:
    friend class ThreadPoolTest;

    AUTIL_LOG_DECLARE();
};

template <typename Lambda, typename Ret>
ThreadPoolBase::Future<Ret> ThreadPoolBase::async(Lambda &&lambda) {
    auto task = std::make_shared<std::packaged_task<Ret()>>(std::forward<Lambda>(lambda));
    auto future = task->get_future();
    asyncImpl([task = std::move(task)] { (*task)(); });
    return future;
}

template <typename Lambda, typename U>
bool ThreadPoolBase::blockingParallel(int parallel_count, Lambda &&lambda) {
    std::vector<Future<bool>> futures;
    futures.reserve(parallel_count - 1);
    for (int i = 1; i < parallel_count; ++i) {
        futures.emplace_back(async([&, i]() -> bool { return lambda(i); }));
    }
    bool result = lambda(0);
    for (auto &future : futures) {
        future.wait();
        assert(future.valid());
        auto r = future.get();
        if (!r) {
            result = false;
        }
    }
    return result;
}

class ThreadPool : public ThreadPoolBase {
    static const size_t MAX_THREAD_NAME_LEN;

public:
    using ThreadHook = std::function<void()>;
    ThreadPool(const size_t threadNum = DEFAULT_THREADNUM,
               const size_t queueSize = DEFAULT_QUEUESIZE,
               bool stopIfHasException = false,
               const std::string &name = "",
               WorkItemQueueFactoryPtr factory = NULL);
    ThreadPool(const size_t threadNum,
               const size_t queueSize,
               WorkItemQueueFactoryPtr factory,
               const std::string &name,
               bool stopIfHasException = false);
    virtual ~ThreadPool();
    virtual void dump(std::string &leadingSpace, std::ostringstream &os) const override;
    virtual bool isFull() const override;
    virtual ERROR_TYPE pushWorkItem(WorkItem *item, bool isBlocked = true) override;
    virtual size_t getItemCount() const override;
    virtual void stop(STOP_TYPE stopType = STOP_AFTER_QUEUE_EMPTY) override;
    virtual void waitFinish();
    const std::vector<ThreadPtr> &getThreads() const { return _threads; }
    void setThreadStartHook(const ThreadHook &startHook) { _threadStartHook = startHook; }
    void setThreadStopHook(const ThreadHook &stopHook) { _threadStopHook = stopHook; }

protected:
    virtual void clearQueue() override;
    virtual void workerLoop();
    virtual void stopThread();
    virtual bool createThreads(const std::string &name) override;
    virtual void asyncImpl(const std::function<void()> &) override;
    void stopTrace();
    ThreadHook _threadStartHook;
    ThreadHook _threadStopHook;
    std::unique_ptr<WorkItemQueue> _queue;
    std::vector<ThreadPtr> _threads;
    int64_t _lastPopTime;
    std::string _stopBackTrace;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ThreadPool> ThreadPoolPtr;
typedef std::shared_ptr<ThreadPoolBase> ThreadPoolBasePtr;

} // namespace autil
