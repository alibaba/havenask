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
#include <memory>
#include <sstream>
#include <string>
#include <unordered_set>

#include "autil/ThreadPool.h"
#include "autil/NoCopyable.h"
#include "autil/WorkItem.h"
#include "autil/WorkItemQueue.h"
#include "tensorflow/core/lib/core/threadpool.h"
#include "tensorflow/core/platform/env.h"

namespace autil {

class TfThreadPool : public ThreadPoolBase {
private:
    using ThreadPoolImpl = tensorflow::thread::ThreadPool;

public:
    TfThreadPool(const size_t threadNum = DEFAULT_THREADNUM,
                 const size_t queueSize = DEFAULT_QUEUESIZE,
                 bool stopIfHasException = false,
                 const std::string &name = ""); // will make an owned TF ThreadPool
    virtual ~TfThreadPool();

    static TfThreadPool from(ThreadPoolImpl *impl, size_t queueSize);

    virtual bool isFull() const override;
    virtual void dump(std::string &leadingSpace, std::ostringstream &os) const override;
    virtual ERROR_TYPE pushWorkItem(WorkItem *item, bool isBlocked = true) override;
    virtual size_t getItemCount() const override;
    virtual bool start(const std::string &name = "") override;
    virtual void stop(STOP_TYPE stopType = STOP_AFTER_QUEUE_EMPTY) override;
    ThreadPoolImpl* getThreadPoolImpl() const { return _impl; }
    
protected:
    virtual bool createThreads(const std::string &name) override;
    virtual void asyncImpl(const std::function<void()> &) override;
    virtual void clearQueue() override;

private:
    TfThreadPool(ThreadPoolImpl *impl, size_t queueSize); // act as an adapter, pool observed
    void scheduleFunc(WorkItem *item);
    
private:
    ThreadPoolImpl *_impl{nullptr};
    bool owned{false};

private:
    AUTIL_LOG_DECLARE();
};

} // namespace autil
