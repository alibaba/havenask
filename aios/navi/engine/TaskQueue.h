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

#include "arpc/common/LockFreeQueue.h"
#include "navi/common.h"
#include "navi/engine/NaviThreadPool.h"

namespace navi {

class NaviWorkerBase;
class NaviThreadPool;
class ConcurrencyConfig;
class NaviThreadPoolItemBase;

struct TaskQueueStat {
    size_t activeThreadCount = 0;
    size_t activeThreadQueueSize = 0;
    size_t idleThreadQueueSize = 0;
    size_t processingCount = 0;
    size_t queueCount = 0;
    size_t processingCountRatio = 0;
    size_t queueCountRatio = 0;
};

class TaskQueueScheduleItemBase : public NaviThreadPoolItemBase {
public:
    TaskQueueScheduleItemBase();
    virtual ~TaskQueueScheduleItemBase();
    TaskQueueScheduleItemBase(const TaskQueueScheduleItemBase &) = delete;
    const TaskQueueScheduleItemBase &operator=(const TaskQueueScheduleItemBase &) = delete;

public:
    bool syncMode() const override { return _sync; }
    void setSyncMode(bool sync) { _sync = sync; }

private:
    bool _sync = false;
};

class TaskQueue {
public:
    TaskQueue();
    virtual ~TaskQueue();
    TaskQueue(const TaskQueue &) = delete;
    TaskQueue &operator=(const TaskQueue &) = delete;

public:
    bool init(const std::string &name, const ConcurrencyConfig &config, TestMode testMode);
    void stop();
    bool push(TaskQueueScheduleItemBase *item);
    NaviThreadPool *getThreadPool() const { return _threadPool.get(); }
    void scheduleNext();
    TaskQueueStat getStat() const;
    size_t getProcessingCount() const { return atomic_read(&_processingCount); }

private:
    virtual void schedule(); // virtual for test
    void incProcessingCount();
    void decProcessingCount();

private:
    TestMode _testMode;
    std::string _desc;
    std::unique_ptr<NaviThreadPool> _threadPool;
    atomic64_t _processingCount;
    size_t _processingMax = 0;
    size_t _scheduleQueueMax = 0;
    arpc::common::LockFreeQueue<TaskQueueScheduleItemBase *> _scheduleQueue;
};

NAVI_TYPEDEF_PTR(TaskQueue);

} // end namespace navi
