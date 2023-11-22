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
#include "navi/engine/TaskQueue.h"

#include "navi/config/NaviConfig.h"
#include "navi/engine/NaviThreadPool.h"
#include "navi/engine/NaviWorkerBase.h"
#include "navi/log/NaviLogger.h"

namespace navi {

TaskQueueScheduleItemBase::TaskQueueScheduleItemBase() {}
TaskQueueScheduleItemBase::~TaskQueueScheduleItemBase() {}

TaskQueue::TaskQueue() {
    atomic_set(&_processingCount, 0);
}

TaskQueue::~TaskQueue() {
}

bool TaskQueue::init(const std::string &name, const ConcurrencyConfig &config, TestMode testMode) {
    {
        char buf[1024];
        snprintf(buf, 1024, "%p,%s", this, name.c_str());
        _desc = std::string(buf);
    }
    NAVI_KERNEL_LOG(
        INFO, "[%s] init with name[%s] config[%s]", _desc.c_str(), name.c_str(), ToJsonString(config).c_str());
    _testMode = testMode;
    auto threadPool = std::make_unique<NaviThreadPool>();
    auto logger = NAVI_TLS_LOGGER ? NAVI_TLS_LOGGER->logger : nullptr;
    if (!threadPool->start(config, logger,
                           name == "" ? "navi_worker" : "nvworker_" + name)) {
        NAVI_KERNEL_LOG(ERROR,
                        "[%s] thread pool [%s] start failed, config[%s]",
                        _desc.c_str(),
                        name.c_str(),
                        ToJsonString(config).c_str());
        return false;
    }
    _threadPool = std::move(threadPool);
    if (_testMode != TM_NONE) {
        _processingMax = std::numeric_limits<size_t>::max();
    } else {
        _processingMax = config.processingSize;
    }
    _scheduleQueueMax = config.queueSize;

    return true;
}

void TaskQueue::stop() {
    NAVI_KERNEL_LOG(INFO, "[%s] begin stop", _desc.c_str());
    size_t count = 0;
    while (!_scheduleQueue.Empty()) {
        if (count++ % 200 == 0) {
            NAVI_KERNEL_LOG(INFO,
                            "[%s] session schedule queue not empty, "
                            "size[%lu], loop[%lu], waiting...",
                            _desc.c_str(),
                            _scheduleQueue.Size(),
                            count);
        }
        usleep(1 * 1000);
    }
    if (_threadPool) {
        NAVI_KERNEL_LOG(INFO, "[%s] begin stop thread pool", _desc.c_str());
        _threadPool->stop();
        _threadPool.reset();
        NAVI_KERNEL_LOG(INFO, "[%s] end stop thread pool", _desc.c_str());
    }
    assert(atomic_read(&_processingCount) == 0 &&
           "processing count must be zero");
    NAVI_KERNEL_LOG(INFO, "[%s] end stop", _desc.c_str());
}

bool TaskQueue::push(TaskQueueScheduleItemBase *item) {
    NAVI_KERNEL_LOG(SCHEDULE1, "[%s] push item[%p] type[%s]", _desc.c_str(), item, typeid(*item).name());
    size_t queueSize = _scheduleQueue.Size();
    size_t processingCount = atomic_read(&_processingCount);
    if (processingCount + queueSize >= _processingMax + _scheduleQueueMax) {
        NAVI_KERNEL_LOG(
            WARN, "[%s] session schedule queue full, limit [%lu], dropped", _desc.c_str(), _scheduleQueueMax);
        item->destroy();
        return false;
    }
    NAVI_KERNEL_LOG(SCHEDULE1,
                    "[%s] before push schedule queue, queue size[%lu] "
                    "processing count[%lu]",
                    _desc.c_str(),
                    queueSize,
                    processingCount);
    _scheduleQueue.Push(item);
    schedule();
    return true;
}

void TaskQueue::schedule() {
    if (size_t processingCount = atomic_read(&_processingCount); processingCount >= _processingMax) {
        NAVI_KERNEL_LOG(SCHEDULE2,
                        "[%s] processing count[%lu] exceed, skip, schedule queue size[%lu]",
                        _desc.c_str(),
                        processingCount,
                        _scheduleQueue.Size());
        return;
    }
    NAVI_KERNEL_LOG(SCHEDULE2, "[%s] before pop schedule queue, queue size[%lu]", _desc.c_str(), _scheduleQueue.Size());
    TaskQueueScheduleItemBase *item = nullptr;
    if (_scheduleQueue.Pop(&item)) {
        bool sync = _testMode != TM_NONE;
        NAVI_KERNEL_LOG(SCHEDULE1, "[%s] item[%p] poped, sync[%d]", _desc.c_str(), item, sync);
        incProcessingCount();
        item->setSyncMode(sync);
        _threadPool->push(item);
    }
}

void TaskQueue::scheduleNext() {
    NAVI_KERNEL_LOG(SCHEDULE2, "[%s] start", _desc.c_str());
    decProcessingCount();
    schedule();
}

void TaskQueue::incProcessingCount() {
    assert(atomic_read(&_processingCount) >= 0 &&
           "processing count must not be negative");
    atomic_inc(&_processingCount);
    NAVI_KERNEL_LOG(SCHEDULE1,
                    "[%s] inc processing count, queue size[%lu] processing count[%lld]",
                    _desc.c_str(),
                    _scheduleQueue.Size(),
                    atomic_read(&_processingCount));
}

void TaskQueue::decProcessingCount() {
    NAVI_KERNEL_LOG(SCHEDULE1,
                    "[%s] dec processing count, queue size[%lu] processing count[%lld]",
                    _desc.c_str(),
                    _scheduleQueue.Size(),
                    atomic_read(&_processingCount));
    atomic_dec(&_processingCount);
    assert(atomic_read(&_processingCount) >= 0 &&
           "processing count must not be negative");
}

TaskQueueStat TaskQueue::getStat() const {
    TaskQueueStat stat;
    stat.activeThreadCount = _threadPool->getRunningThreadCount();
    stat.activeThreadQueueSize = _threadPool->getQueueSize();
    stat.idleThreadQueueSize = _threadPool->getIdleQueueSize();
    stat.processingCount = getProcessingCount();
    stat.queueCount = _scheduleQueue.Size();
    stat.processingCountRatio =
        _processingMax > 0 ? stat.processingCount * 100 / _processingMax : 0;
    stat.queueCountRatio =
        _scheduleQueueMax > 0 ? stat.queueCount * 100 / _scheduleQueueMax : 0;
    return stat;
}

}
