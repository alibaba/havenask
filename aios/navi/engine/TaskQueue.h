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

namespace navi {

class NaviWorkerBase;
class NaviThreadPool;

struct TaskQueue {
public:
    TaskQueue() = default;
    ~TaskQueue() = default;
    TaskQueue(const TaskQueue&) = delete;
    TaskQueue &operator=(const TaskQueue&) = delete;
public:
    std::string name;
    std::shared_ptr<NaviThreadPool> threadPool;
    atomic64_t processingCount;
    size_t processingMax;
    size_t scheduleQueueMax;
    arpc::common::LockFreeQueue<NaviWorkerBase *> sessionScheduleQueue;
};

NAVI_TYPEDEF_PTR(TaskQueue);

} // end namespace navi
