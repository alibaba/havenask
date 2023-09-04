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

#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/ThreadPool.h"
#include "autil/WorkItem.h"
#include "swift/admin/AdminZkDataAccessorPool.h"
#include "swift/admin/WorkerInfo.h"
#include "swift/admin/modules/BaseModule.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace admin {

class DispatchWorkItem : public autil::WorkItem {
public:
    DispatchWorkItem(const AdminZkDataAccessorPoolPtr &accessorPool,
                     const WorkerInfoPtr &worker,
                     const protocol::DispatchInfo &info);
    ~DispatchWorkItem();

public:
    void drop();
    void process();

private:
    AdminZkDataAccessorPoolPtr _accessorPool;
    WorkerInfoPtr _workerInfo;
    protocol::DispatchInfo _dispatchInfo;

private:
    AUTIL_LOG_DECLARE();
};

class MultiThreadTaskDispatcherModule : public BaseModule {
public:
    MultiThreadTaskDispatcherModule();
    ~MultiThreadTaskDispatcherModule();
    MultiThreadTaskDispatcherModule(const MultiThreadTaskDispatcherModule &) = delete;
    MultiThreadTaskDispatcherModule &operator=(const MultiThreadTaskDispatcherModule &) = delete;

public:
    bool doLoad() override;
    bool doUnload() override;
    void dispatchTasks(const std::vector<std::pair<WorkerInfoPtr, protocol::DispatchInfo>> &tasks);

private:
    autil::ThreadPoolPtr _threadPool;
    AdminZkDataAccessorPoolPtr _accessorPool;
    autil::ThreadMutex _lock;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace admin
} // namespace swift
