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
#include "swift/admin/modules/MultiThreadTaskDispatcherModule.h"

#include <memory>
#include <stdint.h>
#include <string>

#include "autil/TimeUtility.h"
#include "swift/admin/AdminZkDataAccessor.h"
#include "swift/common/CreatorRegistry.h"
#include "swift/config/AdminConfig.h"
#include "swift/util/ProtoUtil.h"
#include "swift/util/TargetRecorder.h"

namespace swift {
namespace admin {

using namespace autil;
using namespace swift::protocol;
using namespace swift::util;

AUTIL_LOG_SETUP(swift, DispatchWorkItem);
DispatchWorkItem::DispatchWorkItem(const AdminZkDataAccessorPoolPtr &accessorPool,
                                   const WorkerInfoPtr &worker,
                                   const DispatchInfo &info)
    : _accessorPool(accessorPool), _workerInfo(worker), _dispatchInfo(info) {}

DispatchWorkItem::~DispatchWorkItem() {}

void DispatchWorkItem::drop() {
    AUTIL_LOG(ERROR, "drop work item, failed to dispatch task on broker[%s].", _dispatchInfo.rolename().c_str());
    destroy();
}

void DispatchWorkItem::process() {
    AUTIL_LOG(INFO, "Dispatch task to %s ", ProtoUtil::getDispatchStr(_dispatchInfo).c_str());
    auto zkAccessor = _accessorPool->getOrCreate();
    auto accessor = std::dynamic_pointer_cast<AdminZkDataAccessor>(zkAccessor);
    if (accessor == nullptr) {
        AUTIL_LOG(ERROR, "get null accessor, failed to dispatch task on broker[%s].", _dispatchInfo.rolename().c_str());
    } else {
        if (!accessor->setDispatchedTask(_dispatchInfo)) {
            AUTIL_LOG(ERROR, "Failed to dispatch task on broker[%s].", _dispatchInfo.rolename().c_str());
        } else {
            if (_workerInfo) {
                _workerInfo->updateLastSessionId();
            }
        }
        _accessorPool->put(zkAccessor);
    }
}

AUTIL_LOG_SETUP(swift, MultiThreadTaskDispatcherModule);

MultiThreadTaskDispatcherModule::MultiThreadTaskDispatcherModule() {}

MultiThreadTaskDispatcherModule::~MultiThreadTaskDispatcherModule() {}

bool MultiThreadTaskDispatcherModule::doLoad() {
    if (!_adminConfig->enableMultiThreadDispatchTask()) {
        return true;
    }
    ScopedLock lock(_lock);
    uint32_t threadNum = _adminConfig->getDispatchTaskThreadNum();
    _threadPool.reset(new ThreadPool(threadNum, 1000));
    if (!_threadPool->start("dp_tasks")) {
        AUTIL_LOG(WARN, "thread dp task thread pool failed, thread num [%u]", threadNum);
        return false;
    }
    _accessorPool.reset(new AdminZkDataAccessorPool(_adminConfig->getZkRoot()));
    if (!_accessorPool->init(threadNum)) {
        AUTIL_LOG(WARN, "init zk data accessor pool failed.");
        return false;
    }
    return true;
}

bool MultiThreadTaskDispatcherModule::doUnload() {
    ScopedLock lock(_lock);
    if (_threadPool) {
        _threadPool->stop();
    }
    if (_accessorPool) {
        _accessorPool.reset();
    }
    return true;
}

void MultiThreadTaskDispatcherModule::dispatchTasks(const std::vector<std::pair<WorkerInfoPtr, DispatchInfo>> &tasks) {
    ScopedLock lock(_lock);
    if (!_accessorPool) {
        return;
    }
    for (const auto &task : tasks) {
        auto *workItem = new DispatchWorkItem(_accessorPool, task.first, task.second);
        if (ThreadPool::ERROR_NONE != _threadPool->pushWorkItem(workItem, true)) {
            AUTIL_LOG(
                WARN, "add  work item failed, failed to dispatch task on broker[%s].", task.second.rolename().c_str());
            delete workItem;
        }
    }
    _threadPool->waitFinish();
}
REGISTER_MODULE(MultiThreadTaskDispatcherModule, "M|S", "L");
} // namespace admin
} // namespace swift
