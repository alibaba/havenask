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
#include "swift/admin/modules/WorkerStatusModule.h"

#include <algorithm>
#include <memory>
#include <stddef.h>
#include <string>

#include "autil/TimeUtility.h"
#include "swift/admin/SysController.h"
#include "swift/admin/TopicInStatusManager.h"
#include "swift/admin/WorkerInfo.h"
#include "swift/admin/WorkerStatusWorkItem.h"
#include "swift/admin/WorkerTable.h"
#include "swift/common/Common.h"
#include "swift/common/CreatorRegistry.h"
#include "swift/common/PathDefine.h"
#include "swift/config/AdminConfig.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/Heartbeat.pb.h"

namespace swift {
namespace admin {

using namespace autil;
using namespace swift::protocol;
using namespace swift::common;

AUTIL_LOG_SETUP(swift, WorkerStatusModule);

WorkerStatusModule::WorkerStatusModule() {}

WorkerStatusModule::~WorkerStatusModule() {}

bool WorkerStatusModule::doInit() {
    _workerTable = _sysController->getWorkerTable();
    _topicInStatusManager = _sysController->getTopicInStatusManager();
    return true;
}

bool WorkerStatusModule::doLoad() {
    _threadPool.reset(new autil::ThreadPool(2, 100));
    _threadPool->start("worker_status");
    return true;
}

bool WorkerStatusModule::doUnload() {
    if (_threadPool) {
        _threadPool->stop();
    }
    return true;
}

bool WorkerStatusModule::reportBrokerStatus(const BrokerStatusRequest *request, BrokerStatusResponse *response) {
    const BrokerInMetric &status = request->status();
    BrokerStatusWorkItem *item = new BrokerStatusWorkItem(this, status);
    if (_threadPool) {
        if (ThreadPool::ERROR_NONE != _threadPool->pushWorkItem(item)) {
            AUTIL_LOG(WARN, "push broker status work item failed, request[%s]", request->ShortDebugString().c_str());
            delete item;
            return false;
        }
    }
    return true;
}

bool WorkerStatusModule::updateBrokerInMetric(const BrokerInMetric &bMetric) {
    std::string roleName, address;
    bool ret = PathDefine::parseRoleAddress(bMetric.rolename(), roleName, address);
    if (!ret) {
        AUTIL_LOG(ERROR, "request roleName[%s] invalid", bMetric.rolename().c_str());
        return false;
    }
    BrokerInStatus bStatus;
    bStatus.updateTime = bMetric.reporttime();
    bStatus.cpu = bMetric.cpuratio();
    bStatus.mem = bMetric.memratio();
    bStatus.zfsTimeout = bMetric.zfstimeout();
    for (size_t index = 0; index < bMetric.partinmetrics_size(); ++index) {
        const PartitionInMetric &metric = bMetric.partinmetrics(index);
        bStatus.writeRate1min += metric.writerate1min();
        bStatus.writeRate5min += metric.writerate5min();
        bStatus.readRate1min += metric.readrate1min();
        bStatus.readRate5min += metric.readrate5min();
        bStatus.writeRequest1min += metric.writerequest1min();
        bStatus.writeRequest5min += metric.writerequest5min();
        bStatus.readRequest1min += metric.readrequest1min();
        bStatus.readRequest5min += metric.readrequest5min();
        bStatus.commitDelay = std::max(bStatus.commitDelay, metric.commitdelay());
    }
    _topicInStatusManager->addPartInMetric(bMetric.partinmetrics(),
                                           bMetric.reporttime(),
                                           _adminConfig->getBrokerNetLimitMB(),
                                           _adminConfig->getTopicResourceLimit());
    return _workerTable->addBrokerInMetric(roleName, bStatus);
}

REGISTER_MODULE(WorkerStatusModule, "M|S", "L");

} // namespace admin
} // namespace swift
