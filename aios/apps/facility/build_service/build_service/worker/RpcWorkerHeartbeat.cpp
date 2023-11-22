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
#include "build_service/worker/RpcWorkerHeartbeat.h"

#include <arpc/ThreadPoolDescriptor.h>
#include <cstddef>

#include "alog/Logger.h"
#include "autil/ClosureGuard.h"
#include "autil/TimeUtility.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/proto/ProtoUtil.h"

using namespace std;
using namespace build_service::proto;
using namespace build_service::common;

namespace build_service { namespace worker {
BS_LOG_SETUP(worker, RpcWorkerHeartbeat);
const size_t RpcWorkerHeartbeat::RPC_SERVICE_THREAD_NUM = 1;
const size_t RpcWorkerHeartbeat::RPC_SERVICE_QUEUE_SIZE = 16;
const string RpcWorkerHeartbeat::RPC_SERVICE_THEAD_POOL_NAME = "bs_worker_heartbeat";

RpcWorkerHeartbeat::RpcWorkerHeartbeat(const std::string& identifier, proto::PartitionId partitionId,
                                       WorkerNodeBase* workerNode, WorkerBase* workerBase)
    : WorkerHeartbeat(workerNode)
    , _identifier(identifier)
    , _partitionId(partitionId)
    , _workerBase(workerBase)
    , _started(false)
{
}

RpcWorkerHeartbeat::~RpcWorkerHeartbeat() { stop(); }

void RpcWorkerHeartbeat::stop() { setStarted(false); }

bool RpcWorkerHeartbeat::start()
{
    // one heartbeat thread
    arpc::ThreadPoolDescriptor tpd =
        arpc::ThreadPoolDescriptor(RPC_SERVICE_THEAD_POOL_NAME, RPC_SERVICE_THREAD_NUM, RPC_SERVICE_QUEUE_SIZE);

    if (!_workerBase->registerService(this, tpd)) {
        BS_LOG(ERROR, "register rpc service failed!");
        return false;
    }
    return true;
}

void RpcWorkerHeartbeat::heartbeat(::google::protobuf::RpcController* controller, const proto::TargetRequest* request,
                                   proto::CurrentResponse* response, ::google::protobuf::Closure* done)
{
    autil::ClosureGuard closure(done);
    if (!isStarted()) {
        return;
    }
    string roleId;
    ProtoUtil::partitionIdToRoleId(_partitionId, roleId);
    response->set_identifier(_identifier);
    response->set_starttimestamp(_startTimestamp);
    response->set_roleid(roleId);
    *response->mutable_partitionid() = _partitionId;

    if (!ProtoUtil::checkHeartbeat(*request, _partitionId, true)) {
        return;
    }

    _workerNode->setTargetStatusStr(request->targetstate());
    response->set_currentstate(_workerNode->getCurrentStatusStr());
    _workerNode->setTargetDependResources(request->dependresourcesstr());
    string currentResource;
    _workerNode->getCurrentResources(currentResource);
    response->set_usingresourcesstr(currentResource);
}

bool RpcWorkerHeartbeat::isStarted() const
{
    autil::ScopedLock lock(_startedLock);
    return _started;
}

void RpcWorkerHeartbeat::setStarted(bool started)
{
    autil::ScopedLock lock(_startedLock);
    if (!started) {
        _startTimestamp = -1;
    } else if (!_started) {
        _startTimestamp = autil::TimeUtility::currentTimeInSeconds();
    }
    _started = started;
}

}} // namespace build_service::worker
