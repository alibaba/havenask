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
#include "build_service/admin/WorkerTableBase.h"

#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "build_service/admin/RpcHeartbeat.h"
#include "build_service/proto/HeartbeatDefine.h"

using namespace std;
using namespace build_service::proto;
namespace build_service { namespace admin {
BS_LOG_SETUP(admin, WorkerTableBase);

WorkerTableBase::WorkerTableBase(const std::string& zkRoot, cm_basic::ZkWrapper* zkWrapper)
    : _heartbeat(NULL)
    , _zkRoot(zkRoot)
    , _zkWrapper(zkWrapper)
{
}

WorkerTableBase::~WorkerTableBase() { DELETE_AND_SET_NULL(_heartbeat); }

bool WorkerTableBase::startHeartbeat(const std::string& heartbeatType)
{
    if (heartbeatType != "rpc") {
        // _heartbeat = new ZkHeartbeat(_zkRoot, _zkWrapper);
        // BS_LOG(INFO, "admin zookeeper heartbeat enabled");
        BS_LOG(ERROR, "build service only support rpc heartbeat Type!, config heartbeatType[%s]",
               heartbeatType.c_str());
        return false;
    }
    _heartbeat = new RpcHeartbeat(_zkRoot, _zkWrapper);
    BS_LOG(INFO, "admin rpc heartbeat enabled");
    return _heartbeat->start(ADMIN_HEARTBEAT_INTERVAL);
}

void WorkerTableBase::stopHeartbeat()
{
    if (_heartbeat) {
        _heartbeat->stop();
    }
    DELETE_AND_SET_NULL(_heartbeat);
}

vector<PartitionId> WorkerTableBase::getAllPartitionIds() const
{
    WorkerNodes nodes = getActiveNodes();
    vector<PartitionId> partitionIds;
    partitionIds.reserve(nodes.size());
    for (size_t i = 0; i < nodes.size(); ++i) {
        partitionIds.push_back(nodes[i]->getPartitionId());
    }
    return partitionIds;
}

void WorkerTableBase::syncNodesStatus()
{
    if (_heartbeat) {
        _heartbeat->syncNodesStatus(getActiveNodes());
    }
}

}} // namespace build_service::admin
