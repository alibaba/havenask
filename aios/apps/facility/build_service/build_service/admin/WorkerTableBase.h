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

#include <string>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "build_service/admin/Heartbeat.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class WorkerTableBase
{
public:
    WorkerTableBase(const std::string& zkRoot, cm_basic::ZkWrapper* zkWrapper);
    virtual ~WorkerTableBase();

private:
    WorkerTableBase(const WorkerTableBase&);
    WorkerTableBase& operator=(const WorkerTableBase&);

public:
    bool startHeartbeat(const std::string& heartbeatType);
    void stopHeartbeat();
    void syncNodesStatus();
    std::vector<proto::PartitionId> getAllPartitionIds() const;

protected:
    virtual proto::WorkerNodes getActiveNodes() const = 0;
    virtual proto::WorkerNodes getAllNodes() const = 0;

protected:
    Heartbeat* _heartbeat;
    std::string _zkRoot;
    cm_basic::ZkWrapper* _zkWrapper;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(WorkerTableBase);

}} // namespace build_service::admin
