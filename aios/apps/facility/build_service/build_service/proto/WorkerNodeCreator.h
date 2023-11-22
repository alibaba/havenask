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

#include "build_service/common_define.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"
#include "build_service/util/RangeUtil.h"
#include "indexlib/misc/common.h"

namespace build_service { namespace proto {

template <proto::RoleType ROLE_TYPE>
class WorkerNodeCreator
{
public:
    typedef proto::WorkerNode<ROLE_TYPE> WorkerNodeTyped;
    typedef std::shared_ptr<WorkerNodeTyped> WorkerNodeTypedPtr;

public:
    static proto::PartitionId createPartitionId(uint32_t partitionCount, uint32_t parallelNum, uint32_t roleIdx,
                                                proto::BuildStep buildStep, const proto::BuildId& buildId,
                                                const std::string& clusterName = "", const std::string& nodeName = "",
                                                const std::string& taskId = "")
    {
        proto::PartitionId pid;
        pid.set_role(ROLE_TYPE);
        if (ROLE_TYPE != proto::ROLE_MERGER && ROLE_TYPE != proto::ROLE_TASK) {
            pid.set_step(buildStep);
        }
        *pid.mutable_buildid() = buildId;
        if (!clusterName.empty()) {
            *pid.add_clusternames() = clusterName;
        }
        if (!nodeName.empty()) {
            pid.set_mergeconfigname(nodeName);
        }
        if (!taskId.empty()) {
            pid.set_taskid(taskId);
        }
        std::vector<proto::Range> ranges =
            util::RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, partitionCount, parallelNum);
        *pid.mutable_range() = ranges[roleIdx];
        return pid;
    }

    static WorkerNodeTypedPtr createBackupNode(WorkerNodeTypedPtr srcNode, uint32_t backupId)
    {
        proto::PartitionId targetPid = *srcNode->getPartitionInfo().mutable_pid();
        targetPid.set_backupid(backupId);
        WorkerNodeTypedPtr workerNode(new WorkerNodeTyped(targetPid));
        workerNode->setTargetStatus(srcNode->getTargetStatus());
        if (srcNode->getBackupId() == uint32_t(-1)) { // main node
            workerNode->setSourceNodeId(srcNode->getNodeId());
        } else { // already backup node
            workerNode->setSourceNodeId(srcNode->getSourceNodeId());
        }

        return workerNode;
    }

    static std::vector<WorkerNodeTypedPtr> createNodes(uint32_t partitionCount, uint32_t parallelNum,
                                                       proto::BuildStep buildStep, const proto::BuildId& buildId,
                                                       const std::string& clusterName = "",
                                                       const std::string& mergeConfigName = "",
                                                       const std::string& taskId = "")
    {
        proto::PartitionId defaultPid;
        defaultPid.set_role(ROLE_TYPE);
        if (ROLE_TYPE != proto::ROLE_MERGER) {
            defaultPid.set_step(buildStep);
        }
        *defaultPid.mutable_buildid() = buildId;
        if (!clusterName.empty()) {
            *defaultPid.add_clusternames() = clusterName;
        }
        if (!mergeConfigName.empty()) {
            defaultPid.set_mergeconfigname(mergeConfigName);
        }
        if (!taskId.empty()) {
            defaultPid.set_taskid(taskId);
        }
        std::vector<proto::Range> ranges =
            util::RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, partitionCount, parallelNum);
        std::vector<WorkerNodeTypedPtr> workerNodes;
        workerNodes.reserve(ranges.size());
        for (size_t i = 0; i < ranges.size(); i++) {
            proto::PartitionId pid = defaultPid;
            *pid.mutable_range() = ranges[i];
            WorkerNodeTypedPtr workerNode(new WorkerNodeTyped(pid));
            workerNodes.push_back(workerNode);
        }
        return workerNodes;
    }
    static std::vector<proto::WorkerNodeBasePtr>
    createBaseNodes(uint32_t partitionCount, uint32_t parallelNum, proto::BuildStep buildStep,
                    const proto::BuildId& buildId, const std::string& clusterName = "",
                    const std::string& mergeConfigName = "", const std::string& taskId = "")
    {
        std::vector<proto::WorkerNodeBasePtr> res;
        std::vector<WorkerNodeTypedPtr> workerNodes =
            createNodes(partitionCount, parallelNum, buildStep, buildId, clusterName, mergeConfigName, taskId);
        res.reserve(workerNodes.size());
        for (size_t i = 0; i < workerNodes.size(); i++) {
            res.push_back(DYNAMIC_POINTER_CAST(proto::WorkerNodeBase, workerNodes[i]));
        }
        return res;
    }

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::proto
