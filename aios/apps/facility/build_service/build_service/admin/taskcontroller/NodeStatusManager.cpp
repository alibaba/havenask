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
#include "build_service/admin/taskcontroller/NodeStatusManager.h"

#include <algorithm>
#include <cstddef>
#include <vector>

#include "autil/legacy/fast_jsonizable_dec.h"

using namespace std;

namespace build_service { namespace taskcontroller {
BS_LOG_SETUP(taskcontroller, NodeStatusManager);

NodeStatusManager::NodeStatusManager(const proto::BuildId& buildId, int64_t startTime, int64_t minimumSampleTime)
    : _buildId(buildId)
    , _startTime(startTime)
    , _minimumSampleTime(minimumSampleTime)
{
    BS_LOG(INFO, "node status manager created [%s %s %u], startTime[%ld], minimumSampleTime[%ld]",
           _buildId.datatable().c_str(), _buildId.appname().c_str(), _buildId.generationid(), _startTime,
           _minimumSampleTime);
}

NodeStatusManager::~NodeStatusManager()
{
    BS_LOG(INFO, "node status manager released [%s %s %s %u], startTime[%ld], minimumSampleTime[%ld]",
           _buildId.datatable().c_str(), _firstRoleGroupName.c_str(), _buildId.appname().c_str(),
           _buildId.generationid(), _startTime, _minimumSampleTime);
}

size_t NodeStatusManager::GetRoleCountInGroup(const std::string& roleGroupName) const
{
    auto iter = _nodeStatusMap.find(roleGroupName);
    return iter == _nodeStatusMap.end() ? 0 : iter->second.size();
}

void NodeStatusManager::UpdateFinishedNode(int64_t currentTime, NodeStatusPtr& status,
                                           const proto::ProgressStatus& progressStatus)
{
    if (status->finishTimeInLocal != 0) {
        _finishedNodes[status->roleGroupName] = status;
        _startedNodes[status->roleGroupName].emplace(status->roleName, status);
        return;
    }
    status->finishTimeInLocal = currentTime;
    BS_INTERVAL_LOG(15, DEBUG,
                    "update totalWorkTime [%ld] [%s], currentTime[%ld], firstCreatedTime[%ld],"
                    " progressStatus.reporttimestamp[%ld], progressStatus.starttimestamp[%ld]",
                    status->totalWorkTime, status->roleName.c_str(), currentTime, status->firstCreatedTime,
                    progressStatus.reporttimestamp(), progressStatus.starttimestamp());

    status->totalWorkTime = std::max((status->finishTimeInLocal - status->firstCreatedTime), int64_t(1000 * 1000));
    status->workTimeInLatestSlot = progressStatus.reporttimestamp() - progressStatus.starttimestamp();
    if (progressStatus.progress() == INVALID_PROGRESS) {
        status->processQps = 0.0;
    } else {
        status->processQps = float(progressStatus.progress() - status->initProgress) * 1000000 / status->totalWorkTime;
    }
    if (status->cpuSpeed == common::CpuSpeedEstimater::INVALID_CPU_SPEED &&
        status->cpuSpeedNoColdStart != status->cpuSpeed) {
        BS_LOG(INFO, "node[%s] ip[%s] is finished early, fill node status cpu speed with the no cold start value[%ld].",
               status->roleName.c_str(), status->ip.c_str(), status->cpuSpeedNoColdStart);
        status->cpuSpeed = status->cpuSpeedNoColdStart;
    }
    _finishedNodes[status->roleGroupName] = status;
    _startedNodes[status->roleGroupName].emplace(status->roleName, status);
}

float NodeStatusManager::CalculateAverageQps(const NodeStatusManager::NodeGroupMap& nodes) const
{
    std::vector<float> allQps;
    allQps.reserve(nodes.size());
    GetStartedNodesAttr(nodes, "process_qps", &allQps);
    return util::StatisticUtil::GetPercentile(allQps, /*percent=*/0.5);
}

int64_t NodeStatusManager::CalculateAverageWorkingTime(const NodeMap& nodes) const
{
    std::vector<int64_t> allWorkingTime;
    allWorkingTime.reserve(nodes.size());
    for (const auto& nodeGroupInfo : nodes) {
        const auto& nodeStatus = nodeGroupInfo.second;
        allWorkingTime.push_back(nodeStatus->totalWorkTime);
    }
    return util::StatisticUtil::GetPercentile(allWorkingTime, /*percent=*/0.5);
}

int64_t NodeStatusManager::CalculateAverageWorkingTime(const NodeMap& nodes, size_t sampleCount) const
{
    std::vector<int64_t> allWorkingTime;
    allWorkingTime.reserve(nodes.size());
    for (const auto& nodeGroupInfo : nodes) {
        const auto& nodeStatus = nodeGroupInfo.second;
        allWorkingTime.push_back(nodeStatus->totalWorkTime);
    }
    std::sort(allWorkingTime.begin(), allWorkingTime.end());
    sampleCount = std::min(nodes.size(), sampleCount);
    if (sampleCount < 1) {
        sampleCount = 1;
    }
    size_t medianIdx = (sampleCount - 1) / 2;
    std::nth_element(allWorkingTime.begin(), allWorkingTime.begin() + medianIdx, allWorkingTime.begin() + sampleCount);
    return allWorkingTime[medianIdx];
}

std::string NodeStatusManager::SerializeBackInfo() const
{
    BackupInfo backupInfo;
    unordered_map<string, uint32_t> maxProcessorBackupId;
    for (const auto& nodeGroup : _nodeStatusMap) {
        for (const auto& node : nodeGroup.second) {
            const std::string& roleGroupName = nodeGroup.first;
            const auto& nodeStatus = node.second;
            if (nodeStatus->backupId != 0) {
                if (nodeStatus->workerNode->getPartitionId().role() == proto::ROLE_PROCESSOR) {
                    auto workerNode = nodeStatus->GetWorkerNode<proto::ProcessorNodes>();
                    // processor only serialize max running node
                    if (IsRunning(workerNode)) {
                        maxProcessorBackupId[roleGroupName] =
                            max(maxProcessorBackupId[roleGroupName], nodeStatus->backupId);
                    }
                } else {
                    backupInfo[roleGroupName].emplace_back(nodeStatus->backupId);
                }
            }
        }
    }
    // add processor backup id
    for (auto backupId : maxProcessorBackupId) {
        backupInfo[backupId.first].emplace_back(backupId.second);
    }
    return autil::legacy::FastToJsonString(backupInfo);
}

NodeStatusManager::BackupInfo NodeStatusManager::DeserializeBackInfo(const std::string& backupInfoStr) const
{
    BackupInfo backupInfo;
    try {
        autil::legacy::FastFromJsonString(backupInfo, backupInfoStr);
    } catch (...) {
        BS_LOG(ERROR, "parse json from backupInfoStr failed.");
    }
    return backupInfo;
}

bool NodeStatusManager::IsFinishedGroup(const std::string& roleGroupName) const
{
    auto iter = _finishedNodes.find(roleGroupName);
    if (iter != _finishedNodes.end()) {
        return true;
    }
    return false;
}

}} // namespace build_service::taskcontroller
