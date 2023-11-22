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

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/common/CpuSpeedEstimater.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/proto/RoleNameGenerator.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"
#include "build_service/util/StatisticUtil.h"

namespace build_service { namespace taskcontroller {

// NodeStatusManager only collect node status and do some analysis
// it will not change worker node status, all Node parameters should be "const Node&"
class NodeStatusManager
{
public:
    struct NodeStatus : public autil::legacy::Jsonizable {
        NodeStatus(const std::string& name, const std::string& groupName)
            : nodeStartedTime(-1)
            , firstCreatedTime(autil::TimeUtility::currentTimeInMicroSeconds())
            , lastReportTime(autil::TimeUtility::currentTimeInMicroSeconds())
            , lastActiveTimeInLocal(autil::TimeUtility::currentTimeInMicroSeconds())
            , killTimes(0)
            , lastKillStartedTime(0)
            , finishTimeInLocal(0)
            , roleName(name)
            , roleGroupName(groupName)
            , ip("")
            , cpuSpeed(common::CpuSpeedEstimater::INVALID_CPU_SPEED)
            , cpuSpeedNoColdStart(common::CpuSpeedEstimater::INVALID_CPU_SPEED)
            , networkByteInSpeed(0)
            , networkByteOutSpeed(0)
            , slowNetworkDetectCount(0)
            , processQps(0.0)
            , totalWorkTime(-1)
            , // in micro seconds
            workTimeInLatestSlot(-1)
            , initProgress(0)
            , startCount(0)
            , exceedReleaseLimitTime(-1)
            , backupId(0)
            , buildStep(proto::BUILD_STEP_INC)
            , roleType(proto::ROLE_UNKNOWN)
            , locatorOffset(-1)
            , mergeConfigName("")
            , cpuRatioInWindow(-1)
            , workflowErrorExceedThreadhold(false)
        {
            BS_LOG(INFO, "node status created [%s], createTime [%ld]", roleName.c_str(), firstCreatedTime);
        }
        ~NodeStatus()
        {
            BS_LOG(INFO, "node status released [%s], %s", roleName.c_str(),
                   autil::legacy::ToJsonString(*this, /*isCompact=*/true).c_str());
        }
        int64_t nodeStartedTime;
        int64_t firstCreatedTime;
        int64_t lastReportTime;
        int64_t lastActiveTimeInLocal;
        int64_t killTimes;
        int64_t lastKillStartedTime;
        int64_t finishTimeInLocal;
        std::string roleName;
        std::string roleGroupName;
        std::string ip;
        int64_t cpuSpeed;
        int64_t cpuSpeedNoColdStart;
        uint64_t networkByteInSpeed;
        uint64_t networkByteOutSpeed;
        int32_t slowNetworkDetectCount;
        float processQps;
        int64_t totalWorkTime;
        int64_t workTimeInLatestSlot;
        int64_t initProgress;
        int64_t startCount;
        int64_t exceedReleaseLimitTime;
        uint32_t backupId;
        int32_t buildStep;
        int32_t roleType;
        int64_t locatorOffset;
        std::string mergeConfigName;
        int64_t cpuRatioInWindow;
        bool workflowErrorExceedThreadhold;

        proto::WorkerNodeBasePtr workerNode;
        template <typename Nodes>
        std::shared_ptr<typename proto::WorkerNodes2WorkerTypeTraits<Nodes>::WorkerType> GetWorkerNode()
        {
            return std::dynamic_pointer_cast<typename proto::WorkerNodes2WorkerTypeTraits<Nodes>::WorkerType>(
                workerNode);
        }

        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("node_started_time", nodeStartedTime, nodeStartedTime);
            json.Jsonize("first_created_time", firstCreatedTime, firstCreatedTime);
            json.Jsonize("last_report_time", lastReportTime, lastReportTime);
            json.Jsonize("last_active_time_in_local", lastActiveTimeInLocal, lastActiveTimeInLocal);
            json.Jsonize("kill_times", killTimes, killTimes);
            json.Jsonize("last_kill_started_time", lastKillStartedTime, lastKillStartedTime);
            json.Jsonize("finish_time_in_local", finishTimeInLocal, finishTimeInLocal);
            json.Jsonize("role_name", roleName, roleName);
            json.Jsonize("role_group_name", roleGroupName, roleGroupName);
            json.Jsonize("ip", ip, ip);
            json.Jsonize("cpu_speed", cpuSpeed, cpuSpeed);
            json.Jsonize("cpu_speed_no_cold_start", cpuSpeedNoColdStart, cpuSpeedNoColdStart);
            json.Jsonize("network_bytein_speed", networkByteInSpeed, networkByteInSpeed);
            json.Jsonize("network_byteout_speed", networkByteOutSpeed, networkByteOutSpeed);
            json.Jsonize("process_qps", processQps, processQps);
            json.Jsonize("total_work_time", totalWorkTime, totalWorkTime);
            json.Jsonize("work_time_in_lastest_slot", workTimeInLatestSlot, workTimeInLatestSlot);
            json.Jsonize("init_progress", initProgress, initProgress);
            json.Jsonize("start_count", startCount, startCount);
            json.Jsonize("exceed_release_limit_time", exceedReleaseLimitTime, exceedReleaseLimitTime);
            json.Jsonize("backup_id", backupId, backupId);
            json.Jsonize("build_step", buildStep, buildStep);
            json.Jsonize("role_type", roleType, roleType);
            json.Jsonize("merge_config_name", mergeConfigName, mergeConfigName);
            json.Jsonize("locator_offset", locatorOffset, locatorOffset);
            json.Jsonize("cpu_ratio_in_window", cpuRatioInWindow, cpuRatioInWindow);
            json.Jsonize("workflow_error_exceed_threadhold", workflowErrorExceedThreadhold,
                         workflowErrorExceedThreadhold);
        }

        bool IsIncProcessor() const { return roleType == proto::ROLE_PROCESSOR && buildStep == proto::BUILD_STEP_INC; }
    };
    BS_TYPEDEF_PTR(NodeStatus);

    using NodeStatusPtrVec = std::vector<NodeStatusPtr>;
    // key:  roleGroupId, value: backupIds
    using BackupInfo = std::map<std::string, std::vector<uint32_t>>;
    // roleName -> nodeStatus
    using NodeMap = std::map<std::string, NodeStatusPtr>;
    // roleGroupName -> {roleName -> nodeStatus}
    using NodeGroupMap = std::unordered_map<std::string, NodeMap>;
    using RoleNames = std::unordered_set<std::string>;

    NodeStatusManager(const proto::BuildId& buildId, int64_t startTime, int64_t minimumSampleTime = 60 * 1000 * 1000);
    ~NodeStatusManager();
    NodeStatusManager(const NodeStatusManager&) = delete;
    NodeStatusManager& operator=(const NodeStatusManager&) = delete;

    // TODO: Update is too frequently, maybe we can reduce call Update()
    template <typename Nodes>
    void Update(const Nodes& nodes, int64_t currentTime = -1);

    template <typename NodePtr>
    static bool IsRunning(const NodePtr& node)
    {
        const auto& current = node->getCurrentStatus();
        return current.has_progressstatus() && current.progressstatus().has_reporttimestamp() &&
               current.progressstatus().reporttimestamp() != -1;
    }

    std::string SerializeBackInfo() const;
    BackupInfo DeserializeBackInfo(const std::string& backupInfoStr) const;

    // return if this task finished, finishedNodes tell which nodes are finished
    template <typename Nodes>
    bool CheckFinished(const Nodes& workerNodes, RoleNames* finishedNodes, RoleNames* finishedGroups);

    int64_t GetMinLocator() const { return _minLocatorTs; }

    template <typename T>
    T ExcludeNodes(const T& nodes, const std::vector<std::string>& keyWords) const
    {
        T newNodes;
        for (const auto& [name, node] : nodes) {
            bool skip = false;
            for (const auto& keyWord : keyWords) {
                if (name.find(keyWord) != std::string::npos) {
                    skip = true;
                    break;
                }
            }
            if (!skip) {
                newNodes[name] = node;
            }
        }
        return newNodes;
    }

    NodeGroupMap GetRunningNodes(const std::vector<std::string>& excludeKeyWords) const
    {
        return ExcludeNodes(_runningNodes, excludeKeyWords);
    }
    NodeGroupMap GetStartedNodes(const std::vector<std::string>& excludeKeyWords) const
    {
        return ExcludeNodes(_startedNodes, excludeKeyWords);
    }
    NodeGroupMap GetAllNodeGroups(const std::vector<std::string>& excludeKeyWords) const
    {
        return ExcludeNodes(_nodeStatusMap, excludeKeyWords);
    }
    NodeMap GetFinishedNodes(const std::vector<std::string>& excludeKeyWords) const
    {
        return ExcludeNodes(_finishedNodes, excludeKeyWords);
    }

    std::vector<NodeStatusPtr> GetAllNodes(const std::vector<std::string>& excludeKeyWords) const
    {
        std::vector<NodeStatusPtr> res;
        for (const auto& node : _nodeStatus) {
            if (node == nullptr) {
                continue;
            }
            const auto& roleName = node->roleName;
            bool skip = false;
            for (const auto& keyWord : excludeKeyWords) {
                if (roleName.find(keyWord) != std::string::npos) {
                    skip = true;
                    break;
                }
            }
            if (!skip) {
                res.push_back(node);
            }
        }
        return res;
    }

    const NodeGroupMap& GetRunningNodes() const { return _runningNodes; }
    const NodeGroupMap& GetStartedNodes() const { return _startedNodes; }
    const NodeGroupMap& GetAllNodeGroups() const { return _nodeStatusMap; }

    const std::vector<NodeStatusPtr>& GetAllNodes() const { return _nodeStatus; }
    const NodeMap& GetFinishedNodes() const { return _finishedNodes; }
    size_t GetRoleCountInGroup(const std::string& roleGroupName) const;

    template <typename T>
    void GetStartedNodesAttr(const NodeGroupMap& nodes, const std::string& attrName, std::vector<T>* attributes) const;

    template <typename Node>
    int64_t GetNodeProgress(const Node& node) const;

    int32_t NextBackupId(const std::string& roleGroupName) { return _maxBackIdMap[roleGroupName] + 1; }

    void SetRoleGroupCounter(const std::string& roleGroupName, const std::string& key, uint64_t value)
    {
        _roleGroupCounter[roleGroupName][key] = value;
    }

    uint64_t GetRoleGroupCounter(const std::string& roleGroupName, const std::string& key)
    {
        return _roleGroupCounter[roleGroupName][key];
    }

    std::string GetFirstRoleGroupName() const { return _firstRoleGroupName; }

    bool IsFinishedGroup(const std::string& roleGroupName) const;

    template <typename Nodes>
    int64_t CalculateAverageProgress(const NodeGroupMap& nodes) const;
    int64_t CalculateAverageWorkingTime(const NodeMap& nodes) const;
    int64_t CalculateAverageWorkingTime(const NodeMap& nodes, size_t sampleCount) const;
    float CalculateAverageQps(const NodeGroupMap& nodes) const;

private:
    // debug only
    void TEST_PrintNodes()
    {
        for (const auto& item : _nodeStatusMap) {
            std::cout << "GroupName " << item.first << ": ";
            for (const auto& item2 : item.second) {
                std::cout << "Role: " << item2.first << " ";
            }
            std::cout << std::endl;
        }
    }

    template <typename Node>
    inline bool NeedStopNode(const Node& node) const
    {
        return false;
    }

    template <typename Nodes>
    void UpdateNodeStatusMap(const Nodes& nodes);

    void UpdateFinishedNode(int64_t currentTime, NodeStatusPtr& status, const proto::ProgressStatus& progressStatus);

    template <typename Nodes>
    void UpdateWorkingNode(int64_t currentTime, NodeStatusPtr& status, const proto::ProgressStatus& progressStatus);

    void EraseStartedNode(const NodeStatusPtr& status)
    {
        _startedNodes[status->roleGroupName].erase(status->roleName);
        if (_startedNodes[status->roleGroupName].empty()) {
            _startedNodes.erase(status->roleGroupName);
        }
    }
    void EraseRunningNode(const NodeStatusPtr& status)
    {
        _runningNodes[status->roleGroupName].erase(status->roleName);
        if (_runningNodes[status->roleGroupName].empty()) {
            _runningNodes.erase(status->roleGroupName);
        }
    }
    void EraseNode(const NodeStatusPtr& status)
    {
        EraseStartedNode(status);
        EraseRunningNode(status);
    }

private:
    proto::BuildId _buildId;
    int64_t _startTime;
    int64_t _minimumSampleTime;

    std::vector<NodeStatusPtr> _nodeStatus;
    NodeGroupMap _nodeStatusMap;
    NodeGroupMap _runningNodes;
    NodeGroupMap _startedNodes;
    std::map<std::string, int32_t> _maxBackIdMap; // roleGroupName -> max backupId (default 0)
    NodeMap _finishedNodes;
    std::map<std::string, std::map<std::string, uint64_t>> _roleGroupCounter;
    int64_t _minLocatorTs;
    std::string _firstRoleGroupName;

    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(NodeStatusManager);

template <typename Nodes>
void NodeStatusManager::UpdateWorkingNode(int64_t currentTime, NodeStatusPtr& status,
                                          const proto::ProgressStatus& progressStatus)
{
    // update startedNodeSet and finishedNodeSet
    if (progressStatus.starttimestamp() == -1) {
        EraseNode(status);
        return;
    }
    if (status->GetWorkerNode<Nodes>()->isSuspended()) {
        EraseRunningNode(status);
        return;
    }
    if (progressStatus.startpoint() == INVALID_PROGRESS || progressStatus.progress() == INVALID_PROGRESS) {
        EraseNode(status);
        return;
    }
    status->totalWorkTime = currentTime - status->firstCreatedTime;
    BS_INTERVAL_LOG(15, DEBUG,
                    "update totalWorkTime [%ld] [%s], currentTime[%ld], firstCreatedTime[%ld],"
                    " progressStatus.reporttimestamp[%ld], progressStatus.starttimestamp[%ld]",
                    status->totalWorkTime, status->roleName.c_str(), currentTime, status->firstCreatedTime,
                    progressStatus.reporttimestamp(), progressStatus.starttimestamp());

    status->workTimeInLatestSlot = progressStatus.reporttimestamp() - progressStatus.starttimestamp();
    if (status->workTimeInLatestSlot < _minimumSampleTime) {
        EraseNode(status);
        return;
    }
    status->processQps =
        float(progressStatus.progress() - progressStatus.startpoint()) * 1000000 / status->workTimeInLatestSlot;
    _startedNodes[status->roleGroupName].emplace(status->roleName, status);
    return;
}

template <typename T, typename = void>
struct HasLocatorMethod : std::false_type {
};
template <typename T>
struct HasLocatorMethod<T, std::void_t<decltype(std::declval<T>().currentlocator())>> : std::true_type {
};

template <typename T, typename = void>
struct HasWorkflowErrorExceedThreadholdMethod : std::false_type {
};
template <typename T>
struct HasWorkflowErrorExceedThreadholdMethod<T,
                                              std::void_t<decltype(std::declval<T>().workflowerrorexceedthreadhold())>>
    : std::true_type {
};

template <typename Nodes>
void NodeStatusManager::Update(const Nodes& nodes, int64_t currentTime)
{
    // todo: may need CheckFinished firstly
    if (currentTime == -1) {
        currentTime = autil::TimeUtility::currentTimeInMicroSeconds();
    }
    UpdateNodeStatusMap(nodes);
    _minLocatorTs = -1;
    _runningNodes.clear();
    _startedNodes.clear();
    _finishedNodes.clear();
    _firstRoleGroupName.clear();
    for (size_t i = 0; i < nodes.size(); ++i) {
        bool needPrintNodeInfo = false;
        const auto& node = nodes[i];
        if (node == nullptr) {
            continue;
        }
        const proto::PartitionId& pid = node->getPartitionId();
        std::string roleName = proto::RoleNameGenerator::generateRoleName(pid);
        std::string roleGroupName = proto::RoleNameGenerator::generateRoleGroupName(pid);
        if (i == 0) {
            _firstRoleGroupName = roleGroupName;
        }
        NodeStatusPtr& status = _nodeStatusMap[roleGroupName][roleName];
        auto progressStatus = (node->getCurrentStatus()).progressstatus();
        std::string currentIp = (node->getCurrentStatus()).longaddress().ip();
        if (progressStatus.starttimestamp() != -1) {
            // when node started, lastActiveTimeInLocal move to current time
            status->lastActiveTimeInLocal = currentTime;
        }
        status->buildStep = pid.step();
        status->roleType = pid.role();
        if (pid.has_mergeconfigname()) {
            status->mergeConfigName = pid.mergeconfigname();
        }
        if (status->initProgress == INVALID_PROGRESS && (progressStatus.startpoint() != INVALID_PROGRESS)) {
            status->initProgress = progressStatus.startpoint();
        }
        bool justMigrate = false;
        if (status->ip != currentIp && currentIp != "") {
            status->ip = currentIp;
            status->startCount = 0;
            justMigrate = true;
        }
        if ((node->getCurrentStatus()).has_machinestatus()) {
            int64_t cpuSpeed = (node->getCurrentStatus()).machinestatus().emacpuspeed();
            int64_t cpuSpeedNoColdStart = (node->getCurrentStatus()).machinestatus().emacpuspeednocoldstart();
            if (cpuSpeedNoColdStart != common::CpuSpeedEstimater::INVALID_CPU_SPEED) {
                status->cpuSpeedNoColdStart = cpuSpeedNoColdStart;
            }
            if (justMigrate || cpuSpeed != common::CpuSpeedEstimater::INVALID_CPU_SPEED) {
                status->cpuSpeed = cpuSpeed;
            }
            status->networkByteInSpeed = (node->getCurrentStatus()).machinestatus().byteinspeed();
            status->networkByteOutSpeed = (node->getCurrentStatus()).machinestatus().byteoutspeed();
            status->cpuRatioInWindow = (node->getCurrentStatus()).machinestatus().cpuratioinwindow();
        }

        if (progressStatus.starttimestamp() != status->nodeStartedTime) {
            status->nodeStartedTime = progressStatus.starttimestamp();
            if (progressStatus.starttimestamp() != -1) {
                status->startCount++;
                needPrintNodeInfo = true;
            }
        }

        if (status->lastReportTime < progressStatus.reporttimestamp()) {
            status->lastReportTime = progressStatus.reporttimestamp();
        }
        if (pid.has_backupid() && pid.backupid() != 0) {
            status->backupId = pid.backupid();
        }

        status->workerNode = node;
        auto current = node->getCurrentStatus();
        if constexpr (HasLocatorMethod<decltype(current)>::value) {
            status->locatorOffset = current.currentlocator().checkpoint();
        } else {
            status->locatorOffset = -1;
        }

        if constexpr (HasWorkflowErrorExceedThreadholdMethod<decltype(current)>::value) {
            status->workflowErrorExceedThreadhold = node->getCurrentStatus().workflowerrorexceedthreadhold();
        } else {
            status->workflowErrorExceedThreadhold = false;
        }
        int64_t locatorTs = progressStatus.locatortimestamp();
        _minLocatorTs = (_minLocatorTs == -1) ? locatorTs : std::min(_minLocatorTs, locatorTs);

        if (node->isFinished()) {
            UpdateFinishedNode(currentTime, status, progressStatus);
            continue;
        }
        _runningNodes[status->roleGroupName].emplace(status->roleName, status);
        UpdateWorkingNode<Nodes>(currentTime, status, progressStatus);
        if (needPrintNodeInfo) {
            BS_LOG(INFO,
                   "node[%p:%s] is started, startTs[%ld], reportTs[%ld], current progress [%ld], start point[%ld], "
                   "start count[%ld]",
                   node.get(), status->roleName.c_str(), progressStatus.starttimestamp(),
                   progressStatus.reporttimestamp(), progressStatus.progress(), progressStatus.startpoint(),
                   status->startCount);
        }
    }
}

template <typename Node>
int64_t NodeStatusManager::GetNodeProgress(const Node& node) const
{
    auto progressStatus = (node->getCurrentStatus()).progressstatus();
    return progressStatus.progress();
}

template <typename Nodes>
int64_t NodeStatusManager::CalculateAverageProgress(const NodeStatusManager::NodeGroupMap& nodes) const
{
    assert(nodes.size());
    std::vector<int64_t> allProgress;
    allProgress.reserve(nodes.size());
    for (const auto& nodeGroupInfo : nodes) {
        const auto& nodeMap = nodeGroupInfo.second;
        int64_t maxNodeProgress = 0;
        for (const auto& nodeStatusInfo : nodeMap) {
            const auto& nodeStatus = nodeStatusInfo.second;
            const auto& workerNode = nodeStatus->GetWorkerNode<Nodes>();
            int64_t nodeProgress = GetNodeProgress(workerNode);
            maxNodeProgress = std::max(maxNodeProgress, nodeProgress);
        }
        allProgress.push_back(maxNodeProgress);
    }
    return util::StatisticUtil::GetPercentile(allProgress, 0.5);
}

template <typename Nodes>
bool NodeStatusManager::CheckFinished(const Nodes& workerNodes, RoleNames* finishedNodes, RoleNames* finishedGroups)
{
    Update(workerNodes);
    for (const auto& node : workerNodes) {
        if (node == nullptr) {
            continue;
        }
        std::string roleGroupName = proto::RoleNameGenerator::generateRoleGroupName(node->getPartitionId());
        std::string roleName = proto::RoleNameGenerator::generateRoleName(node->getPartitionId());

        if (node->isFinished() || NeedStopNode(node)) {
            if (!node->isFinished()) {
                BS_LOG(INFO, "Role Group[%s] has been finished", node->getPartitionId().ShortDebugString().c_str());
            }
            finishedNodes->emplace(roleName);
            finishedGroups->emplace(roleGroupName);
        }
    }
    return finishedGroups->size() == _nodeStatusMap.size();
}

// for single builder task
template <>
inline bool NodeStatusManager::NeedStopNode(const proto::BuilderNodePtr& node) const
{
    const proto::BuilderTarget& target = node->getTargetStatus();
    const proto::BuilderCurrent& current = node->getCurrentStatus();
    return target.has_stoptimestamp() && target == current.targetstatus();
}

// for processor task
template <>
inline bool NodeStatusManager::NeedStopNode(const proto::ProcessorNodePtr& node) const
{
    const proto::ProcessorCurrent& current = node->getCurrentStatus();
    const proto::ProcessorTarget& target = node->getTargetStatus();
    bool hasStopTimestamp = target.has_stoptimestamp();

    if (current.datasourcefinish()) {
        BS_LOG(INFO, "processor[%s] has finished processing data source",
               node->getPartitionId().ShortDebugString().c_str());
        return true;
    } else if (hasStopTimestamp) {
        if (current.has_status() && current.status() == proto::WS_STOPPED) {
            BS_LOG(INFO, "processor[%s] has stopped", node->getPartitionId().ShortDebugString().c_str());
            return true;
        }
    }
    return false;
}

// for merger task
template <>
inline bool NodeStatusManager::NeedStopNode(const proto::MergerNodePtr& node) const
{
    const proto::MergerCurrent& current = node->getCurrentStatus();
    const proto::MergerTarget& target = node->getTargetStatus();
    return target == current.targetstatus();
}

template <>
inline void NodeStatusManager::UpdateWorkingNode<proto::MergerNodes>(int64_t currentTime, NodeStatusPtr& status,
                                                                     const proto::ProgressStatus& progressStatus)
{
    // update startedNodeSet and finishedNodeSet
    if (progressStatus.starttimestamp() == -1) {
        EraseNode(status);
        return;
    }
    if (status->GetWorkerNode<proto::MergerNodes>()->isSuspended()) {
        EraseRunningNode(status);
        return;
    }
    status->totalWorkTime = currentTime - status->firstCreatedTime;
    BS_INTERVAL_LOG(15, DEBUG,
                    "update totalWorkTime [%ld] [%s], currentTime[%ld], firstCreatedTime[%ld],"
                    " progressStatus.reporttimestamp[%ld], progressStatus.starttimestamp[%ld]",
                    status->totalWorkTime, status->roleName.c_str(), currentTime, status->firstCreatedTime,
                    progressStatus.reporttimestamp(), progressStatus.starttimestamp());

    status->workTimeInLatestSlot = progressStatus.reporttimestamp() - progressStatus.starttimestamp();
    if (status->workTimeInLatestSlot < _minimumSampleTime) {
        EraseNode(status);
        return;
    }
    _startedNodes[status->roleGroupName].emplace(status->roleName, status);
    return;
}

template <>
inline void NodeStatusManager::UpdateWorkingNode<proto::TaskNodes>(int64_t currentTime, NodeStatusPtr& status,
                                                                   const proto::ProgressStatus& progressStatus)
{
    // update startedNodeSet and finishedNodeSet
    if (progressStatus.starttimestamp() == -1) {
        EraseNode(status);
        return;
    }
    if (status->GetWorkerNode<proto::TaskNodes>()->isSuspended()) {
        EraseRunningNode(status);
        return;
    }
    status->totalWorkTime = currentTime - status->firstCreatedTime;
    status->workTimeInLatestSlot = progressStatus.reporttimestamp() - progressStatus.starttimestamp();
    BS_INTERVAL_LOG(15, DEBUG,
                    "update totalWorkTime [%ld] [%s], currentTime[%ld], firstCreatedTime[%ld],"
                    " progressStatus.reporttimestamp[%ld], progressStatus.starttimestamp[%ld]",
                    status->totalWorkTime, status->roleName.c_str(), currentTime, status->firstCreatedTime,
                    progressStatus.reporttimestamp(), progressStatus.starttimestamp());

    if (status->workTimeInLatestSlot < _minimumSampleTime) {
        EraseNode(status);
        return;
    }
    _startedNodes[status->roleGroupName].emplace(status->roleName, status);
    return;
}

template <typename Nodes>
void NodeStatusManager::UpdateNodeStatusMap(const Nodes& nodes)
{
    _nodeStatus.clear();
    NodeGroupMap newMap;
    std::map<std::string, std::map<std::string, uint64_t>> newCounterMap;
    std::map<std::string, int32_t> newMaxBackIdMap;

    // some nodes in "nodes" maybe removed or inserted compared to "_nodeStatusMap"
    for (const auto& node : nodes) {
        const proto::PartitionId& pid = node->getPartitionId();
        std::string roleName = proto::RoleNameGenerator::generateRoleName(pid);
        std::string roleGroupName = proto::RoleNameGenerator::generateRoleGroupName(pid);
        newCounterMap[roleGroupName] = _roleGroupCounter[roleGroupName];
        auto& roleGroup = _nodeStatusMap[roleGroupName];
        auto&& iter = roleGroup.find(roleName);
        if (iter != roleGroup.end()) {
            newMap[roleGroupName][roleName] = iter->second;
        } else {
            newMap[roleGroupName][roleName] = std::make_shared<NodeStatus>(roleName, roleGroupName);
        }
        int32_t backId = pid.has_backupid() ? pid.backupid() : 0;
        newMaxBackIdMap[roleGroupName] = std::max(newMaxBackIdMap[roleGroupName], backId);
        _nodeStatus.emplace_back(newMap[roleGroupName][roleName]);
    }
    _nodeStatusMap.swap(newMap);
    _maxBackIdMap.swap(newMaxBackIdMap);
    _roleGroupCounter.swap(newCounterMap);
}

template <typename T>
void NodeStatusManager::GetStartedNodesAttr(const NodeStatusManager::NodeGroupMap& nodes, const std::string& attrName,
                                            std::vector<T>* attributes) const
{
    for (const auto& nodeGroupInfo : nodes) {
        const auto& nodeGroup = nodeGroupInfo.second;
        T maxValue = std::numeric_limits<T>::min();
        for (const auto& node : nodeGroup) {
            const auto& nodeStatus = node.second;
            if (attrName == "cpu_speed") {
                maxValue = std::max(maxValue, static_cast<T>(nodeStatus->cpuSpeed));
            } else if (attrName == "process_qps") {
                maxValue = std::max(maxValue, static_cast<T>(nodeStatus->processQps));
            } else {
                assert(false);
            }
        }
        if (nodeGroup.empty()) {
            BS_INTERVAL_LOG(100, WARN, "nodeGroup[%s] should not be empty!", nodeGroupInfo.first.c_str());
        }
        attributes->push_back(maxValue);
    }
    // for debug
    if (attrName == "cpu_speed") {
        std::sort(attributes->begin(), attributes->end());
        std::string str;
        for (const auto& attr : *attributes) {
            str += std::to_string(attr) + ",";
        }
        BS_INTERVAL_LOG(100, INFO, "firstRoleGroupName:[%s], %s [%lu]:%s", _firstRoleGroupName.c_str(),
                        attrName.c_str(), attributes->size(), str.c_str());
    }
}

}} // namespace build_service::taskcontroller
