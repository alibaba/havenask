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
#ifndef ISEARCH_BS_NEWSLOWNODEDETECTSTRATEGY_H
#define ISEARCH_BS_NEWSLOWNODEDETECTSTRATEGY_H

#include "build_service/admin/CpuSpeedDetectStrategy.h"
#include "build_service/admin/LocatorDetectStrategy.h"
#include "build_service/admin/SlowNodeDetectStrategy.h"
#include "build_service/admin/taskcontroller/NodeStatusManager.h"
#include "build_service/common/CpuSpeedEstimater.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

template <typename Nodes>
class NewSlowNodeDetectStrategy : public SlowNodeDetectStrategy
{
public:
    using NodeStatusPtr = taskcontroller::NodeStatusManager::NodeStatusPtr;
    using NodeStatusMap = std::map<std::string, NodeStatusPtr>;
    using NodeStatusPtrVec = taskcontroller::NodeStatusManager::NodeStatusPtrVec;

    NewSlowNodeDetectStrategy(const config::SlowNodeDetectConfig& config, int64_t startTime);
    ~NewSlowNodeDetectStrategy() = default;
    NewSlowNodeDetectStrategy(const NewSlowNodeDetectStrategy&) = delete;
    NewSlowNodeDetectStrategy& operator=(const NewSlowNodeDetectStrategy&) = delete;

    void UniqueNodes(NodeStatusPtrVec* slowNodes) const
    {
        std::sort(slowNodes->begin(), slowNodes->end(), [](const NodeStatusPtr& left, const NodeStatusPtr& right) {
            return left->roleGroupName < right->roleGroupName;
        });
        auto iter = std::unique(slowNodes->begin(), slowNodes->end(),
                                [](const NodeStatusPtr& left, const NodeStatusPtr& right) {
                                    return left->roleGroupName == right->roleGroupName;
                                });
        slowNodes->erase(iter, slowNodes->end());
    }

    void DetectSlowNode(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, int64_t currentTime,
                        NodeStatusPtrVec* slowNodes) const override
    {
        DetectSlowByLocator(nodeStatusManager, currentTime, slowNodes);
        DetectLongTailNode(nodeStatusManager, currentTime, slowNodes);
        DetectSlowCpuSpeedNode(nodeStatusManager, currentTime, slowNodes);
        DetectSlowNetworkSpeedNode(nodeStatusManager, currentTime, slowNodes);
        UniqueNodes(slowNodes);
    }

    int64_t GetHighestCpuSpeedInNodeGroup(const NodeStatusMap& nodeMap) const
    {
        int64_t maxCpuSpeed = common::CpuSpeedEstimater::INVALID_CPU_SPEED;
        for (const auto& nodeStatusInfo : nodeMap) {
            const auto& nodeStatus = nodeStatusInfo.second;
            if (nodeStatus->cpuSpeed > maxCpuSpeed) {
                maxCpuSpeed = nodeStatus->cpuSpeed;
            }
        }
        return maxCpuSpeed;
    }

    bool HasFastNetworkNodeInNodeGroup(const NodeStatusMap& nodeMap, const uint64_t networkSpeedThreshold) const
    {
        for (const auto& nodeStatusInfo : nodeMap) {
            const auto& nodeStatus = nodeStatusInfo.second;
            if (nodeStatus->networkByteInSpeed < networkSpeedThreshold &&
                nodeStatus->networkByteOutSpeed < networkSpeedThreshold) {
                return true;
            }
        }
        return false;
    }

    inline NodeStatusPtr GetCandidateNodeStatus(const NodeStatusMap& nodeMap,
                                                const taskcontroller::NodeStatusManagerPtr& nodeStatusManager) const
    {
        int candidateNodeProgress = -1;
        NodeStatusPtr candidateNodeStatus;
        for (const auto& nodeStatusInfo : nodeMap) {
            const auto& nodeStatus = nodeStatusInfo.second;
            const auto& node = nodeStatus->GetWorkerNode<Nodes>();
            if (node->isFinished()) {
                return NodeStatusPtr();
            }
            if (nodeStatusManager->IsFinishedGroup(nodeStatus->roleGroupName)) {
                return NodeStatusPtr();
            }
            int nodeProgress = nodeStatusManager->GetNodeProgress(node);
            if (candidateNodeProgress < 0 || nodeProgress > candidateNodeProgress) {
                candidateNodeStatus = nodeStatus;
                candidateNodeProgress = nodeProgress;
            }
        }
        return candidateNodeStatus;
    }

    NodeStatusPtr GetNodeStatus(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager,
                                NodeStatusPtrVec* slowNodes,
                                const taskcontroller::NodeStatusManager::NodeMap& nodeMap) const
    {
        if (nodeMap.empty()) {
            return nullptr;
        }
        auto candidateNodeStatus = GetCandidateNodeStatus(nodeMap, nodeStatusManager);
        if (!candidateNodeStatus) {
            return nullptr;
        }
        if (candidateNodeStatus->lastKillStartedTime == candidateNodeStatus->nodeStartedTime) {
            return nullptr;
        }
        return candidateNodeStatus;
    }

    taskcontroller::NodeStatusManager::NodeGroupMap
    GetRunningNodes(const std::string& excludeKey, const taskcontroller::NodeStatusManagerPtr& nodeStatusManager) const
    {
        std::vector<std::string> excludeKeyWords = GetExcludeKeyWords(excludeKey);
        const auto& startedNodes = nodeStatusManager->GetStartedNodes(excludeKeyWords);
        if (startedNodes.size() == 1) {
            return {};
        }
        auto allNodeGroups = nodeStatusManager->GetAllNodeGroups(excludeKeyWords);
        uint32_t minimumSampleCount = std::max(2u, (uint32_t)(allNodeGroups.size() * _slowNodeSampleRatio));
        if (startedNodes.size() < minimumSampleCount) {
            BS_INTERVAL_LOG(300, WARN, "[%s] started nodes size [%lu] is smaller than minimum sample count[%u]",
                            nodeStatusManager->GetFirstRoleGroupName().c_str(), startedNodes.size(),
                            minimumSampleCount);
            return {};
        }

        return nodeStatusManager->GetRunningNodes(excludeKeyWords);
    }

    void DetectSlowNetworkSpeedNode(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, int64_t currentTime,
                                    NodeStatusPtrVec* slowNodes) const
    {
        if (!_enableNetworkSpeedStrategy) {
            return;
        }
        int32_t slowNetworkDetectCountLimit = 120;
        for (auto& nodeGroupInfo : GetRunningNodes("network", nodeStatusManager)) {
            auto candidateNodeStatus = GetNodeStatus(nodeStatusManager, slowNodes, nodeGroupInfo.second);
            if (!candidateNodeStatus) {
                continue;
            }
            if (HasFastNetworkNodeInNodeGroup(nodeGroupInfo.second, _networkSpeedThreshold)) {
                continue;
            }
            if (candidateNodeStatus->networkByteInSpeed > _networkSpeedThreshold ||
                candidateNodeStatus->networkByteOutSpeed > _networkSpeedThreshold) {
                candidateNodeStatus->slowNetworkDetectCount++;
                if (candidateNodeStatus->slowNetworkDetectCount < slowNetworkDetectCountLimit) {
                    BS_LOG(DEBUG,
                           "trigger network. slowNetworkDetectCount[%d] < slowNetworkDetectCountLimit[%d],"
                           "node will not be selected, slow network speed node ip[%s], roleName[%s], byteInSpeed[%lu],"
                           " byteOutSpeed[%lu], networkSpeedThreshold[%lu]",
                           candidateNodeStatus->slowNetworkDetectCount, slowNetworkDetectCountLimit,
                           candidateNodeStatus->ip.c_str(), candidateNodeStatus->roleName.c_str(),
                           candidateNodeStatus->networkByteInSpeed, candidateNodeStatus->networkByteOutSpeed,
                           _networkSpeedThreshold);
                } else {
                    slowNodes->emplace_back(candidateNodeStatus);
                    BS_LOG(INFO,
                           "trigger network. select slow network speed node ip[%s], slowNetworkDetectCount[%d],"
                           "roleName[%s], byteInSpeed[%lu], byteOutSpeed[%lu], networkSpeedThreshold[%lu]",
                           candidateNodeStatus->ip.c_str(), candidateNodeStatus->slowNetworkDetectCount,
                           candidateNodeStatus->roleName.c_str(), candidateNodeStatus->networkByteInSpeed,
                           candidateNodeStatus->networkByteOutSpeed, _networkSpeedThreshold);
                }
            }
        }
    }

    void DetectSlowByLocator(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, int64_t currentTime,
                             NodeStatusPtrVec* slowNodes) const
    {
        // TODO(tianxiao) 重构相同的逻辑抽出去
        if (!_enableDetectSlowByLocator) {
            return;
        }
        LocatorDetectStrategy strategy(_locatorCheckCondition, _locatorCheckGap);
        std::vector<std::string> excludeKeyWords = GetExcludeKeyWords("locator");
        const auto& runningNodes = nodeStatusManager->GetRunningNodes(excludeKeyWords);
        for (auto& nodeGroupInfo : runningNodes) {
            auto candidateNodeStatus = GetNodeStatus(nodeStatusManager, slowNodes, nodeGroupInfo.second);
            if (!candidateNodeStatus) {
                continue;
            }
            std::string workIdentifier = candidateNodeStatus->roleName + "@" + candidateNodeStatus->ip;
            bool isSlowNode = strategy.detect(candidateNodeStatus->locatorOffset, workIdentifier);
            if (isSlowNode) {
                slowNodes->emplace_back(candidateNodeStatus);
            }
        }
    }

    void DetectLongTailNode(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, int64_t currentTime,
                            NodeStatusPtrVec* slowNodes) const
    {
        // use excludeKeyWords temporarily until a better solution comes up
        std::string excludeKey = "long_tail";
        std::vector<std::string> excludeKeyWords = GetExcludeKeyWords(excludeKey);
        const auto& startedNodes = nodeStatusManager->GetStartedNodes(excludeKeyWords);
        auto finishedNodes = nodeStatusManager->GetFinishedNodes(excludeKeyWords);
        uint32_t minimumSampleCountForLongTail =
            std::max(1u, (uint32_t)(startedNodes.size() * _longTailNodeSampleRatio));

        if (finishedNodes.size() < minimumSampleCountForLongTail) {
            BS_INTERVAL_LOG(
                300, WARN,
                "[%s] stop detecting long tail, finished nodes size [%lu] is smaller than minimum sample count[%u]",
                nodeStatusManager->GetFirstRoleGroupName().c_str(), finishedNodes.size(),
                minimumSampleCountForLongTail);
            return;
        }
        int64_t avgWorkingTime =
            nodeStatusManager->CalculateAverageWorkingTime(finishedNodes, minimumSampleCountForLongTail);
        if (avgWorkingTime <= 0) {
            return;
        }
        int64_t longTailThreshold = std::max(int64_t(avgWorkingTime * _longTailNodeMultiplier), _slowTimeThreshold);
        BS_INTERVAL_LOG(300, INFO, "[%s] long tail threshold[%ld] us, finished node size[%lu], started node size[%lu]",
                        nodeStatusManager->GetFirstRoleGroupName().c_str(), longTailThreshold, finishedNodes.size(),
                        startedNodes.size());
        for (auto& nodeGroupInfo : GetRunningNodes(excludeKey, nodeStatusManager)) {
            const auto& nodeMap = nodeGroupInfo.second;
            if (nodeMap.empty()) {
                continue;
            }
            const auto& allRoleOfThisRoleGroup =
                (nodeStatusManager->GetAllNodeGroups().find(nodeGroupInfo.first))->second;
            NodeStatusPtr candidateNodeStatus = GetCandidateNodeStatus(allRoleOfThisRoleGroup, nodeStatusManager);
            if (!candidateNodeStatus) {
                continue;
            }
            const auto& candidateNode = candidateNodeStatus->GetWorkerNode<Nodes>();
            SelectLongTailNode(candidateNodeStatus, candidateNode, avgWorkingTime, slowNodes);
        }
    }

    void DetectSlowCpuSpeedNode(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, int64_t currentTime,
                                NodeStatusPtrVec* slowNodes) const
    {
        std::string excludeKey = "cpu_speed";
        std::vector<int64_t> allCpuSpeed;
        std::vector<std::string> excludeKeyWords = GetExcludeKeyWords(excludeKey);
        const auto& startedNodes = nodeStatusManager->GetStartedNodes(excludeKeyWords);
        nodeStatusManager->GetStartedNodesAttr(startedNodes, excludeKey, &allCpuSpeed);
        CpuSpeedDetectStrategy strategy(allCpuSpeed, _cpuSpeedFixedThreshold);

        for (auto& nodeGroupInfo : GetRunningNodes(excludeKey, nodeStatusManager)) {
            auto candidateNodeStatus = GetNodeStatus(nodeStatusManager, slowNodes, nodeGroupInfo.second);
            if (!candidateNodeStatus) {
                continue;
            }
            int64_t highestCpuSpeed = GetHighestCpuSpeedInNodeGroup(nodeGroupInfo.second);
            if (highestCpuSpeed == common::CpuSpeedEstimater::INVALID_CPU_SPEED) {
                continue;
            }
            CpuSpeedDetectStrategy::Node node(highestCpuSpeed, candidateNodeStatus->ip, candidateNodeStatus->roleName);
            bool isSlowNode = strategy.detect(node);
            if (isSlowNode) {
                slowNodes->emplace_back(candidateNodeStatus);
            }
        }
    }

    template <typename Node>
    void SelectLongTailNode(const NodeStatusPtr& nodeStatus, const Node& node, int64_t avgWorkingTime,
                            NodeStatusPtrVec* slowNodes) const
    {
        auto progressStatus = (node->getCurrentStatus()).progressstatus();
        if (progressStatus.starttimestamp() == -1 || progressStatus.reporttimestamp() == -1 ||
            nodeStatus->lastKillStartedTime == nodeStatus->nodeStartedTime) {
            // node just started
            return;
        }
        int64_t longTailThreshold = std::max(int64_t(avgWorkingTime * _longTailNodeMultiplier), _slowTimeThreshold);
        if (nodeStatus->workTimeInLatestSlot > longTailThreshold) {
            BS_LOG(WARN,
                   "trigger long tail. long tail node detected [%s], node workingTime in latest slot[%ld]us,"
                   "average workingTime[%ld]us, long tail threshold[%ld]us",
                   nodeStatus->roleName.c_str(), nodeStatus->workTimeInLatestSlot, avgWorkingTime, longTailThreshold);
            slowNodes->emplace_back(nodeStatus);
        }
        return;
    }

private:
    float _slowNodeSampleRatio;
    float _longTailNodeSampleRatio;
    float _longTailNodeMultiplier;
    uint64_t _networkSpeedThreshold;
    int64_t _cpuSpeedFixedThreshold;
    bool _enableNetworkSpeedStrategy;
    bool _enableDetectSlowByLocator;
    std::string _locatorCheckCondition;
    int64_t _locatorCheckGap;

    BS_LOG_DECLARE();
};

BS_LOG_SETUP_TEMPLATE(admin, NewSlowNodeDetectStrategy, Nodes);

template <>
inline taskcontroller::NodeStatusManager::NodeStatusPtr
NewSlowNodeDetectStrategy<proto::MergerNodes>::GetCandidateNodeStatus(
    const taskcontroller::NodeStatusManager::NodeMap& nodeMap,
    const taskcontroller::NodeStatusManagerPtr& nodeStatusManager) const
{
    int candidateNodeWorkTime = 1 << 30;
    taskcontroller::NodeStatusManager::NodeStatusPtr candidateNodeStatus;
    for (const auto& nodeStatusInfo : nodeMap) {
        const auto& nodeStatus = nodeStatusInfo.second;
        const auto& node = nodeStatus->GetWorkerNode<proto::MergerNodes>();
        if (node->isFinished()) {
            return taskcontroller::NodeStatusManager::NodeStatusPtr();
        }
        if (nodeStatusManager->IsFinishedGroup(nodeStatus->roleGroupName)) {
            return taskcontroller::NodeStatusManager::NodeStatusPtr();
        }
        int nodeWorkeTime = nodeStatus->workTimeInLatestSlot;
        if (nodeWorkeTime < candidateNodeWorkTime) {
            candidateNodeStatus = nodeStatus;
            candidateNodeWorkTime = nodeWorkeTime;
        }
    }
    return candidateNodeStatus;
}

template <>
inline taskcontroller::NodeStatusManager::NodeStatusPtr
NewSlowNodeDetectStrategy<proto::TaskNodes>::GetCandidateNodeStatus(
    const taskcontroller::NodeStatusManager::NodeMap& nodeMap,
    const taskcontroller::NodeStatusManagerPtr& nodeStatusManager) const
{
    int candidateNodeWorkTime = 1 << 30;
    taskcontroller::NodeStatusManager::NodeStatusPtr candidateNodeStatus;
    for (const auto& nodeStatusInfo : nodeMap) {
        const auto& nodeStatus = nodeStatusInfo.second;
        const auto& node = nodeStatus->GetWorkerNode<proto::TaskNodes>();
        if (node->isFinished()) {
            return taskcontroller::NodeStatusManager::NodeStatusPtr();
        }
        if (nodeStatusManager->IsFinishedGroup(nodeStatus->roleGroupName)) {
            return taskcontroller::NodeStatusManager::NodeStatusPtr();
        }
        int nodeWorkeTime = nodeStatus->workTimeInLatestSlot;
        if (nodeWorkeTime < candidateNodeWorkTime) {
            candidateNodeStatus = nodeStatus;
            candidateNodeWorkTime = nodeWorkeTime;
        }
    }
    return candidateNodeStatus;
}

template <typename Nodes>
NewSlowNodeDetectStrategy<Nodes>::NewSlowNodeDetectStrategy(const config::SlowNodeDetectConfig& config,
                                                            int64_t startTime)
    : SlowNodeDetectStrategy(config, startTime)
    , _slowNodeSampleRatio(config.slowNodeSampleRatio)
    , _longTailNodeSampleRatio(config.longTailNodeSampleRatio)
    , _longTailNodeMultiplier(config.longTailNodeMultiplier)
    , _networkSpeedThreshold(config.networkSpeedThreshold)
    , _cpuSpeedFixedThreshold(config.cpuSpeedFixedThreshold)
    , _enableNetworkSpeedStrategy(config.enableNetworkSpeedStrategy)
    , _enableDetectSlowByLocator(config.enableDetectSlowByLocator)
    , _locatorCheckCondition(config.locatorCheckCondition)
    , _locatorCheckGap(config.locatorCheckGap)
{
    _slowNodeSampleRatio = std::min(std::max(0.5f, _slowNodeSampleRatio), 1.0f);
    _longTailNodeSampleRatio = std::min(std::max(0.6f, _longTailNodeSampleRatio), 1.0f);
    _longTailNodeMultiplier = std::min(std::max(1.1f, _longTailNodeMultiplier), 3.0f);
    // cpu speed fixed threshold is just a temporary solution for small sample number,
    // should not be set too big, in case of massive nodes are detected when the cluster is overloaded
    _cpuSpeedFixedThreshold = std::min(CpuSpeedDetectStrategy::CPU_FIXED_THREADHOLD, _cpuSpeedFixedThreshold);
    BS_LOG(INFO,
           "NewSlowNodeDetectStrategy initial parameters: "
           "slowNodeSampleRatio[%.2f], longTailNodeSampleRatio[%.2f], longTailNodeMultiplier[%.2f], "
           "enableNetworkSpeedStrategy[%d], networkSpeedThreshold[%lu], notWorkTimeThreshold[%ld],"
           "cpuSpeedFixedThreshold[%ld]",
           _slowNodeSampleRatio, _longTailNodeSampleRatio, _longTailNodeMultiplier, _enableNetworkSpeedStrategy,
           _networkSpeedThreshold, config.notWorkTimeThreshold, _cpuSpeedFixedThreshold);
}

}} // namespace build_service::admin

#endif // ISEARCH_BS_NEWSLOWNODEDETECTSTRATEGY_H
