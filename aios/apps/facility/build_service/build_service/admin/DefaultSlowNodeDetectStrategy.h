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
#include <limits>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "build_service/admin/SlowNodeDetectStrategy.h"
#include "build_service/admin/taskcontroller/NodeStatusManager.h"
#include "build_service/config/SlowNodeDetectConfig.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

template <typename Nodes>
class DefaultSlowNodeDetectStrategy : public SlowNodeDetectStrategy
{
public:
    using NodeStatusPtr = taskcontroller::NodeStatusManager::NodeStatusPtr;
    using NodeStatusMap = std::map<std::string, NodeStatusPtr>;
    using NodeStatusPtrVec = taskcontroller::NodeStatusManager::NodeStatusPtrVec;

    DefaultSlowNodeDetectStrategy(const config::SlowNodeDetectConfig& config, int64_t startTime);
    ~DefaultSlowNodeDetectStrategy() {}
    DefaultSlowNodeDetectStrategy(const DefaultSlowNodeDetectStrategy&) = delete;
    DefaultSlowNodeDetectStrategy& operator=(const DefaultSlowNodeDetectStrategy&) = delete;

    inline NodeStatusPtr GetCandidateNodeStatus(const NodeStatusMap& nodeMap,
                                                const taskcontroller::NodeStatusManagerPtr& nodeStatusManager,
                                                float qpsThreshold, bool useQpsThreshold) const
    {
        int candidateNodeProgress = -1;
        NodeStatusPtr candidateNodeStatus;
        for (const auto& nodeStatusInfo : nodeMap) {
            const auto& nodeStatus = nodeStatusInfo.second;
            const auto& node = nodeStatus->GetWorkerNode<Nodes>();
            if (((nodeStatus->processQps >= qpsThreshold) && useQpsThreshold) || node->isFinished()) {
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

    void DetectLongTailNode(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, int64_t currentTime,
                            bool useQpsThreshold, NodeStatusPtrVec* slowNodes) const
    {
        const auto& startedNodes = nodeStatusManager->GetStartedNodes();
        if (startedNodes.size() == 1) {
            return;
        }
        uint32_t minimumSampleCount =
            std::max(2u, (uint32_t)(nodeStatusManager->GetAllNodeGroups().size() * _slowNodeSampleRatio));
        if (startedNodes.size() < minimumSampleCount) {
            BS_INTERVAL_LOG(
                300, WARN,
                "stop detecting long tail, started nodes size [%lu] is smaller than minimum sample count[%u]",
                startedNodes.size(), minimumSampleCount);
            return;
        }
        float qpsThreshold = nodeStatusManager->CalculateAverageQps(startedNodes) * _slowQpsJudgeRatio;
        const auto& finishedNodes = nodeStatusManager->GetFinishedNodes();
        uint32_t minimumSampleCountForLongTail =
            std::max(1u, (uint32_t)(startedNodes.size() * _longTailNodeSampleRatio));

        if (finishedNodes.size() < minimumSampleCountForLongTail) {
            BS_INTERVAL_LOG(
                300, WARN,
                "stop detecting long tail, finished nodes size [%lu] is smaller than minimum sample count[%u]",
                finishedNodes.size(), minimumSampleCountForLongTail);
            return;
        }
        int64_t avgWorkingTime = nodeStatusManager->CalculateAverageWorkingTime(finishedNodes);
        if (avgWorkingTime <= 0) {
            return;
        }
        const auto& runningNodes = nodeStatusManager->GetRunningNodes();
        for (auto& nodeGroupInfo : runningNodes) {
            // select the fastest node to stand for this group, check whether this group is too slow compared to others
            // (1) for migrate strategy, group has only one node
            // (2) for backup strategy, it's no matter select which node in group
            // so it's safe to select fastest node in group as slow node
            const auto& nodeMap = nodeGroupInfo.second;
            if (nodeMap.empty()) {
                continue;
            }
            const auto& allRoleOfThisRoleGroup =
                (nodeStatusManager->GetAllNodeGroups().find(nodeGroupInfo.first))->second;
            NodeStatusPtr candidateNodeStatus =
                GetCandidateNodeStatus(allRoleOfThisRoleGroup, nodeStatusManager, qpsThreshold, useQpsThreshold);
            if (!candidateNodeStatus) {
                continue;
            }
            const auto& candidateNode = candidateNodeStatus->GetWorkerNode<Nodes>();
            SelectLongTailNode(candidateNodeStatus, candidateNode, avgWorkingTime, slowNodes);
        }
    }

    void DetectSlowQpsNode(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, int64_t currentTime,
                           bool useQpsThreshold, NodeStatusPtrVec* slowNodes) const
    {
        const auto& startedNodes = nodeStatusManager->GetStartedNodes();
        if (startedNodes.size() == 1) {
            return;
        }
        uint32_t minimumSampleCount =
            std::max(2u, (uint32_t)(nodeStatusManager->GetAllNodeGroups().size() * _slowNodeSampleRatio));

        if (startedNodes.size() < minimumSampleCount) {
            BS_INTERVAL_LOG(300, WARN, "started nodes size [%lu] is smaller than minimum sample count[%u]",
                            startedNodes.size(), minimumSampleCount);
            return;
        }
        float qpsThreshold = nodeStatusManager->CalculateAverageQps(startedNodes) * _slowQpsJudgeRatio;
        int64_t avgProgress = nodeStatusManager->CalculateAverageProgress<Nodes>(startedNodes);
        float minQps = std::numeric_limits<float>::max();

        NodeStatusPtr slowNode;
        const auto& runningNodes = nodeStatusManager->GetRunningNodes();
        for (auto& nodeGroupInfo : runningNodes) {
            const auto& nodeMap = nodeGroupInfo.second;
            if (nodeMap.empty()) {
                continue;
            }
            NodeStatusPtr candidateNodeStatus =
                GetCandidateNodeStatus(nodeMap, nodeStatusManager, qpsThreshold, useQpsThreshold);
            if (!candidateNodeStatus) {
                continue;
            }
            const auto& candidateNode = candidateNodeStatus->GetWorkerNode<Nodes>();
            int candidateNodeProgress = nodeStatusManager->GetNodeProgress(candidateNode);

            if (candidateNodeProgress < avgProgress) {
                SelectSlowQpsNode(candidateNodeStatus, avgProgress, candidateNodeProgress, &minQps, slowNode);
            }
        }
        if (slowNode) {
            // for every role group, just select one slow node to handle at one time
            for (const auto& node : *slowNodes) {
                if (node->roleGroupName == slowNode->roleGroupName) {
                    return;
                }
            }
            slowNodes->emplace_back(slowNode);
        }
    }

    inline void DetectSlowNode(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, int64_t currentTime,
                               NodeStatusPtrVec* slowNodes) const override
    {
        bool useQpsThreshold = true;
        DetectLongTailNode(nodeStatusManager, currentTime, useQpsThreshold, slowNodes);
        DetectSlowQpsNode(nodeStatusManager, currentTime, useQpsThreshold, slowNodes);
    }

    void SelectSlowQpsNode(const NodeStatusPtr& nodeStatus, int64_t avgProgress, int64_t nodeProgress, float* minQps,
                           NodeStatusPtr& slowNode) const
    {
        float currentQps = nodeStatus->processQps;
        int64_t chaseTime = (avgProgress - nodeProgress) / currentQps * 1000 * 1000;
        if (currentQps <= 0 || chaseTime > _slowTimeThreshold) {
            if (nodeStatus->lastKillStartedTime == nodeStatus->nodeStartedTime) {
                return;
            }
            if (currentQps < *minQps) {
                *minQps = currentQps;
                slowNode = nodeStatus;
                return;
            }
        }
        return;
    }

    template <typename Node>
    void SelectLongTailNode(const NodeStatusPtr& nodeStatus, const Node& node, int64_t avgWorkingTime,
                            NodeStatusPtrVec* slowNodes) const
    {
        auto progressStatus = (node->getCurrentStatus()).progressstatus();
        if (progressStatus.starttimestamp() == -1 || progressStatus.reporttimestamp() == -1 ||
            nodeStatus->lastKillStartedTime == nodeStatus->nodeStartedTime) {
            BS_LOG(WARN,
                   "long tail node skipped [%s], node workingTime in latest slot[%ld]us,"
                   "average workingTime[%ld] us, startTs[%ld], reportTs[%ld]"
                   "lastKillStartedTime[%ld], nodeStartedTime[%ld]",
                   nodeStatus->roleName.c_str(), nodeStatus->workTimeInLatestSlot, avgWorkingTime,
                   progressStatus.starttimestamp(), progressStatus.reporttimestamp(), nodeStatus->lastKillStartedTime,
                   nodeStatus->nodeStartedTime);
            return;
        }
        int64_t longTailThreshold = std::max(int64_t(avgWorkingTime * 2), _slowTimeThreshold);
        if (nodeStatus->workTimeInLatestSlot > longTailThreshold) {
            BS_LOG(WARN,
                   "long tail node detected [%s], node workingTime in latest slot[%ld]us,"
                   "average workingTime[%ld]us, long tail threshold[%ld]us",
                   nodeStatus->roleName.c_str(), nodeStatus->workTimeInLatestSlot, avgWorkingTime, longTailThreshold);
            slowNodes->emplace_back(nodeStatus);
        } else {
            BS_LOG(WARN,
                   "long tail node skipped [%s], node workingTime in latest slot[%ld]us,"
                   "average workingTime[%ld]us, long tail threshold[%ld]us",
                   nodeStatus->roleName.c_str(), nodeStatus->workTimeInLatestSlot, avgWorkingTime, longTailThreshold);
        }
        return;
    }

private:
    float _slowNodeSampleRatio;
    float _slowQpsJudgeRatio;
    float _longTailNodeSampleRatio;

    BS_LOG_DECLARE();
};

BS_LOG_SETUP_TEMPLATE(admin, DefaultSlowNodeDetectStrategy, Nodes);

template <>
inline void DefaultSlowNodeDetectStrategy<proto::MergerNodes>::DetectSlowNode(
    const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, int64_t currentTime,
    taskcontroller::NodeStatusManager::NodeStatusPtrVec* slowNodes) const
{
    bool useQpsThreshold = false;
    DetectLongTailNode(nodeStatusManager, currentTime, useQpsThreshold, slowNodes);
}

template <>
inline taskcontroller::NodeStatusManager::NodeStatusPtr
DefaultSlowNodeDetectStrategy<proto::MergerNodes>::GetCandidateNodeStatus(
    const std::map<std::string, taskcontroller::NodeStatusManager::NodeStatusPtr>& nodeMap,
    const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, float qpsThreshold, bool useQpsThreshold) const
{
    int candidateNodeWorkTime = 1 << 30;
    taskcontroller::NodeStatusManager::NodeStatusPtr candidateNodeStatus;
    for (const auto& nodeStatusInfo : nodeMap) {
        const auto& nodeStatus = nodeStatusInfo.second;
        const auto& node = nodeStatus->GetWorkerNode<proto::MergerNodes>();
        if (node->isFinished()) {
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
DefaultSlowNodeDetectStrategy<Nodes>::DefaultSlowNodeDetectStrategy(const config::SlowNodeDetectConfig& config,
                                                                    int64_t startTime)
    : SlowNodeDetectStrategy(config, startTime)
    , _slowNodeSampleRatio(config.slowNodeSampleRatio)
    , _slowQpsJudgeRatio(config.slowQpsJudgeRatio)
    , _longTailNodeSampleRatio(config.longTailNodeSampleRatio)
{
    _slowNodeSampleRatio = std::min(std::max(0.5f, _slowNodeSampleRatio), 1.0f);
    _slowQpsJudgeRatio = std::min(std::max(0.1f, _slowQpsJudgeRatio), 1.0f);
    _longTailNodeSampleRatio = std::min(std::max(0.6f, _longTailNodeSampleRatio), 1.0f);
    BS_LOG(INFO,
           "DefaultSlowNodeDetectStrategy initial parameters 0: slowNodeSampleRatio[%.2f], slowQpsJudgeRatio[%.2f], "
           "longTailNodeSampleRatio[%.2f].",
           _slowNodeSampleRatio, _slowQpsJudgeRatio, _longTailNodeSampleRatio);
}

}} // namespace build_service::admin
