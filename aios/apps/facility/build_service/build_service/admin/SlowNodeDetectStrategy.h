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

#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "build_service/admin/SlowNodeMetricReporter.h"
#include "build_service/admin/taskcontroller/NodeStatusManager.h"
#include "build_service/common_define.h"
#include "build_service/config/SlowNodeDetectConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"
#include "kmonitor/client/core/MetricsTags.h"

namespace build_service { namespace admin {

class SlowNodeDetectStrategy
{
public:
    struct AbnormalNodes {
        taskcontroller::NodeStatusManager::NodeStatusPtrVec deadNodes;
        taskcontroller::NodeStatusManager::NodeStatusPtrVec restartNodes;
        taskcontroller::NodeStatusManager::NodeStatusPtrVec slowNodes;
        taskcontroller::NodeStatusManager::NodeStatusPtrVec reclaimNodes;
    };

    SlowNodeDetectStrategy(const config::SlowNodeDetectConfig& config, int64_t startTime);
    virtual ~SlowNodeDetectStrategy() {}
    SlowNodeDetectStrategy(const SlowNodeDetectStrategy&) = delete;
    SlowNodeDetectStrategy& operator=(const SlowNodeDetectStrategy&) = delete;

    template <typename Nodes>
    void Detect(const SlowNodeMetricReporterPtr& reporter,
                const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, int64_t currentTime,
                AbnormalNodes* abnormalNodes) const
    {
        DetectDeadNode(nodeStatusManager, currentTime, &(abnormalNodes->deadNodes));
        DetectRestartNode<Nodes>(nodeStatusManager, currentTime, &(abnormalNodes->restartNodes));
        DetectSlowNode(nodeStatusManager, currentTime, &(abnormalNodes->slowNodes));
        DetectReclaimNode<Nodes>(nodeStatusManager, currentTime, &(abnormalNodes->reclaimNodes));
        UniqueNodes(abnormalNodes);

        if (reporter != nullptr && reporter->EnableDetailMetric() && abnormalNodes != nullptr) {
            proto::RoleType roleType = proto::WorkerNodes2WorkerTypeTraits<Nodes>::WorkerRoleType;
            std::string roleTypeStr = proto::ProtoUtil::toRoleString(roleType);
            auto reportDetect = [&](const taskcontroller::NodeStatusManager::NodeStatusPtrVec& nodes,
                                    SlowNodeMetricReporter::DetectType type) {
                for (auto node : nodes) {
                    if (!node) {
                        continue;
                    }
                    kmonitor::MetricsTags tags;
                    tags.AddTag("roleType", roleTypeStr);
                    tags.AddTag("nodeIp", node->ip);
                    tags.AddTag("roleGroup", node->roleGroupName);
                    tags.AddTag("backupId", autil::StringUtil::toString(node->backupId));
                    reporter->IncreaseDetectQps(type, tags);
                }
            };
            reportDetect(abnormalNodes->deadNodes, SlowNodeMetricReporter::DT_DEAD);
            reportDetect(abnormalNodes->restartNodes, SlowNodeMetricReporter::DT_RESTART);
            reportDetect(abnormalNodes->slowNodes, SlowNodeMetricReporter::DT_SLOW);
            reportDetect(abnormalNodes->reclaimNodes, SlowNodeMetricReporter::DT_RECLAIM);
        }
    }

protected:
    int64_t _slowTimeThreshold;
    int64_t _notWorkTimeThreshold;
    int64_t _restartCountThreshold;
    int64_t _startTime;
    std::map<std::string, std::vector<std::string>> _excludeKeywords;

    std::vector<std::string> GetExcludeKeyWords(const std::string& strategy) const;

private:
    virtual void DetectSlowNode(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, int64_t currentTime,
                                taskcontroller::NodeStatusManager::NodeStatusPtrVec* slowNodes) const = 0;

    void DetectDeadNode(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, int64_t currentTime,
                        taskcontroller::NodeStatusManager::NodeStatusPtrVec* deadNodes) const
    {
        auto excludeKeyWords = GetExcludeKeyWords("dead");
        auto nodeStatuses = nodeStatusManager->GetAllNodes(excludeKeyWords);
        for (const auto& nodeStatus : nodeStatuses) {
            if (nodeStatus->finishTimeInLocal != 0) {
                continue;
            }
            if (nodeStatusManager->IsFinishedGroup(nodeStatus->roleGroupName)) {
                BS_INTERVAL_LOG(600, INFO, "skip finished role[%s] on dead node detect", nodeStatus->roleName.c_str());
                continue;
            }
            if (currentTime - nodeStatus->lastReportTime >= _slowTimeThreshold) {
                BS_LOG(WARN,
                       "trigger dead. select dead node [%s] , currentTime[%ld], lastReportTime[%ld], "
                       "slowTimeThreshold[%ld]",
                       nodeStatus->roleName.c_str(), currentTime, nodeStatus->lastReportTime, _slowTimeThreshold);
                deadNodes->emplace_back(nodeStatus);
                continue;
            }
            // if node not work for a long time, kill it

            if (nodeStatus->nodeStartedTime == -1 &&
                currentTime - nodeStatus->lastActiveTimeInLocal >= _notWorkTimeThreshold) {
                BS_LOG(WARN,
                       "trigger dead. select long time not started node [%s], "
                       "currentTime[%ld], lastActiveTimeInLocal[%ld], notWorkThreshold[%ld]",
                       nodeStatus->roleName.c_str(), currentTime, nodeStatus->lastActiveTimeInLocal,
                       _notWorkTimeThreshold);
                deadNodes->emplace_back(nodeStatus);
            }
        }
    }

    template <typename Nodes>
    void DetectReclaimNode(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, int64_t currentTime,
                           taskcontroller::NodeStatusManager::NodeStatusPtrVec* reclaimNodes) const
    {
        auto excludeKeyWords = GetExcludeKeyWords("reclaim");
        auto nodeStatuses = nodeStatusManager->GetAllNodes(excludeKeyWords);
        for (const auto& nodeStatus : nodeStatuses) {
            BS_LOG(DEBUG, "[test]Node[%s]", nodeStatus->roleName.c_str());
            BS_LOG(DEBUG, "[test]Node finishTimeInLocal[%ld]", nodeStatus->finishTimeInLocal);
            if (nodeStatus->finishTimeInLocal != 0) {
                continue;
            }
            if (nodeStatusManager->IsFinishedGroup(nodeStatus->roleGroupName)) {
                BS_INTERVAL_LOG(600, INFO, "skip finished role[%s] on reclaim node detect",
                                nodeStatus->roleName.c_str());
                continue;
            }
            const auto& workerNode = nodeStatus->GetWorkerNode<Nodes>();
            const auto& partitionInfo = workerNode->getPartitionInfo();
            std::vector<std::string> slotAddrs;
            bool isReclaim = false;
            BS_LOG(DEBUG, "[test]Node partitionInfo[%s]", partitionInfo.ShortDebugString().c_str());
            for (const auto& slotInfo : partitionInfo.slotinfos()) {
                if (slotInfo.reclaim()) {
                    isReclaim = true;
                    slotAddrs.push_back(slotInfo.id().slaveaddress());
                }
            }
            if (isReclaim) {
                BS_LOG(WARN, "trigger reclaim. select reclaim node[%s][%s], will release soon.",
                       nodeStatus->roleName.c_str(), autil::StringUtil::toString(slotAddrs, ";").c_str());
                reclaimNodes->emplace_back(nodeStatus);
            }
        }
    }

    template <typename Nodes>
    void DetectRestartNode(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, int64_t currentTime,
                           taskcontroller::NodeStatusManager::NodeStatusPtrVec* restartNodes) const
    {
        auto excludeKeyWords = GetExcludeKeyWords("restart");
        auto nodeStatuses = nodeStatusManager->GetAllNodes(excludeKeyWords);
        for (const auto& nodeStatus : nodeStatuses) {
            if (nodeStatus->finishTimeInLocal != 0) {
                continue;
            }
            if (nodeStatusManager->IsFinishedGroup(nodeStatus->roleGroupName)) {
                BS_INTERVAL_LOG(600, INFO, "skip finished role[%s] on restart node detect",
                                nodeStatus->roleName.c_str());
                continue;
            }
            if (nodeStatus->startCount > _restartCountThreshold) {
                const auto& workerNode = nodeStatus->GetWorkerNode<Nodes>();
                const auto& partitionInfo = workerNode->getPartitionInfo();
                std::vector<std::string> slotAddrs;
                slotAddrs.reserve(partitionInfo.slotinfos().size());
                for (const auto& slotInfo : partitionInfo.slotinfos()) {
                    slotAddrs.push_back(slotInfo.id().slaveaddress());
                }
                BS_LOG(WARN, "trigger restart. select restart node[%s][%s], restart count[%ld], exceed limit[%ld]",
                       nodeStatus->roleName.c_str(), autil::StringUtil::toString(slotAddrs, ";").c_str(),
                       nodeStatus->startCount, _restartCountThreshold);
                restartNodes->emplace_back(nodeStatus);
            }
        }
    }
    void UniqueNodes(AbnormalNodes* abnormalNodes) const;

    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SlowNodeDetectStrategy);

}} // namespace build_service::admin
