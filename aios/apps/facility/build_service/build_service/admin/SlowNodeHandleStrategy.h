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
#ifndef ISEARCH_BS_SLOWNODEHANDLESTRATEGY_H
#define ISEARCH_BS_SLOWNODEHANDLESTRATEGY_H

#include "build_service/admin/SlowNodeDetectStrategy.h"
#include "build_service/admin/SlowNodeMetricReporter.h"
#include "build_service/common_define.h"
#include "build_service/proto/WorkerNodeCreator.h"
#include "build_service/util/Log.h"
#include "indexlib/index_base/branch_fs.h"

namespace build_service { namespace admin {

#define MIN_HANDLE_INTERVAL uint64_t(20ull * 60 * 1000000)
#define MAX_HANDLE_INTERVAL uint64_t(80ull * 60 * 1000000)
#define LAST_HANDLE_TIME "lastHandleTime"
#define HANDLE_INTERVAL "handleInterval"

class SlowNodeHandleStrategy
{
public:
    struct Metric {
        int64_t slowNodeKillTimes = 0;
        int64_t deadNodeKillTimes = 0;
        int64_t restartNodeKillTimes = 0;

        int64_t slowNodeBackupCreateTimes = 0;
        int64_t effectiveBackupNodeCount = 0;

        bool operator==(const Metric& other) const
        {
            return slowNodeKillTimes == other.slowNodeKillTimes && deadNodeKillTimes == other.deadNodeKillTimes &&
                   restartNodeKillTimes == other.restartNodeKillTimes &&
                   slowNodeBackupCreateTimes == other.slowNodeBackupCreateTimes &&
                   effectiveBackupNodeCount == other.effectiveBackupNodeCount;
        }
        bool operator!=(const Metric& other) const { return !(*this == other); }

        double effectiveBackupNodeRate() const
        {
            if (effectiveBackupNodeCount == 0 || slowNodeBackupCreateTimes == 0) {
                return 0;
            }
            return double(effectiveBackupNodeCount) / slowNodeBackupCreateTimes;
        }
    };

    SlowNodeHandleStrategy(const config::SlowNodeDetectConfig& config, int64_t startTime)
        : _killTimesLimit(config.maxKillTimesLimit)
        , _killTimesResetWindow(config.killTimesResetWindow * 1000000)
        , _backupNodeCountLimit(
              std::min((int64_t)indexlib::index_base::BranchFS::MAX_BRANCH_ID, config.backupNodeCountLimit))
        , _slowNodeHandleStrategy(config.slowNodeHandleStrategy)
        , _enableHandleInterval(config.enableHandleStrategyInterval)
        , _enableHighQualitySlot(config.enableHighQualitySlot)
    {
        BS_LOG(INFO, "slowNodeHandleStrategy use %s strategy, enableHighQualitySlot[%d]",
               _slowNodeHandleStrategy.c_str(), _enableHighQualitySlot);
    }
    virtual ~SlowNodeHandleStrategy() {}
    SlowNodeHandleStrategy(const SlowNodeHandleStrategy&) = delete;
    SlowNodeHandleStrategy& operator=(const SlowNodeHandleStrategy&) = delete;

    template <typename Nodes>
    void Handle(const SlowNodeMetricReporterPtr& reporter,
                const taskcontroller::NodeStatusManagerPtr& nodeStatusManager,
                SlowNodeDetectStrategy::AbnormalNodes* abnormalNodes, Nodes* nodes, Metric* metric);

    template <typename Nodes>
    void simpleHandle(const SlowNodeMetricReporterPtr& reporter,
                      const taskcontroller::NodeStatusManagerPtr& nodeStatusManager,
                      SlowNodeDetectStrategy::AbnormalNodes* abnormalNodes, Nodes* nodes, Metric* metric);

private:
    template <typename Nodes>
    bool CreateBackupNode(const SlowNodeMetricReporterPtr& reporter,
                          taskcontroller::NodeStatusManager::NodeStatusPtr& status, Nodes* nodes,
                          const taskcontroller::NodeStatusManagerPtr& nodeStatusManager);

    template <typename Nodes>
    bool CreateBackupNodeForReclaim(const SlowNodeMetricReporterPtr& reporter,
                                    taskcontroller::NodeStatusManager::NodeStatusPtr& status, Nodes* nodes,
                                    const taskcontroller::NodeStatusManagerPtr& nodeStatusManager);

    template <typename Nodes>
    bool MigrateNode(const SlowNodeMetricReporterPtr& reporter,
                     taskcontroller::NodeStatusManager::NodeStatusPtr& status,
                     const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, int64_t& metric,
                     bool needCheckKillTimesLimit = true);

    void UpdateHandleInfo(taskcontroller::NodeStatusManager::NodeStatusPtr& status,
                          const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, uint64_t currentHandleInterval,
                          uint64_t lastHandleTime, int64_t currentTime)
    {
        nodeStatusManager->SetRoleGroupCounter(status->roleGroupName, LAST_HANDLE_TIME, currentTime);
        if (currentHandleInterval >= MAX_HANDLE_INTERVAL) {
            nodeStatusManager->SetRoleGroupCounter(status->roleGroupName, HANDLE_INTERVAL, MIN_HANDLE_INTERVAL);
            BS_LOG(INFO,
                   "Node [%s] handle interval[%lu] >= MAX_HANDLE_INTERVAL[%lu], reset to MIN_HANDLE_INTERVAL[%lu],"
                   "last handle time [%ld]",
                   status->roleName.c_str(), currentHandleInterval, MAX_HANDLE_INTERVAL, MIN_HANDLE_INTERVAL,
                   lastHandleTime);
        } else {
            // handle interval penalty
            nodeStatusManager->SetRoleGroupCounter(status->roleGroupName, HANDLE_INTERVAL,
                                                   std::min(currentHandleInterval * 2, MAX_HANDLE_INTERVAL));
        }
    }

private:
    int64_t _killTimesLimit;
    int64_t _killTimesResetWindow;
    int64_t _backupNodeCountLimit;
    std::string _slowNodeHandleStrategy;
    bool _enableHandleInterval;
    bool _enableHighQualitySlot;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SlowNodeHandleStrategy);

template <typename Nodes>
void SlowNodeHandleStrategy::Handle(const SlowNodeMetricReporterPtr& reporter,
                                    const taskcontroller::NodeStatusManagerPtr& nodeStatusManager,
                                    SlowNodeDetectStrategy::AbnormalNodes* abnormalNodes, Nodes* nodes, Metric* metric)
{
    proto::RoleType roleType = proto::WorkerNodes2WorkerTypeTraits<Nodes>::WorkerRoleType;
    for (auto& nodeStatus : abnormalNodes->deadNodes) {
        MigrateNode<Nodes>(reporter, nodeStatus, nodeStatusManager, metric->deadNodeKillTimes);
    }
    for (auto& nodeStatus : abnormalNodes->restartNodes) {
        MigrateNode<Nodes>(reporter, nodeStatus, nodeStatusManager, metric->restartNodeKillTimes,
                           /*needCheckKillTimesLimit=*/false);
    }
    if (_slowNodeHandleStrategy == BACKUP_NODE_HANDLE_STRATEGY &&
        (roleType == proto::ROLE_BUILDER || roleType == proto::ROLE_MERGER || roleType == proto::ROLE_TASK)) {
        for (auto& nodeStatus : abnormalNodes->slowNodes) {
            if (!CreateBackupNode<Nodes>(reporter, nodeStatus, nodes, nodeStatusManager)) {
                BS_INTERVAL_LOG(100, WARN, "Create backupNode[slowNode] for rolegroup [%s] failed",
                                nodeStatus->roleGroupName.c_str());
            } else {
                ++(metric->slowNodeBackupCreateTimes);
            }
        }
    } else {
        for (auto& nodeStatus : abnormalNodes->slowNodes) {
            MigrateNode<Nodes>(reporter, nodeStatus, nodeStatusManager, metric->slowNodeKillTimes);
        }
    }
}

template <typename Nodes>
void SlowNodeHandleStrategy::simpleHandle(const SlowNodeMetricReporterPtr& reporter,
                                          const taskcontroller::NodeStatusManagerPtr& nodeStatusManager,
                                          SlowNodeDetectStrategy::AbnormalNodes* abnormalNodes, Nodes* nodes,
                                          Metric* metric)
{
    for (auto& nodeStatus : abnormalNodes->deadNodes) {
        MigrateNode<Nodes>(reporter, nodeStatus, nodeStatusManager, metric->deadNodeKillTimes);
    }
    for (auto& nodeStatus : abnormalNodes->restartNodes) {
        MigrateNode<Nodes>(reporter, nodeStatus, nodeStatusManager, metric->restartNodeKillTimes,
                           /*needCheckKillTimesLimit=*/false);
    }
    for (auto& nodeStatus : abnormalNodes->slowNodes) {
        MigrateNode<Nodes>(reporter, nodeStatus, nodeStatusManager, metric->slowNodeKillTimes);
    }
}

template <typename Nodes>
bool SlowNodeHandleStrategy::CreateBackupNode(const SlowNodeMetricReporterPtr& reporter,
                                              taskcontroller::NodeStatusManager::NodeStatusPtr& status, Nodes* nodes,
                                              const taskcontroller::NodeStatusManagerPtr& nodeStatusManager)
{
    auto currentTime = autil::TimeUtility::currentTimeInMicroSeconds();
    int32_t backupId = nodeStatusManager->NextBackupId(status->roleGroupName);
    int32_t roleCount = nodeStatusManager->GetRoleCountInGroup(status->roleGroupName);
    if (roleCount > _backupNodeCountLimit) {
        BS_INTERVAL_LOG(100, WARN, "RoleGroup [%s] reach backupNode count limit, will not create new backupNode",
                        status->roleGroupName.c_str());
        return false;
    }
    uint64_t lastHandleTime = nodeStatusManager->GetRoleGroupCounter(status->roleGroupName, LAST_HANDLE_TIME);
    uint64_t handleInterval =
        std::max(nodeStatusManager->GetRoleGroupCounter(status->roleGroupName, HANDLE_INTERVAL), MIN_HANDLE_INTERVAL);
    if ((lastHandleTime == 0 || currentTime - lastHandleTime >= handleInterval) || !_enableHandleInterval) {
        const auto& workerNode = status->GetWorkerNode<Nodes>();
        auto backupNode =
            proto::WorkerNodeCreator<proto::WorkerNodes2WorkerTypeTraits<Nodes>::WorkerRoleType>::createBackupNode(
                workerNode, backupId);
        if (_enableHighQualitySlot && !(status->IsIncProcessor())) {
            backupNode->setHighQuality(true);
            BS_LOG(INFO, "set highQuality Node [%s], backupId[%d]", status->roleName.c_str(), backupId);
        }
        nodes->emplace_back(backupNode);
        status->slowNetworkDetectCount = 0;

        if (reporter && reporter->EnableDetailMetric()) {
            kmonitor::MetricsTags tags;
            proto::RoleType roleType = proto::WorkerNodes2WorkerTypeTraits<Nodes>::WorkerRoleType;
            tags.AddTag("roleType", proto::ProtoUtil::toRoleString(roleType));
            tags.AddTag("oldIp", status->ip);
            tags.AddTag("roleGroup", status->roleGroupName);
            tags.AddTag("backupId", autil::StringUtil::toString(backupId));
            tags.AddTag("setHighQuality", backupNode->isHighQuality() ? "true" : "false");
            reporter->IncreaseHandleQps(SlowNodeMetricReporter::HT_BACKUP, tags);
        }
        UpdateHandleInfo(status, nodeStatusManager, handleInterval, lastHandleTime, currentTime);
        BS_LOG(INFO, "RoleGroup [%s] create a backup node", status->roleGroupName.c_str());
        return true;
    }
    BS_INTERVAL_LOG(100, WARN,
                    "RoleGroup [%s] will not create a backup node, "
                    "gap between currentTime [%ld] and lastHandleTime [%ld] is less than handleInterval[%ld]",
                    status->roleGroupName.c_str(), currentTime, lastHandleTime, handleInterval);
    return false;
}

template <typename Nodes>
bool SlowNodeHandleStrategy::CreateBackupNodeForReclaim(const SlowNodeMetricReporterPtr& reporter,
                                                        taskcontroller::NodeStatusManager::NodeStatusPtr& status,
                                                        Nodes* nodes,
                                                        const taskcontroller::NodeStatusManagerPtr& nodeStatusManager)
{
    // quick reclaim, not consider and upodate handle interval,
    // note : not affect node status manager and other handler's behaviour
    int32_t backupId = nodeStatusManager->NextBackupId(status->roleGroupName);
    const auto& workerNode = status->GetWorkerNode<Nodes>();
    if (_enableHighQualitySlot && !(status->IsIncProcessor())) {
        workerNode->setHighQuality(true);
        BS_LOG(INFO, "set highQuality Node [%s]", status->roleName.c_str());
    }
    int32_t oldBackupId = workerNode->getPartitionId().has_backupid() ? workerNode->getPartitionId().backupid() : 0;
    if (oldBackupId + 1 == backupId) {
        auto backupNode =
            proto::WorkerNodeCreator<proto::WorkerNodes2WorkerTypeTraits<Nodes>::WorkerRoleType>::createBackupNode(
                workerNode, backupId);
        if (_enableHighQualitySlot && !(status->IsIncProcessor())) {
            backupNode->setHighQuality(true);
            BS_LOG(INFO, "set highQuality Node [%s], backupId[%d]", status->roleName.c_str(), backupId);
        }

        if (reporter && reporter->EnableDetailMetric()) {
            kmonitor::MetricsTags tags;
            proto::RoleType roleType = proto::WorkerNodes2WorkerTypeTraits<Nodes>::WorkerRoleType;
            tags.AddTag("roleType", proto::ProtoUtil::toRoleString(roleType));
            tags.AddTag("oldIp", status->ip);
            tags.AddTag("roleGroup", status->roleGroupName);
            tags.AddTag("backupId", autil::StringUtil::toString(backupId));
            tags.AddTag("setHighQuality", backupNode->isHighQuality() ? "true" : "false");
            reporter->IncreaseHandleQps(SlowNodeMetricReporter::HT_BACKUP, tags);
        }
        nodes->emplace_back(backupNode);
        status->slowNetworkDetectCount = 0;
        BS_LOG(INFO, "RoleGroup [%s] create a backup node for reclaiming", status->roleGroupName.c_str());
        return true;
    } else {
        BS_INTERVAL_LOG(100, INFO,
                        "RoleGroup [%s] reclaim node backupid[%d], "
                        "but latest backupid[%d], do not create new backup node!",
                        status->roleGroupName.c_str(), oldBackupId, backupId - 1);
    }
    return true;
}

template <typename Nodes>
bool SlowNodeHandleStrategy::MigrateNode(const SlowNodeMetricReporterPtr& reporter,
                                         taskcontroller::NodeStatusManager::NodeStatusPtr& status,
                                         const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, int64_t& metric,
                                         bool needCheckKillTimesLimit)
{
    auto currentTime = autil::TimeUtility::currentTimeInMicroSeconds();
    status->lastReportTime = std::max(currentTime, status->lastReportTime); // reset report time
    status->lastActiveTimeInLocal = currentTime;
    const auto& workerNode = status->GetWorkerNode<Nodes>();

    if (status->exceedReleaseLimitTime != -1 && currentTime - status->exceedReleaseLimitTime > _killTimesResetWindow) {
        BS_LOG(INFO,
               "Node [%s] need reset killTimes [%ld] to 0, "
               "last exceedReleaseLimitTime [%ld]",
               status->roleName.c_str(), status->killTimes, status->exceedReleaseLimitTime);
        status->killTimes = 0;
        status->exceedReleaseLimitTime = -1;
        workerNode->resetExceedReleaseLimit();
    }
    int64_t killTimes = 0;
    if (needCheckKillTimesLimit) {
        killTimes = status->killTimes;
    }
    if (killTimes + 1 <= _killTimesLimit || !needCheckKillTimesLimit) {
        uint64_t lastHandleTime = nodeStatusManager->GetRoleGroupCounter(status->roleGroupName, LAST_HANDLE_TIME);
        uint64_t handleInterval = std::max(
            nodeStatusManager->GetRoleGroupCounter(status->roleGroupName, HANDLE_INTERVAL), MIN_HANDLE_INTERVAL);
        if ((lastHandleTime == 0 || currentTime - lastHandleTime >= handleInterval) || !_enableHandleInterval) {
            std::string nodeStatusStr = ToJsonString(status, true);
            const auto& partitionInfo = workerNode->getPartitionInfo();
            std::vector<std::string> slotAddrs;
            slotAddrs.reserve(partitionInfo.slotinfos().size());
            for (const auto& slotInfo : partitionInfo.slotinfos()) {
                slotAddrs.push_back(slotInfo.id().slaveaddress());
            }
            BS_LOG(INFO, "migrate node trigger. Node [%s] need kill, addrs [%s]", nodeStatusStr.c_str(),
                   autil::StringUtil::toString(slotAddrs, ";").c_str());
            if (_enableHighQualitySlot && !(status->IsIncProcessor())) {
                workerNode->setHighQuality(true);
                BS_LOG(INFO, "set highQuality Node [%s]", status->roleName.c_str());
            }

            if (reporter && reporter->EnableDetailMetric()) {
                kmonitor::MetricsTags tags;
                proto::RoleType roleType = proto::WorkerNodes2WorkerTypeTraits<Nodes>::WorkerRoleType;
                tags.AddTag("roleType", proto::ProtoUtil::toRoleString(roleType));
                tags.AddTag("oldIp", status->ip);
                tags.AddTag("roleGroup", status->roleGroupName);
                tags.AddTag("backupId", autil::StringUtil::toString(status->backupId));
                tags.AddTag("setHighQuality", workerNode->isHighQuality() ? "true" : "false");
                reporter->IncreaseHandleQps(SlowNodeMetricReporter::HT_MIGRATE, tags);
            }

            workerNode->changeSlots();
            status->lastKillStartedTime = status->nodeStartedTime;
            status->startCount = 0;
            status->slowNetworkDetectCount = 0;
            status->killTimes++;
            if (status->killTimes == _killTimesLimit) {
                BS_LOG(INFO, "Role [%s] reach kill limit [%ld], disable for release", status->roleName.c_str(),
                       _killTimesLimit);
                workerNode->setExceedReleaseLimit();
                if (status->exceedReleaseLimitTime == -1) {
                    status->exceedReleaseLimitTime = currentTime;
                }
            }
            metric++;
            UpdateHandleInfo(status, nodeStatusManager, handleInterval, lastHandleTime, currentTime);
            return true;
        }
        BS_LOG(WARN,
               "migrate node failed. Role [%s] will not migrate, "
               "gap between currentTime [%ld] and lastHandleTime [%lu] is less than handleInterval[%lu]",
               status->roleName.c_str(), currentTime, lastHandleTime, handleInterval);
        return false;
    }
    BS_LOG(WARN, "migrate node failed. Node [%s] killTimes[%ld] exceeds limit[%ld]", status->roleName.c_str(),
           killTimes, _killTimesLimit);
    return false;
}

}} // namespace build_service::admin

#endif // ISEARCH_BS_SLOWNODEHANDLESTRATEGY_H
