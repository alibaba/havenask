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

#include <cstdint>
#include <google/protobuf/service.h>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/admin/AgentRoleInfo.h"
#include "build_service/admin/WorkerTable.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "hippo/DriverCommon.h"
#include "hippo/LeaderSerializer.h"
#include "hippo/MasterDriver.h"
#include "kmonitor_adapter/Metric.h"
#include "kmonitor_adapter/Monitor.h"
#include "master_framework/AppPlan.h"
#include "master_framework/RolePlan.h"
#include "master_framework/SimpleMasterScheduler.h"
#include "master_framework/common.h"
#include "worker_framework/LeaderChecker.h"
#include "worker_framework/ZkState.h"

namespace build_service { namespace admin {

class AgentSimpleMasterScheduler : public master_framework::simple_master::SimpleMasterScheduler
{
public:
    AgentSimpleMasterScheduler(const std::string& zkRoot, cm_basic::ZkWrapper* zkWrapper,
                               const std::shared_ptr<master_framework::simple_master::SimpleMasterScheduler>& scheduler,
                               kmonitor_adapter::Monitor* monitor = nullptr);

    ~AgentSimpleMasterScheduler();

    AgentSimpleMasterScheduler(const AgentSimpleMasterScheduler&) = delete;
    AgentSimpleMasterScheduler& operator=(const AgentSimpleMasterScheduler&) = delete;
    AgentSimpleMasterScheduler(AgentSimpleMasterScheduler&&) = delete;
    AgentSimpleMasterScheduler& operator=(AgentSimpleMasterScheduler&&) = delete;

public:
    bool init(const std::string& hippoZkRoot, worker_framework::LeaderChecker* leaderChecker,
              const std::string& applicationId) override;

    void setAppPlan(const master_framework::simple_master::AppPlan& appPlan) override;

    void getAllRoleSlots(std::map<std::string, master_framework::master_base::SlotInfos>& roleSlots) override;

    void releaseSlots(const std::vector<hippo::SlotId>& slots, const hippo::ReleasePreference& pref) override;

    std::vector<hippo::SlotInfo> getRoleSlots(const std::string& roleName) override;

    bool start() override { return _scheduler->start(); }

    bool stop() override { return _scheduler->stop(); }

    bool isLeader() override { return _scheduler->isLeader(); }

    bool isConnected() override { return _scheduler->isConnected(); }

    void reAllocRoleSlots(const std::string& roleName) override { _scheduler->reAllocRoleSlots(roleName); }

    hippo::LeaderSerializer* createLeaderSerializer(const std::string& zookeeperRoot, const std::string& fileName,
                                                    const std::string& backupRoot = "") override
    {
        return _scheduler->createLeaderSerializer(zookeeperRoot, fileName, backupRoot);
    }

    hippo::MasterDriver* getMasterDriver() override { return _scheduler->getMasterDriver(); }
    void setMasterDriver(hippo::MasterDriver* masterDriver) override { _scheduler->setMasterDriver(masterDriver); }
    ::google::protobuf::Service* getService() override { return _scheduler->getService(); }
    ::google::protobuf::Service* getOpsService() override { return _scheduler->getOpsService(); }

public:
    //<has synced, ange identifier>
    // if not has synced, need to wait several loop,
    std::pair<bool, std::string> getAgentIdentifier(const std::string& agentRoleName) const;
    bool isAgentServiceReady(const std::string& agentRoleName) const;
    bool declareAgentRole(const std::string& agentRole, const AgentRoleInfoPtr& agentInfo);
    void removeAgentRole(const std::string& agentRole);
    void removeUselessAgentNodes(const std::set<proto::BuildId>& inUseBuildIds);
    void getAgentRoleNames(const proto::BuildId& id, std::set<std::string>& agentRoles);

    /* return in black agentRole name */
    std::string addTargetRoleToBlackList(const std::string& roleName,
                                         const std::vector<hippo::SlotInfo>& roleSlotInfos);

public:
    bool isUsingAgentNode(const std::string& roleName) const
    {
        std::string tmp;
        return findAgentRoleName(roleName, tmp);
    }

    bool findAgentRoleName(const std::string& targetRole, std::string& agentRole) const;
    void getAgentRoleNames(std::vector<std::string>& names);
    bool getInBlackListAgentRoles(const std::string& targetRole, std::set<std::string>& inblackListAgentRoles) const;
    bool isInBlackList(const std::string& targetRole, const std::string& targetAgentRole) const;
    bool getAgentNodeInBlackListTimestamp(const std::string& agentRole, int64_t& timestamp) const;
    bool hasValidTargetRoles(const std::string& agentRole) const;
    void getTotalResourceAmount(int64_t& totalCpuAmount, int64_t& totalMemAmount) const;

private:
    void enableClearUselessAgentZkNodes();
    void updateTargetRoleMap();
    bool refreshBlackListInfo();
    void fillAgentSlotInfo(const std::map<std::string, master_framework::master_base::SlotInfos>& roleSlots);
    void clearUselessZkNodes();
    bool checkPackageInfoMatch(const master_framework::master_base::PackageInfos& lft,
                               const master_framework::master_base::PackageInfos& rht) const;
    void updateAgentNodeInfos();
    void workLoop();

    bool isInBlackListUnsafe(const std::string& targetRole, const std::string& targetAgentRole) const;
    bool getAgentNodeSlotInfo(const std::string& agentRole, bool& isReclaim, std::string& slotAddress) const;
    bool getFreshBlackListData(std::string& data);
    void recoverBlackListRoleMap();
    void serializeBlackListData();
    void refreshInBlackListAgentInfo();
    void fillAgentIdentifier(const std::map<std::string, SlotInfos>& roleSlots);
    void updateResourceStatistic(const std::map<std::string, SlotInfos>& roleSlots);
    void syncSlotInfosUnsafe(std::map<std::string, SlotInfos>& roleSlots);

private:
    static constexpr int32_t CLEAR_USELESS_ZK_NODE_INTERVAL = 1200; // 20 min

    struct BlackAgentNode : public autil::legacy::Jsonizable {
        std::string agentRole;
        std::string slotAddress; /* in blacklist slot address */
        int64_t timestamp = -1;  /* in blacklist timestamp */
        bool isReclaim = false;

        virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
        {
            json.Jsonize("agent", agentRole, agentRole);
            json.Jsonize("slot_address", slotAddress, slotAddress);
            json.Jsonize("timestamp", timestamp, timestamp);
            json.Jsonize("reclaim", isReclaim, isReclaim);
        }
    };

private:
    std::string _zkRoot;
    std::string _appName;

    worker_framework::ZkState _zkStatus;
    WorkerTable* _workerTable; // for agent nodes
    std::map<std::string, AgentRoleInfoPtr> _agentRoles;
    std::map<std::string, std::string> _targetRoleMap; /* first: originRoleName, second: agentRoleName */
    std::map<std::string, std::string> _agentIdentifiers;

    typedef std::vector<BlackAgentNode> AgentBlackList;
    std::map<std::string, AgentBlackList> _blackListRoleMap;        /* first: roleName */
    std::unordered_map<std::string, int64_t> _inBlackListAgentInfo; /* first: agentRoleName, second : timestamp */

    mutable autil::RecursiveThreadMutex _lock;
    autil::LoopThreadPtr _loopThread;

    std::shared_ptr<master_framework::simple_master::SimpleMasterScheduler> _scheduler;

    volatile int64_t _lastClearTimestamp;
    volatile int64_t _lastSyncSlotTimestamp;
    volatile bool _needSyncBlackList;
    volatile int64_t _totalCpuAmount;
    volatile int64_t _totalMemAmount;

    kmonitor_adapter::MetricPtr _setPlanLatency;
    kmonitor_adapter::MetricPtr _agentNodeCount;
    kmonitor_adapter::MetricPtr _blackListNodeCount;
    kmonitor_adapter::MetricPtr _useAgentRoleCount;
    kmonitor_adapter::MetricPtr _reclaimingAgentNodeCount;
    kmonitor_adapter::MetricPtr _cpuAmountMetric;
    kmonitor_adapter::MetricPtr _memAmountMetric;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(AgentSimpleMasterScheduler);

}} // namespace build_service::admin
