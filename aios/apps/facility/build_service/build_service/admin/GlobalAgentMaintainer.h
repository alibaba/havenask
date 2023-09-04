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

#include "build_service/admin/AgentMetricReporter.h"
#include "build_service/admin/AgentRoleInfo.h"
#include "build_service/admin/SingleGlobalAgentGroup.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "worker_framework/ZkState.h"

namespace build_service { namespace admin {

class AgentSimpleMasterScheduler;

class GlobalAgentMaintainer
{
public:
    using AgentRoleTargetMap = AgentRolePlanMaker::AgentRoleTargetMap;
    using AgentRoleTargetMapPtr = AgentRolePlanMaker::AgentRoleTargetMapPtr;
    using TargetRoleConfigMap = std::map<std::string, std::string>; /* roleName -> configName */
    using TargetRoleConfigMapPtr = std::shared_ptr<TargetRoleConfigMap>;

    struct FlexibleScaleConfig : public autil::legacy::Jsonizable {
    public:
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
        bool check() const;
        bool operator==(const FlexibleScaleConfig& other) const;

        static std::string getKey(const std::string& globalId, const std::string& id) { return globalId + "." + id; }
        std::string getKey() const { return getKey(globalId, groupId); }

        std::string globalId;
        std::string groupId;
        uint32_t minNodeCount = 0;
        uint32_t maxNodeCount = 0;
        uint32_t reservedIdleNodeCount = 0;
        int64_t reduceCapacityInterval = std::numeric_limits<int64_t>::max();
    };

    static constexpr char GLOBAL_AGENT_CONFIG_KEY[] = "global_agent_config";
    static constexpr char FLEX_SCALE_CONFIG_KEY[] = "flexible_scale_config";
    static constexpr char FLEX_UPDATE_LOG_KEY[] = "flexible_update_log";

public:
    GlobalAgentMaintainer(cm_basic::ZkWrapper* zkWrapper, const std::string& zkRoot,
                          const std::string& adminServiceName, const std::string& amonitorPort,
                          kmonitor_adapter::Monitor* monitor = NULL);
    ~GlobalAgentMaintainer();

public:
    bool initFromZkRoot();
    bool loadFromString(const std::string& content);

    void addTargetRolePlan(const master_framework::master_base::RolePlanMap& rolePlanMap,
                           const AgentRoleInfoMapPtr& agentRolePlan, const std::string& configPath,
                           const std::string& targetGroupId);

    AgentRoleInfoMapPtr makePlan(AgentSimpleMasterScheduler* scheduler,
                                 const master_framework::simple_master::AppPlan& appPlan);

    void fillGlobalAgentBuildIds(std::set<proto::BuildId>& buildIds);

    std::shared_ptr<AgentRoleTargetMap> getAgentRoleTargetMap() const;
    const std::vector<SingleGlobalAgentGroupPtr>& getGlobalAgentGroups() const { return _agentGroups; }

    void fillScheduleInfo(AgentRolePlanMaker::AgentGroupScheduleInfo& info) const;

    const std::vector<FlexibleScaleConfig>& getFlexibleScaleConfigs() const { return _flexibleScaleConfigs; }
    const std::map<std::string, int64_t>& getFlexibleAgentUpdateLog() const { return _flexibleAgentGroupUpdateLog; }

    bool getFlexibleUpdatePlan(std::map<std::pair<std::string, std::string>, uint32_t>& plan) const;

private:
    void updateRoleTargetMap(const AgentRoleTargetMapPtr& targetRoleMap);
    bool loadAgentGroups(const autil::legacy::json::JsonArray& jsonArray);
    bool checkFlexibleScaleConfig();
    bool rewriteFlexibleUpdateLog();
    bool loadRoleTargetMap();
    void rewriteGlobalAgentInfo(const AgentRoleInfoMapPtr& plan, const std::set<std::string>& configs);
    void reportScheduleInfoMetrics();
    void resetTargetRoleState();

    void setAgentRoleTargetMap(const std::shared_ptr<AgentRoleTargetMap>& map);

    bool generateFlexibleUpdatePlan(const AgentRolePlanMaker::AgentGroupScheduleInfo& info,
                                    std::map<std::pair<std::string, std::string>, uint32_t>& plan) const;

private:
    cm_basic::ZkWrapper* _zkWrapper;
    std::string _zkRoot;
    worker_framework::ZkState _zkStatus;
    AgentMetricReporter _reporter;

    std::string _adminServiceName;
    std::string _amonitorPort;

    using GlobalTargetConfig = std::set<std::string>;

    std::vector<SingleGlobalAgentGroupPtr> _agentGroups;
    std::vector<GlobalTargetConfig> _groupTargetConfigs;
    std::unordered_map<std::string, size_t> _targetRoles;
    TargetRoleConfigMapPtr _targetRoleConfigMap;
    util::SharedPtrGuard<AgentRoleTargetMap> _latestRoleTargetMapGuard;
    std::string _latestRoleTargetMapString;
    kmonitor_adapter::MetricPtr _syncRoleTargetQpsMetric;

    std::vector<FlexibleScaleConfig> _flexibleScaleConfigs;
    std::map<std::string, int64_t> _flexibleAgentGroupUpdateLog;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(GlobalAgentMaintainer);

}} // namespace build_service::admin
