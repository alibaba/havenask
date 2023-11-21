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

#include <functional>
#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/RangeUtil.h"
#include "build_service/admin/AgentRoleInfo.h"
#include "build_service/admin/AppPlanMaker.h"
#include "build_service/common_define.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class AgentSimpleMasterScheduler;

class AgentRolePlanMaker
{
public:
    // agentRole -> <agentIdentifier, [normal roles]>
    typedef std::map<std::string, std::pair<std::string, std::vector<std::string>>> AgentRoleTargetMap;
    typedef std::shared_ptr<AgentRoleTargetMap> AgentRoleTargetMapPtr;
    using GetAgentRoleHistoryFunc = std::function<AgentRoleTargetMapPtr()>;

    struct AgentGroupMetricData {
        proto::BuildId buildId;
        std::string groupId;
        uint32_t totalAgentNodeCnt = 0;
        uint32_t assignAgentNodeCnt = 0;
        uint32_t idleAgentNodeCnt = 0;  // no target role agent
        uint32_t readyAgentNodeCnt = 0; // identifier not empty
        uint32_t totalRoleCnt = 0;
        uint32_t assignRoleCnt = 0;
        bool isGlobalAgent = false;
        bool isDynamicMapping = false;
    };
    // key: agentGroupId
    typedef std::map<std::string, AgentGroupMetricData> AgentGroupScheduleInfo;

public:
    AgentRolePlanMaker(const proto::BuildId& buildId, const std::string& zkRoot, const std::string& adminServiceName,
                       const std::string& amonitorPort, bool isGlobalAgent, GetAgentRoleHistoryFunc func);

    ~AgentRolePlanMaker();

public:
    void init(const config::AgentGroupConfigPtr& config, const std::shared_ptr<AppPlanMaker>& planMaker);

    std::pair<AgentRoleInfoMapPtr, AgentRoleTargetMapPtr> makePlan(AgentSimpleMasterScheduler* scheduler,
                                                                   const AppPlanPtr& appPlan) const;

    void getScheduleInfo(AgentGroupScheduleInfo& info) const;

    bool allowAssignedToGlobalAgentNodes() const;
    const std::string& getTargetGlobalAgentGroupId() const;

private:
    AgentRoleInfoMapPtr prepareAgentRolePlan() const;

    // inheritRoleMap [workerRole -> pair<agentRole, agentIdentifier>]
    void extractInheritRoleMap(const AgentRoleInfoMapPtr& agentRolePlan,
                               std::map<std::string, std::pair<std::string, std::string>>& inheritRoleMap) const;

    bool staticScheduleTargetRole(const AppPlanPtr& appPlan, const AgentRoleInfoMapPtr& agentRolePlan,
                                  const std::string& targetRoleName, size_t groupIdx, std::string& agentRoleName) const;

    bool inheritFromDynamicAgentNode(AgentSimpleMasterScheduler* scheduler,
                                     const std::map<std::string, std::pair<std::string, std::string>>& inheritRoleMap,
                                     const AppPlanPtr& appPlan, const AgentRoleInfoMapPtr& agentRoleMap,
                                     const std::string& targetRoleName, size_t groupIdx,
                                     std::string& agentRoleName) const;

    void executeDynamicSchedule(AgentSimpleMasterScheduler* scheduler, const AppPlanPtr& appPlan,
                                const AgentRoleInfoMapPtr& agentRoleMap,
                                const std::map<size_t, std::vector<std::string>>& needDynamicScheduleRoles) const;

    std::shared_ptr<AgentRoleTargetMap> finalizeAgentRolePlan(AgentSimpleMasterScheduler* scheduler,
                                                              const AgentRoleInfoMapPtr& agentRoleMap) const;

    void rewriteAgentRoleArguments(const std::string& roleName, RolePlan& rolePlan) const;

    bool calculateAgentNodeIndex(const autil::RangeVec& ranges, const std::string& roleName, size_t& index) const;

    static bool checkAgentResourceLimit(const AppPlanPtr& appPlan, const AgentRoleInfoMapPtr& agentRoleMap,
                                        const std::string& agentRoleName, const std::string& targetRoleName,
                                        const config::AgentConfig& config);

    static size_t calculateAgentNodeIndex(const autil::RangeVec& ranges, const std::string& roleName, uint32_t from,
                                          uint32_t to);

    static bool getResourceAmount(const RolePlan& plan, const std::string& name, int32_t& amount);

private:
    proto::BuildId _buildId;
    std::string _zkRoot;
    std::string _adminServiceName;
    std::string _amonitorPort;

    GetAgentRoleHistoryFunc _getAgentHistoryFunc;
    config::AgentGroupConfigPtr _agentGroupConfig;

    std::vector<autil::RangeVec> _agentRoleRanges;
    std::vector<std::vector<std::string>> _agentRoleNameGroups;
    mutable std::map<std::string, int64_t> _coolingDownRoles;
    mutable std::vector<uint32_t> _candidateCntVec;
    mutable std::vector<uint32_t> _roleMatchCntVec;
    mutable std::vector<uint32_t> _idleAgentCntVec;
    mutable std::vector<uint32_t> _readyAgentCntVec;
    mutable std::vector<uint32_t> _assignAgentCntVec;

    AgentRoleInfoMap _protoTypeMap;
    bool _needDynamicMapping;
    bool _isGlobalAgent;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(AgentRolePlanMaker);

}} // namespace build_service::admin
