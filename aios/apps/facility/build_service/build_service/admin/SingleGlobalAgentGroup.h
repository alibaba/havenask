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

#include "build_service/admin/AgentRolePlanMaker.h"
#include "build_service/admin/AppPlanMaker.h"
#include "build_service/common_define.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "master_framework/AppPlan.h"

namespace build_service { namespace admin {

class SingleGlobalAgentGroup
{
public:
    using AgentRoleTargetMapPtr = AgentRolePlanMaker::AgentRoleTargetMapPtr;
    using AgentGroupScheduleInfo = AgentRolePlanMaker::AgentGroupScheduleInfo;

public:
    SingleGlobalAgentGroup(const std::string& zkRoot, const std::string& adminServiceName,
                           const std::string& amonitorPort);

    ~SingleGlobalAgentGroup();

public:
    bool loadFromJson(const autil::legacy::Any& any, AgentRolePlanMaker::GetAgentRoleHistoryFunc func);

    bool matchPackage(const std::vector<hippo::PackageInfo>& package);

    std::pair<AgentRoleInfoMapPtr, AgentRoleTargetMapPtr> makePlan(AgentSimpleMasterScheduler* scheduler,
                                                                   const AppPlanPtr& appPlan) const;

    const std::string& getAgentGroupName() const { return _agentGroupName; }
    const proto::BuildId& getAgentGroupBuildId() const { return _buildId; }
    bool enableGlobalAgentNodes() const;
    bool enablePreloadConfigs() const { return _preloadConfig; }

    void getScheduleInfo(AgentGroupScheduleInfo& info) const;
    const std::string& getConfigJsonString() const { return _configJsonStr; }
    std::vector<config::AgentConfig> getAgentConfigs() const;

private:
    std::string _zkRoot;
    std::string _adminServiceName;
    std::string _amonitorPort;

    AppPlanMakerPtr _planMaker;
    config::AgentGroupConfigPtr _agentGroupConfig;
    AgentRolePlanMakerPtr _agentRolePlanMaker;
    proto::BuildId _buildId;
    std::string _agentGroupName;
    bool _preloadConfig = true;
    std::string _configJsonStr;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SingleGlobalAgentGroup);

}} // namespace build_service::admin
