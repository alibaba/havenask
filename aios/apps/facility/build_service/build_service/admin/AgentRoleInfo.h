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

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "master_framework/RolePlan.h"

namespace build_service { namespace admin {

struct AgentRoleInfo {
    master_framework::master_base::RolePlan plan;
    std::set<std::string> targetRoles;
    int64_t inBlackListTimeout = std::numeric_limits<int64_t>::max();
    size_t agentConfigGroupIdx = std::numeric_limits<size_t>::max();
    size_t agentRoleNodeIdx = std::numeric_limits<size_t>::max();
    std::string configPath; // for generation level agent

    std::set<std::string> globalTargetConfigs;                                     // for global level agent
    std::shared_ptr<std::map<std::string, std::string>> globalTargetRoleConfigMap; /* targetRoleName -> configPath */

    bool isDynamicMapping = false;
    bool isGlobalAgent = false;
};

BS_TYPEDEF_PTR(AgentRoleInfo);

typedef std::map<std::string, AgentRoleInfoPtr> AgentRoleInfoMap;
typedef std::shared_ptr<AgentRoleInfoMap> AgentRoleInfoMapPtr;

}} // namespace build_service::admin
