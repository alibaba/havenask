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
#include "build_service/config/AgentGroupConfig.h"

#include "autil/Regex.h"

using namespace std;

namespace build_service { namespace config {
BS_LOG_SETUP(config, AgentConfig);
BS_LOG_SETUP(config, AgentGroupConfig);

AgentConfig::AgentConfig()
    : agentNodeCount(0)
    , inBlackListTimeout(DEFAULT_IN_BLACKLIST_TIMEOUT)
    , lazyAllocate(false)
    , dynamicRoleMapping(false)
{
}

AgentConfig::~AgentConfig() {}

void AgentConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("id", identifier);
    json.Jsonize("role_pattern", rolePattern);
    json.Jsonize("node_count", agentNodeCount);
    json.Jsonize("in_blacklist_timeout", inBlackListTimeout, inBlackListTimeout);
    json.Jsonize("lazy_allocate", lazyAllocate, lazyAllocate);
    json.Jsonize("dynamic_role_mapping", dynamicRoleMapping, dynamicRoleMapping);
    json.Jsonize("resource_limits", resourceLimitParams, resourceLimitParams);
}

bool AgentConfig::check() const
{
    if (identifier.empty()) {
        string errorInfo = "invalid id [" + identifier + "] for agent config.";
        BS_INTERVAL_LOG(10, ERROR, "%s", errorInfo.c_str());
        return false;
    }
    if (agentNodeCount == 0) {
        string errorInfo = "invalid agentNodeCount [" + autil::StringUtil::toString(agentNodeCount) +
                           "] for agent config, id [" + identifier + "]";
        BS_INTERVAL_LOG(10, ERROR, "%s", errorInfo.c_str());
        return false;
    }
    if (rolePattern.empty()) {
        string errorInfo = "invalid pattern [" + rolePattern + "] for agent config, id [" + identifier + "]";
        BS_INTERVAL_LOG(10, ERROR, "%s", errorInfo.c_str());
        return false;
    }
    return true;
}

AgentGroupConfig::AgentGroupConfig() {}

AgentGroupConfig::~AgentGroupConfig() {}

void AgentGroupConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("agent_node_groups", _agentConfigs);
}

bool AgentGroupConfig::check() const
{
    for (auto& config : _agentConfigs) {
        if (!config.check()) {
            return false;
        }
    }
    return true;
}

bool AgentGroupConfig::match(const string& roleName, size_t& idx) const
{
    auto iter = _roleMatchInfoCache.find(roleName);
    if (iter != _roleMatchInfoCache.end()) {
        if (iter->second < 0) {
            return false;
        }
        idx = (size_t)iter->second;
        return true;
    }
    for (size_t i = 0; i < _agentConfigs.size(); i++) {
        if (autil::Regex::match(roleName, _agentConfigs[i].rolePattern)) {
            idx = i;
            _roleMatchInfoCache[roleName] = i;
            return true;
        }
    }
    _roleMatchInfoCache[roleName] = -1;
    return false;
}

bool AgentGroupConfig::needDynamicMapping() const
{
    for (auto& config : _agentConfigs) {
        if (config.dynamicRoleMapping) {
            return true;
        }
    }
    return false;
}

}} // namespace build_service::config
