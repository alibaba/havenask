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
    , flexibleIdleAgentCount(0)
    , inBlackListTimeout(DEFAULT_IN_BLACKLIST_TIMEOUT)
    , lazyAllocate(false)
    , dynamicRoleMapping(false)
    , exclusive(false)
{
}

AgentConfig::~AgentConfig() {}

void AgentConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("id", identifier);
    json.Jsonize("role_pattern", rolePattern);
    json.Jsonize("node_count", agentNodeCount);
    json.Jsonize("flexible_idle_agent_count", flexibleIdleAgentCount, flexibleIdleAgentCount);
    json.Jsonize("in_blacklist_timeout", inBlackListTimeout, inBlackListTimeout);
    json.Jsonize("lazy_allocate", lazyAllocate, lazyAllocate);
    json.Jsonize("dynamic_role_mapping", dynamicRoleMapping, dynamicRoleMapping);
    json.Jsonize("resource_limits", resourceLimitParams, resourceLimitParams);
    json.Jsonize("exclusive", exclusive, exclusive);
}

bool AgentConfig::check() const
{
    if (identifier.empty() || identifier.find(".") != string::npos) {
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
    if (flexibleIdleAgentCount != 0 && !dynamicRoleMapping) {
        string errorInfo = "invalid flexibleIdleAgentCount [" + autil::StringUtil::toString(flexibleIdleAgentCount) +
                           "] with non dynamicRoleMapping for agent config, id [" + identifier + "]";
        BS_INTERVAL_LOG(10, ERROR, "%s", errorInfo.c_str());
        return false;
    }
    if (flexibleIdleAgentCount != 0 && flexibleIdleAgentCount > agentNodeCount) {
        string errorInfo = "invalid flexibleIdleAgentCount [" + autil::StringUtil::toString(flexibleIdleAgentCount) +
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

const std::string AgentGroupConfig::AGENT_CONFIG_GROUP_KEY = "agent_node_groups";

AgentGroupConfig::AgentGroupConfig(bool defaultEnableGlobalAgent) : _enableGlobalAgentNodes(defaultEnableGlobalAgent) {}

AgentGroupConfig::~AgentGroupConfig() {}

void AgentGroupConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize(AGENT_CONFIG_GROUP_KEY, _agentConfigs, _agentConfigs);
    json.Jsonize("enable_global_agent_nodes", _enableGlobalAgentNodes, _enableGlobalAgentNodes);
    json.Jsonize("target_global_agent_group_id", _targetGlobalAgentGroupName, _targetGlobalAgentGroupName);
    if (!_targetGlobalAgentGroupName.empty() && _targetGlobalAgentGroupName.find(".") != string::npos) {
        BS_LOG(INFO, "target_global_agent_group_id [%s] has invalid char '.', will use '-' instead.",
               _targetGlobalAgentGroupName.c_str());
        autil::StringUtil::replaceAll(_targetGlobalAgentGroupName, ".", "-");
    }
}

bool AgentGroupConfig::check() const
{
    std::set<std::string> idSet;
    for (auto& config : _agentConfigs) {
        if (!config.check()) {
            return false;
        }
        idSet.insert(config.identifier);
    }
    return idSet.size() == _agentConfigs.size();
}

bool AgentGroupConfig::match(const string& roleName, size_t& idx) const
{
    int64_t currentTimeInSec = autil::TimeUtility::currentTimeInSeconds();
    if (_cacheExpireTimestamp <= currentTimeInSec) {
        _cacheExpireTimestamp = currentTimeInSec + DEFAULT_CACHE_EXPIRE_TIME;
        // make cache expire, to retire useless role cache info
        std::map<std::string, int32_t> emptyCache;
        _roleMatchInfoCache.swap(emptyCache);
    }

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
