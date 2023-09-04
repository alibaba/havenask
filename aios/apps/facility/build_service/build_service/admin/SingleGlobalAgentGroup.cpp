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
#include "build_service/admin/SingleGlobalAgentGroup.h"

#include "autil/CRC32C.h"

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, SingleGlobalAgentGroup);

SingleGlobalAgentGroup::SingleGlobalAgentGroup(const std::string& zkRoot, const std::string& adminServiceName,
                                               const std::string& amonitorPort)
    : _zkRoot(zkRoot)
    , _adminServiceName(adminServiceName)
    , _amonitorPort(amonitorPort)
{
}

SingleGlobalAgentGroup::~SingleGlobalAgentGroup() {}

bool SingleGlobalAgentGroup::loadFromJson(const autil::legacy::Any& any,
                                          AgentRolePlanMaker::GetAgentRoleHistoryFunc func)
{
    AppPlanMakerPtr planMaker = std::make_shared<AppPlanMaker>(_adminServiceName, "rpc");
    config::AgentGroupConfigPtr config = std::make_shared<config::AgentGroupConfig>(true);
    try {
        autil::legacy::FromJson(*planMaker, any);
        autil::legacy::FromJson(*config, any);
        auto jsonMap = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(any);
        auto iter = jsonMap.find("global_id"); // set user-defined global_id
        if (iter != jsonMap.end()) {
            _agentGroupName = autil::legacy::AnyCast<std::string>(iter->second);
            if (_agentGroupName.find(".") != string::npos) {
                BS_LOG(INFO, "global_id [%s] has invalid char '.', will use '-' instead.", _agentGroupName.c_str());
                autil::StringUtil::replaceAll(_agentGroupName, ".", "-");
            }
        }
        iter = jsonMap.find("global_agent_preload_config"); // enable preload config
        if (iter != jsonMap.end()) {
            _preloadConfig = autil::legacy::AnyCast<bool>(iter->second);
        }
    } catch (const autil::legacy::ExceptionBase& e) {
        string errorMsg = "jsonize " + autil::legacy::json::ToString(any) + " failed, exception: " + string(e.what());
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    _planMaker = planMaker;
    _agentGroupConfig = config;
    assert(_agentGroupConfig->getTargetGlobalAgentGroupId().empty());
    if (_agentGroupName.empty()) {
        string packageInfoStr = autil::legacy::ToJsonString(planMaker->getPackageInfos(), true);
        uint32_t packageInfoCrc = autil::CRC32C::Value(packageInfoStr.c_str(), packageInfoStr.size());
        _agentGroupName = "package_crc_" + autil::StringUtil::toString(packageInfoCrc);
        BS_LOG(INFO, "generate global_id [%s] for agent group with package info [%s]", _agentGroupName.c_str(),
               packageInfoStr.c_str());
    }
    if (!_agentGroupConfig->check()) {
        BS_LOG(ERROR, "global agentGroupConfig check fail for id [%s]", _agentGroupName.c_str());
        return false;
    }
    _buildId.set_datatable(_agentGroupName);
    _buildId.set_generationid(0);
    _buildId.set_appname("global-agent-pool");

    _agentRolePlanMaker.reset(
        new AgentRolePlanMaker(_buildId, _zkRoot, _adminServiceName, _amonitorPort, true, std::move(func)));
    _agentRolePlanMaker->init(_agentGroupConfig, _planMaker);
    _configJsonStr = autil::legacy::json::ToString(any);
    return true;
}

bool SingleGlobalAgentGroup::matchPackage(const std::vector<hippo::PackageInfo>& rht)
{
    assert(_planMaker);
    const auto& lft = _planMaker->getPackageInfos();
    if (lft.size() != rht.size()) {
        return false;
    }
    for (size_t i = 0; i < lft.size(); i++) {
        if (lft[i].URI != rht[i].URI || lft[i].type != rht[i].type) {
            return false;
        }
    }
    return true;
}

std::pair<AgentRoleInfoMapPtr, SingleGlobalAgentGroup::AgentRoleTargetMapPtr>
SingleGlobalAgentGroup::makePlan(AgentSimpleMasterScheduler* scheduler, const AppPlanPtr& appPlan) const
{
    if (!_agentRolePlanMaker) {
        return {nullptr, nullptr};
    }
    return _agentRolePlanMaker->makePlan(scheduler, appPlan);
}

bool SingleGlobalAgentGroup::enableGlobalAgentNodes() const
{
    return (_agentGroupConfig != nullptr) ? _agentGroupConfig->enableGlobalAgentNodes() : false;
}

void SingleGlobalAgentGroup::getScheduleInfo(AgentGroupScheduleInfo& info) const
{
    if (_agentRolePlanMaker) {
        _agentRolePlanMaker->getScheduleInfo(info);
    }
}

std::vector<config::AgentConfig> SingleGlobalAgentGroup::getAgentConfigs() const
{
    std::vector<config::AgentConfig> ret;
    if (_agentGroupConfig) {
        ret = _agentGroupConfig->getAgentConfigs();
    }
    return ret;
}

}} // namespace build_service::admin
