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
#include "build_service/admin/GlobalAgentMaintainer.h"

#include "autil/ZlibCompressor.h"
#include "build_service/common/PathDefine.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/util/Monitor.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace build_service::proto;
using namespace build_service::common;
using namespace worker_framework;
using namespace master_framework::master_base;
using namespace master_framework::simple_master;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, GlobalAgentMaintainer);

void GlobalAgentMaintainer::FlexibleScaleConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("global_id", globalId);
    json.Jsonize("id", groupId);
    json.Jsonize("min_node_count", minNodeCount);
    json.Jsonize("max_node_count", maxNodeCount);
    json.Jsonize("reserved_idle_node_count", reservedIdleNodeCount);
    json.Jsonize("reduce_interval", reduceCapacityInterval, reduceCapacityInterval);
}

bool GlobalAgentMaintainer::FlexibleScaleConfig::operator==(const FlexibleScaleConfig& other) const
{
    return globalId == other.globalId && groupId == other.groupId && minNodeCount == other.minNodeCount &&
           maxNodeCount == other.maxNodeCount && reservedIdleNodeCount == other.reservedIdleNodeCount &&
           reduceCapacityInterval == other.reduceCapacityInterval;
}

bool GlobalAgentMaintainer::FlexibleScaleConfig::check() const
{
    if (globalId.empty() || groupId.empty()) {
        BS_LOG(ERROR, "global_id or id is empty");
        return false;
    }
    if (minNodeCount > maxNodeCount) {
        BS_LOG(ERROR, "min_node_count [%u] is bigger than maxNodeCount [%u]", minNodeCount, maxNodeCount);
        return false;
    }
    if (reservedIdleNodeCount > maxNodeCount) {
        BS_LOG(ERROR, "reserved_idle_node_count [%u] is bigger than maxNodeCount [%u]", reservedIdleNodeCount,
               maxNodeCount);
        return false;
    }
    return true;
}

GlobalAgentMaintainer::GlobalAgentMaintainer(cm_basic::ZkWrapper* zkWrapper, const std::string& zkRoot,
                                             const std::string& adminServiceName, const std::string& amonitorPort,
                                             kmonitor_adapter::Monitor* monitor)
    : _zkWrapper(zkWrapper)
    , _zkRoot(zkRoot)
    , _zkStatus(_zkWrapper, fslib::util::FileUtil::joinFilePath(_zkRoot, PathDefine::ZK_GLOBAL_AGENT_TARGET_INFO))
    , _reporter(monitor, true)
    , _adminServiceName(adminServiceName)
    , _amonitorPort(amonitorPort)
{
    if (monitor) {
        _syncRoleTargetQpsMetric = monitor->registerQPSMetric("global_agent.roleTargetSyncQps", kmonitor::FATAL);
    }
}

GlobalAgentMaintainer::~GlobalAgentMaintainer() {}

bool GlobalAgentMaintainer::initFromZkRoot()
{
    ZkState zkStatus(_zkWrapper, fslib::util::FileUtil::joinFilePath(_zkRoot, PathDefine::ZK_GLOBAL_AGENT_CONFIG));
    std::string configStr;
    WorkerState::ErrorCode ec = zkStatus.read(configStr);
    if (WorkerState::EC_FAIL == ec) {
        BS_LOG(ERROR, "read global_agent_config failed, ec[%d].", (int)ec);
        return false;
    }
    if (configStr.empty()) {
        return true;
    }
    return loadFromString(configStr);
}

/*
  {
     "global_agent_config" :  [
         {
            "global_id" : "ha3_313",
            "agent_node_groups" : [],
            "packages" : [],
            "role_default" : ...
         },
         {
            "agent_node_groups" : [],
            "packages" : [],
            "role_default" : ...
         }
     ]
  }
 */
bool GlobalAgentMaintainer::loadFromString(const std::string& content)
{
    autil::legacy::json::JsonMap jsonMap;
    try {
        autil::legacy::Any a = autil::legacy::json::ParseJson(content);
        jsonMap = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(a);
        auto iter = jsonMap.find(GLOBAL_AGENT_CONFIG_KEY);
        if (iter == jsonMap.end()) {
            BS_LOG(ERROR, "global_agent_config not find.");
            return false;
        }
        auto jsonArray = autil::legacy::AnyCast<autil::legacy::json::JsonArray>(iter->second);
        if (!loadAgentGroups(jsonArray)) {
            return false;
        }

        iter = jsonMap.find(FLEX_SCALE_CONFIG_KEY);
        if (iter != jsonMap.end()) {
            autil::legacy::FromJson(_flexibleScaleConfigs, iter->second);
        }
        if (!checkFlexibleScaleConfig()) {
            return false;
        }
        iter = jsonMap.find(FLEX_UPDATE_LOG_KEY);
        if (iter != jsonMap.end()) {
            autil::legacy::FromJson(_flexibleAgentGroupUpdateLog, iter->second);
        }
    } catch (const autil::legacy::ExceptionBase& e) {
        string errorMsg = "jsonize " + content + " failed, exception: " + string(e.what());
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    string writeContent = content;
    if (rewriteFlexibleUpdateLog()) {
        jsonMap[FLEX_UPDATE_LOG_KEY] = autil::legacy::ToJson(_flexibleAgentGroupUpdateLog);
        writeContent = autil::legacy::json::ToString(jsonMap);
    }
    _groupTargetConfigs.resize(_agentGroups.size());
    _targetRoleConfigMap = std::make_shared<TargetRoleConfigMap>();
    if (!loadRoleTargetMap()) {
        BS_LOG(ERROR, "load global_agent_target_info failed.");
        return false;
    }
    ZkState zkStatus(_zkWrapper, fslib::util::FileUtil::joinFilePath(_zkRoot, PathDefine::ZK_GLOBAL_AGENT_CONFIG));
    WorkerState::ErrorCode ec = zkStatus.write(writeContent);
    if (WorkerState::EC_FAIL == ec) {
        BS_LOG(ERROR, "write global_agent_config failed, ec[%d].", (int)ec);
        return false;
    }
    return true;
}

void GlobalAgentMaintainer::addTargetRolePlan(const RolePlanMap& rolePlanMap, const AgentRoleInfoMapPtr& agentRolePlan,
                                              const string& configPath, const string& targetGroupId)
{
    if (_agentGroups.empty()) {
        return;
    }
    size_t idx = 0;
    for (const auto& [roleName, rolePlan] : rolePlanMap) {
        bool inAgentPlan = false;
        if (agentRolePlan) {
            for (const auto& [_, agentRoleInfo] : *agentRolePlan) {
                if (agentRoleInfo->targetRoles.find(roleName) != agentRoleInfo->targetRoles.end()) {
                    inAgentPlan = true;
                    break;
                }
            }
        }
        if (inAgentPlan) {
            continue;
        }
        bool match = false;
        for (size_t i = 0; i < _agentGroups.size(); i++) {
            if (idx >= _agentGroups.size()) {
                idx = 0;
            }
            bool matchTargetGroup = (targetGroupId.empty() || targetGroupId == _agentGroups[idx]->getAgentGroupName());
            if (matchTargetGroup && _agentGroups[idx]->matchPackage(rolePlan.packageInfos)) {
                match = true;
                break;
            }
            ++idx;
        }
        if (!match) {
            continue;
        }
        _targetRoles[roleName] = idx;
        if (_agentGroups[idx]->enablePreloadConfigs()) {
            _groupTargetConfigs[idx].insert(configPath);
            (*_targetRoleConfigMap)[roleName] = configPath;
        }
    }
}

AgentRoleInfoMapPtr GlobalAgentMaintainer::makePlan(AgentSimpleMasterScheduler* scheduler, const AppPlan& appPlan)
{
    std::vector<AppPlanPtr> appPlanVec;
    appPlanVec.resize(_agentGroups.size());
    for (size_t i = 0; i < _agentGroups.size(); i++) {
        appPlanVec[i] = std::make_shared<AppPlan>();
    }

    for (auto item : appPlan.rolePlans) {
        const std::string& roleName = item.first;
        auto iter = _targetRoles.find(roleName);
        if (iter == _targetRoles.end()) {
            continue;
        }
        size_t idx = iter->second;
        assert(idx < appPlanVec.size());
        appPlanVec[idx]->rolePlans[roleName] = item.second;
    }

    AgentRoleInfoMapPtr plan;
    AgentRoleTargetMapPtr targetRoleMap;
    for (size_t i = 0; i < _agentGroups.size(); i++) {
        auto result = _agentGroups[i]->makePlan(scheduler, appPlanVec[i]);
        rewriteGlobalAgentInfo(result.first, _groupTargetConfigs[i]);
        if (plan == nullptr) {
            plan = result.first;
        } else if (result.first) { // merge agent plan
            for (auto& [key, value] : *result.first) {
                (*plan)[key] = value;
            }
        }
        if (targetRoleMap == nullptr) {
            targetRoleMap = result.second;
        } else if (result.second) { // merge target role map
            for (auto& [key, value] : *result.second) {
                (*targetRoleMap)[key] = value;
            }
        }
    }
    if (plan != nullptr) {
        reportScheduleInfoMetrics();
    }
    updateRoleTargetMap(targetRoleMap);
    resetTargetRoleState();
    return plan;
}

void GlobalAgentMaintainer::fillGlobalAgentBuildIds(std::set<proto::BuildId>& buildIds)
{
    for (auto& agentGroup : _agentGroups) {
        assert(agentGroup);
        buildIds.insert(agentGroup->getAgentGroupBuildId());
    }
}

void GlobalAgentMaintainer::resetTargetRoleState()
{
    _targetRoles.clear();
    for (size_t i = 0; i < _groupTargetConfigs.size(); i++) {
        _groupTargetConfigs[i].clear();
    }
    _targetRoleConfigMap = std::make_shared<TargetRoleConfigMap>();
}

void GlobalAgentMaintainer::updateRoleTargetMap(const AgentRoleTargetMapPtr& targetRoleMap)
{
    string content;
    if (targetRoleMap) {
        content = autil::legacy::FastToJsonString(*targetRoleMap, true);
    }
    if (content == _latestRoleTargetMapString) {
        return;
    }

    setAgentRoleTargetMap(targetRoleMap);
    _latestRoleTargetMapString = content;
    std::string finalStatusStr;
    autil::ZlibCompressor compressor;
    compressor.compress(content, finalStatusStr);
    if (WorkerState::EC_FAIL == _zkStatus.write(finalStatusStr)) {
        BS_LOG(WARN, "update global_agent_target_info file [%s] failed", content.c_str());
    }
    REPORT_KMONITOR_METRIC(_syncRoleTargetQpsMetric, 1);
}

bool GlobalAgentMaintainer::loadRoleTargetMap()
{
    std::string content;
    WorkerState::ErrorCode ec = _zkStatus.read(content);
    if (WorkerState::EC_FAIL == ec) {
        BS_LOG(ERROR, "read global_agent_target_info failed, ec[%d].", (int)ec);
        return false;
    }
    if (content.empty()) {
        return true;
    }

    autil::ZlibCompressor compressor;
    string decompressedStr;
    if (!compressor.decompress(content, decompressedStr)) {
        BS_LOG(ERROR, "decompress black list string failed [%s]", content.c_str());
        return false;
    }

    AgentRoleTargetMapPtr history = std::make_shared<AgentRolePlanMaker::AgentRoleTargetMap>();
    try {
        autil::legacy::FastFromJsonString(*history, decompressedStr);
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "From json string failed, exception[%s]", e.what());
        return false;
    } catch (...) {
        AUTIL_LOG(ERROR, "From json string failed [%s]", decompressedStr.c_str());
        return false;
    }
    setAgentRoleTargetMap(history);
    _latestRoleTargetMapString = decompressedStr;
    return true;
}

void GlobalAgentMaintainer::rewriteGlobalAgentInfo(const AgentRoleInfoMapPtr& plan, const GlobalTargetConfig& configs)
{
    if (plan == nullptr || configs.empty()) {
        return;
    }
    for (auto& [_, value] : *plan) {
        value->globalTargetConfigs.insert(configs.begin(), configs.end());
        value->globalTargetRoleConfigMap = _targetRoleConfigMap;
    }
}

void GlobalAgentMaintainer::reportScheduleInfoMetrics()
{
    AgentRolePlanMaker::AgentGroupScheduleInfo info;
    fillScheduleInfo(info);
    _reporter.reportMetric(info);
}

void GlobalAgentMaintainer::fillScheduleInfo(AgentRolePlanMaker::AgentGroupScheduleInfo& info) const
{
    for (size_t i = 0; i < _agentGroups.size(); i++) {
        _agentGroups[i]->getScheduleInfo(info);
    }
}

void GlobalAgentMaintainer::setAgentRoleTargetMap(const AgentRoleTargetMapPtr& map)
{
    _latestRoleTargetMapGuard.set(map);
}

GlobalAgentMaintainer::AgentRoleTargetMapPtr GlobalAgentMaintainer::getAgentRoleTargetMap() const
{
    AgentRoleTargetMapPtr map;
    _latestRoleTargetMapGuard.get(map);
    return map;
}

bool GlobalAgentMaintainer::loadAgentGroups(const autil::legacy::json::JsonArray& jsonArray)
{
    std::set<std::string> uniqSet;
    for (auto& item : jsonArray) {
        auto singleGroup = std::make_shared<SingleGlobalAgentGroup>(_zkRoot, _adminServiceName, _amonitorPort);
        if (!singleGroup->loadFromJson(item, [this]() { return getAgentRoleTargetMap(); })) {
            return false;
        }
        const std::string& groupName = singleGroup->getAgentGroupName();
        if (uniqSet.find(groupName) != uniqSet.end()) {
            BS_LOG(ERROR, "find duplicate global agent group with name [%s]", groupName.c_str());
            return false;
        }
        uniqSet.insert(groupName);
        if (singleGroup->enableGlobalAgentNodes()) {
            BS_LOG(INFO, "enable global agent nodes for agent group [%s]", groupName.c_str());
            _agentGroups.push_back(singleGroup);
        }
    }
    return true;
}

bool GlobalAgentMaintainer::checkFlexibleScaleConfig()
{
    for (auto& flexConfig : _flexibleScaleConfigs) {
        if (!flexConfig.check()) {
            BS_LOG(ERROR, "invalid flexible_scale_config [%s]", autil::legacy::ToJsonString(flexConfig, true).c_str());
            return false;
        }
        bool found = false;
        for (auto& singleGroup : _agentGroups) {
            if (singleGroup->getAgentGroupName() != flexConfig.globalId) {
                continue;
            }
            auto agentConfigs = singleGroup->getAgentConfigs();
            for (auto& config : agentConfigs) {
                if (config.identifier == flexConfig.groupId) {
                    if (config.flexibleIdleAgentCount != 0) {
                        BS_LOG(ERROR,
                               "target [%s.%s] already set flexible_idle_agent_count, "
                               "not support flexible scale.",
                               flexConfig.globalId.c_str(), flexConfig.groupId.c_str());
                        return false;
                    }
                    found = true;
                    break;
                }
            }
            break;
        }
        if (!found) {
            BS_LOG(ERROR, "invalid flexible_scale_config [%s], target not exist!",
                   autil::legacy::ToJsonString(flexConfig, true).c_str());
            return false;
        }
    }
    return true;
}

bool GlobalAgentMaintainer::rewriteFlexibleUpdateLog()
{
    bool ret = false;
    for (auto& flexConfig : _flexibleScaleConfigs) {
        string key = flexConfig.getKey();
        if (_flexibleAgentGroupUpdateLog.find(key) != _flexibleAgentGroupUpdateLog.end()) {
            continue;
        }
        _flexibleAgentGroupUpdateLog[key] = autil::TimeUtility::currentTimeInSeconds();
        ret = true;
    }
    return ret;
}

bool GlobalAgentMaintainer::getFlexibleUpdatePlan(std::map<std::pair<std::string, std::string>, uint32_t>& plan) const
{
    if (_flexibleScaleConfigs.empty()) {
        return false;
    }
    AgentRolePlanMaker::AgentGroupScheduleInfo info;
    fillScheduleInfo(info);
    return generateFlexibleUpdatePlan(info, plan);
}

bool GlobalAgentMaintainer::generateFlexibleUpdatePlan(
    const AgentRolePlanMaker::AgentGroupScheduleInfo& info,
    std::map<std::pair<std::string, std::string>, uint32_t>& plan) const
{
    plan.clear();
    bool ret = false;
    for (auto& flexConfig : _flexibleScaleConfigs) {
        string key = flexConfig.getKey();
        auto iter = info.find(key);
        if (iter == info.end()) {
            BS_LOG(ERROR, "no schedule info found for [%s]", key.c_str());
            continue;
        }

        if (iter->second.totalAgentNodeCnt > flexConfig.maxNodeCount) {
            BS_LOG(INFO, "flexible decrease agent node count from [%u] to [%u] for [%s]",
                   iter->second.totalAgentNodeCnt, flexConfig.maxNodeCount, key.c_str());
            auto target = make_pair(flexConfig.globalId, flexConfig.groupId);
            plan[target] = flexConfig.maxNodeCount;
            ret = true;
            continue;
        }
        if (iter->second.totalAgentNodeCnt < flexConfig.minNodeCount) {
            BS_LOG(INFO, "flexible increase agent node count from [%u] to [%u] for [%s]",
                   iter->second.totalAgentNodeCnt, flexConfig.minNodeCount, key.c_str());
            auto target = make_pair(flexConfig.globalId, flexConfig.groupId);
            plan[target] = flexConfig.minNodeCount;
            ret = true;
            continue;
        }

        if (iter->second.idleAgentNodeCnt < flexConfig.reservedIdleNodeCount &&
            iter->second.totalAgentNodeCnt < flexConfig.maxNodeCount) {
            // need expand capacity
            uint32_t increaseNodeCnt = flexConfig.reservedIdleNodeCount - iter->second.idleAgentNodeCnt;
            uint32_t newNodeCnt = std::min(flexConfig.maxNodeCount, iter->second.totalAgentNodeCnt + increaseNodeCnt);
            BS_LOG(INFO, "flexible increase agent node count from [%u] to [%u] for [%s]",
                   iter->second.totalAgentNodeCnt, newNodeCnt, key.c_str());
            auto target = make_pair(flexConfig.globalId, flexConfig.groupId);
            plan[target] = newNodeCnt;
            ret = true;
            continue;
        }
        if (iter->second.idleAgentNodeCnt <= flexConfig.reservedIdleNodeCount ||
            iter->second.totalAgentNodeCnt == flexConfig.minNodeCount) {
            // no need reduce capacity
            continue;
        }
        auto tsIter = _flexibleAgentGroupUpdateLog.find(key);
        if (tsIter == _flexibleAgentGroupUpdateLog.end()) {
            continue;
        }
        auto tsGap = autil::TimeUtility::currentTimeInSeconds() - tsIter->second;
        if (tsGap < flexConfig.reduceCapacityInterval) {
            // not reach interval
            continue;
        }
        auto target = make_pair(flexConfig.globalId, flexConfig.groupId);
        uint32_t decreaseNodeCnt = iter->second.idleAgentNodeCnt - flexConfig.reservedIdleNodeCount;
        uint32_t newNodeCnt =
            (iter->second.totalAgentNodeCnt <= decreaseNodeCnt) ? 0 : iter->second.totalAgentNodeCnt - decreaseNodeCnt;
        newNodeCnt = std::max(flexConfig.minNodeCount, newNodeCnt);
        BS_LOG(INFO, "flexible decrease agent node count from [%u] to [%u] for [%s]", iter->second.totalAgentNodeCnt,
               newNodeCnt, key.c_str());
        plan[target] = newNodeCnt;
        ret = true;
    }
    return ret;
}

}} // namespace build_service::admin
