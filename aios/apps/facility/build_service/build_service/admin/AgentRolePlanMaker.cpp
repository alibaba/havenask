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
#include "build_service/admin/AgentRolePlanMaker.h"

#include <queue>

#include "build_service/admin/AgentSimpleMasterScheduler.h"
#include "build_service/proto/ProtoUtil.h"

using namespace std;
using namespace build_service::config;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, AgentRolePlanMaker);

AgentRolePlanMaker::AgentRolePlanMaker(const proto::BuildId& buildId, const std::string& zkRoot,
                                       const std::string& adminServiceName, const std::string& amonitorPort,
                                       bool isGlobalAgent, GetAgentRoleHistoryFunc func)
    : _buildId(buildId)
    , _zkRoot(zkRoot)
    , _adminServiceName(adminServiceName)
    , _amonitorPort(amonitorPort)
    , _getAgentHistoryFunc(std::move(func))
    , _needDynamicMapping(false)
    , _isGlobalAgent(isGlobalAgent)
{
}

AgentRolePlanMaker::~AgentRolePlanMaker() {}

void AgentRolePlanMaker::init(const AgentGroupConfigPtr& agentGroupConfig,
                              const std::shared_ptr<AppPlanMaker>& planMaker)
{
    if (agentGroupConfig == nullptr) {
        return;
    }

    if (!agentGroupConfig->check()) {
        BS_LOG(ERROR, "buildId [%s] with invalid group config [%s], check fail.", _buildId.ShortDebugString().c_str(),
               autil::legacy::ToJsonString(*agentGroupConfig, true).c_str());
        return;
    }

    assert(planMaker);
    _agentGroupConfig = agentGroupConfig;
    _needDynamicMapping = agentGroupConfig->needDynamicMapping();
    _agentRoleNameGroups.resize(agentGroupConfig->size());
    _candidateCntVec.resize(agentGroupConfig->size(), 0);
    _roleMatchCntVec.resize(agentGroupConfig->size(), 0);
    _idleAgentCntVec.resize(agentGroupConfig->size(), 0);
    _readyAgentCntVec.resize(agentGroupConfig->size(), 0);
    _assignAgentCntVec.resize(agentGroupConfig->size(), 0);

    for (size_t i = 0; i < agentGroupConfig->size(); i++) {
        auto& config = (*agentGroupConfig)[i];
        for (size_t j = 0; j < config.agentNodeCount; j++) {
            AgentRoleInfoPtr info = std::make_shared<AgentRoleInfo>();
            std::string agentRoleName;
            tie(agentRoleName, info->plan) = planMaker->createAgentRolePlan(
                _buildId, config.identifier, config.agentNodeCount, j, config.dynamicRoleMapping);
            rewriteAgentRoleArguments(agentRoleName, info->plan);
            info->inBlackListTimeout = config.inBlackListTimeout;
            info->agentConfigGroupIdx = i;
            info->agentRoleNodeIdx = j;
            info->isDynamicMapping = config.dynamicRoleMapping;
            info->isGlobalAgent = _isGlobalAgent;
            _protoTypeMap.insert(make_pair(agentRoleName, info));
            _agentRoleNameGroups[i].push_back(agentRoleName);
        }
        _agentRoleRanges.push_back(
            autil::RangeUtil::splitRange(0, autil::RangeUtil::MAX_PARTITION_RANGE, config.agentNodeCount));
    }
}

std::pair<AgentRoleInfoMapPtr, AgentRolePlanMaker::AgentRoleTargetMapPtr>
AgentRolePlanMaker::makePlan(AgentSimpleMasterScheduler* scheduler, const AppPlanPtr& appPlan) const
{
    assert(appPlan);
    AgentRoleInfoMapPtr agentRolePlan;
    AgentRoleTargetMapPtr roleTargetMap;
    if (_agentGroupConfig == nullptr || scheduler == nullptr) {
        return std::make_pair(agentRolePlan, roleTargetMap);
    }

    assert(_candidateCntVec.size() == _roleMatchCntVec.size() && _candidateCntVec.size() == _idleAgentCntVec.size() &&
           _candidateCntVec.size() == _readyAgentCntVec.size() && _candidateCntVec.size() == _assignAgentCntVec.size());
    for (size_t i = 0; i < _candidateCntVec.size(); i++) {
        _candidateCntVec[i] = 0;
        _roleMatchCntVec[i] = 0;
        _idleAgentCntVec[i] = 0;
        _readyAgentCntVec[i] = 0;
        _assignAgentCntVec[i] = 0;
    }

    agentRolePlan = prepareAgentRolePlan();
    std::map<std::string, std::pair<std::string, std::string>> inheritRoleMap;
    if (_needDynamicMapping) {
        extractInheritRoleMap(agentRolePlan, inheritRoleMap);
    }

    // static scheduler & inherit already allocated dynamic mapping agent roles
    std::map<size_t, std::vector<std::string>> needDynamicScheduleRoles;
    for (auto item : appPlan->rolePlans) {
        const std::string& roleName = item.first;
        size_t idx;
        if (!_agentGroupConfig->match(roleName, idx)) {
            continue;
        }

        if (idx < _candidateCntVec.size()) {
            _candidateCntVec[idx]++;
        }
        std::string agentRoleName;
        if (staticScheduleTargetRole(appPlan, agentRolePlan, roleName, idx, agentRoleName) ||
            inheritFromDynamicAgentNode(scheduler, inheritRoleMap, appPlan, agentRolePlan, roleName, idx,
                                        agentRoleName)) {
            auto iter = agentRolePlan->find(agentRoleName);
            assert(iter != agentRolePlan->end());
            iter->second->targetRoles.insert(roleName);
            if (idx < _roleMatchCntVec.size()) {
                _roleMatchCntVec[idx]++;
            }
            continue;
        }
        const auto& config = (*_agentGroupConfig)[idx];
        if (config.dynamicRoleMapping) {
            needDynamicScheduleRoles[idx].push_back(roleName);
        }
    }

    // re-schedule for dynamic mapping
    executeDynamicSchedule(scheduler, appPlan, agentRolePlan, needDynamicScheduleRoles);
    // filter useless agent roles
    roleTargetMap = finalizeAgentRolePlan(scheduler, agentRolePlan);
    return std::make_pair(agentRolePlan, roleTargetMap);
}

AgentRoleInfoMapPtr AgentRolePlanMaker::prepareAgentRolePlan() const
{
    AgentRoleInfoMapPtr agentRolePlan(new AgentRoleInfoMap);
    for (auto [role, pInfo] : _protoTypeMap) {
        AgentRoleInfoPtr info = std::make_shared<AgentRoleInfo>();
        *info = *pInfo;
        agentRolePlan->insert(std::make_pair(role, info));
    }
    return agentRolePlan;
}

bool AgentRolePlanMaker::staticScheduleTargetRole(const AppPlanPtr& appPlan, const AgentRoleInfoMapPtr& agentRolePlan,
                                                  const std::string& targetRoleName, size_t groupIdx,
                                                  std::string& agentRoleName) const
{
    const auto& agentConfig = (*_agentGroupConfig)[groupIdx];
    // static schedule by range
    if (agentConfig.dynamicRoleMapping) {
        return false;
    }
    size_t cursor;
    if (!calculateAgentNodeIndex(_agentRoleRanges[groupIdx], targetRoleName, cursor)) {
        return false;
    }
    agentRoleName = _agentRoleNameGroups[groupIdx][cursor];
    return checkAgentResourceLimit(appPlan, agentRolePlan, agentRoleName, targetRoleName, agentConfig);
}

bool AgentRolePlanMaker::inheritFromDynamicAgentNode(
    AgentSimpleMasterScheduler* scheduler,
    const std::map<std::string, std::pair<std::string, std::string>>& inheritRoleMap, const AppPlanPtr& appPlan,
    const AgentRoleInfoMapPtr& agentRolePlan, const std::string& targetRoleName, size_t groupIdx,
    std::string& agentRoleName) const
{
    const auto& agentConfig = (*_agentGroupConfig)[groupIdx];
    // try inherit from current agent node for dynamic scheduler
    if (!agentConfig.dynamicRoleMapping || scheduler == nullptr) {
        return false;
    }

    auto iter = inheritRoleMap.find(targetRoleName);
    if (iter == inheritRoleMap.end()) {
        return false;
    }
    std::string lastAgentIdentifier;
    std::tie(agentRoleName, lastAgentIdentifier) = iter->second;
    if (scheduler->isInBlackList(targetRoleName, agentRoleName)) {
        BS_LOG(WARN, "inherit agent role [%s] for target role [%s] fail, agent is in black list.",
               agentRoleName.c_str(), targetRoleName.c_str());
        return false;
    }
    auto it = agentRolePlan->find(agentRoleName);
    assert(it != agentRolePlan->end());
    if (it->second->agentConfigGroupIdx != groupIdx) {
        BS_LOG(ERROR, "role [%s] inherit from legacy agent [%s], match latest agent config id [%lu].",
               targetRoleName.c_str(), agentRoleName.c_str(), groupIdx);
        return false;
    }
    auto [synced, currentIdentifier] = scheduler->getAgentIdentifier(agentRoleName);
    if (synced && currentIdentifier.empty() && !lastAgentIdentifier.empty()) {
        // maybe reclaiming
        BS_LOG(INFO, "role [%s] will rescheduler since agent identifier change from [%s] to empty",
               targetRoleName.c_str(), lastAgentIdentifier.c_str());
        return false;
    }
    if (synced && !currentIdentifier.empty() && !lastAgentIdentifier.empty() &&
        currentIdentifier != lastAgentIdentifier) {
        // identifier changed
        BS_LOG(INFO, "role [%s] will rescheduler since agent identifier change from [%s] to [%s]",
               targetRoleName.c_str(), lastAgentIdentifier.c_str(), currentIdentifier.c_str());
        return false;
    }
    return checkAgentResourceLimit(appPlan, agentRolePlan, agentRoleName, targetRoleName, agentConfig);
}

namespace {
struct AgentScheduleInfo {
    std::string agentRole;
    size_t targetRoleCnt = 0;
    size_t agentRoleIdx = std::numeric_limits<size_t>::max();
    int64_t inBlackListTimestamp = 0;
    bool alreadyAllocated = false;
    bool serviceReady = false;
};

class AgentScheduleInfoCmp
{
public:
    bool operator()(const AgentScheduleInfo& lft, const AgentScheduleInfo& rht)
    {
        if (lft.alreadyAllocated != rht.alreadyAllocated) {
            return !lft.alreadyAllocated;
        }
        if (lft.serviceReady != rht.serviceReady) {
            return !lft.serviceReady;
        }
        if (lft.inBlackListTimestamp != rht.inBlackListTimestamp) {
            return lft.inBlackListTimestamp > rht.inBlackListTimestamp;
        }
        if (lft.targetRoleCnt != rht.targetRoleCnt) {
            return lft.targetRoleCnt > rht.targetRoleCnt;
        }
        if (lft.agentRoleIdx != rht.agentRoleIdx) {
            return lft.agentRoleIdx > rht.agentRoleIdx;
        }
        return lft.agentRole > rht.agentRole;
    }
};
}; // namespace

void AgentRolePlanMaker::executeDynamicSchedule(
    AgentSimpleMasterScheduler* scheduler, const AppPlanPtr& appPlan, const AgentRoleInfoMapPtr& agentRolePlan,
    const std::map<size_t, std::vector<std::string>>& needDynamicScheduleRoles) const
{
    if (scheduler == nullptr || needDynamicScheduleRoles.empty()) {
        return;
    }

    std::shared_ptr<AgentRoleTargetMap> oldRoleTargetMap;
    if (_getAgentHistoryFunc) {
        oldRoleTargetMap = _getAgentHistoryFunc();
    }
    for (auto it = needDynamicScheduleRoles.begin(); it != needDynamicScheduleRoles.end(); it++) {
        auto& config = (*_agentGroupConfig)[it->first];
        assert(config.dynamicRoleMapping);
        std::priority_queue<AgentScheduleInfo, std::vector<AgentScheduleInfo>, AgentScheduleInfoCmp> heap;
        for (auto& agentRole : _agentRoleNameGroups[it->first]) {
            auto iter = agentRolePlan->find(agentRole);
            assert(iter != agentRolePlan->end());
            AgentScheduleInfo info;
            info.alreadyAllocated = oldRoleTargetMap && oldRoleTargetMap->find(agentRole) != oldRoleTargetMap->end();
            info.serviceReady = scheduler->isAgentServiceReady(agentRole);
            info.agentRole = agentRole;
            info.targetRoleCnt = iter->second->targetRoles.size();
            info.agentRoleIdx = iter->second->agentRoleNodeIdx;
            scheduler->getAgentNodeInBlackListTimestamp(agentRole, info.inBlackListTimestamp);
            heap.push(info);
        }
        for (auto& role : it->second) {
            auto tmpIter = _coolingDownRoles.find(role);
            if (tmpIter != _coolingDownRoles.end()) {
                int64_t currentTime = autil::TimeUtility::currentTimeInSeconds();
                if ((currentTime - tmpIter->second) < config.inBlackListTimeout) {
                    BS_INTERVAL_LOG(300, INFO,
                                    "dynamic schedule target role [%s] is cooling down, "
                                    "current cooling time[%ld], target[%ld].",
                                    role.c_str(), (currentTime - tmpIter->second), config.inBlackListTimeout);
                    continue;
                }
                // all current agent retired (by config timeout) from blacklist will trigger re-schedule
                BS_LOG(INFO, "cooling down target role [%s] will try dynamic schedule again.", role.c_str());
                _coolingDownRoles.erase(role);
            }

            std::set<std::string> inBlackListAgentRoles;
            scheduler->getInBlackListAgentRoles(role, inBlackListAgentRoles);
            std::vector<AgentScheduleInfo> filterAgents;
            while (!heap.empty()) {
                auto candidate = heap.top();
                heap.pop();
                if (inBlackListAgentRoles.find(candidate.agentRole) == inBlackListAgentRoles.end() &&
                    checkAgentResourceLimit(appPlan, agentRolePlan, candidate.agentRole, role, config)) {
                    auto iter = agentRolePlan->find(candidate.agentRole);
                    assert(iter != agentRolePlan->end());
                    BS_LOG(INFO, "dynamic schedule target role [%s] to agent node [%s]", role.c_str(),
                           candidate.agentRole.c_str());
                    iter->second->targetRoles.insert(role);
                    if (it->first < _roleMatchCntVec.size()) {
                        _roleMatchCntVec[it->first]++;
                    }
                    candidate.targetRoleCnt++;
                    heap.push(candidate);
                    break;
                }
                filterAgents.emplace_back(candidate);
            }
            if (heap.empty()) {
                _coolingDownRoles[role] = autil::TimeUtility::currentTimeInSeconds();
                BS_LOG(INFO, "dynamic schedule target role [%s], no matched agent nodes", role.c_str());
            }
            for (auto& item : filterAgents) {
                heap.push(item);
            }
        }
    }
}

std::shared_ptr<AgentRolePlanMaker::AgentRoleTargetMap>
AgentRolePlanMaker::finalizeAgentRolePlan(AgentSimpleMasterScheduler* scheduler,
                                          const AgentRoleInfoMapPtr& agentRolePlan) const
{
    assert(scheduler);
    std::shared_ptr<AgentRoleTargetMap> oldRoleTargetMap;
    if (_getAgentHistoryFunc) {
        oldRoleTargetMap = _getAgentHistoryFunc();
    }

    map<size_t, vector<AgentRoleInfoPtr>> idleAgentInfoMap;
    for (auto iter = agentRolePlan->begin(); iter != agentRolePlan->end(); iter++) {
        if (!iter->second->targetRoles.empty()) {
            continue;
        }
        size_t idx = iter->second->agentConfigGroupIdx;
        if (idx < _idleAgentCntVec.size()) {
            _idleAgentCntVec[idx]++;
        }
        auto& config = (*_agentGroupConfig)[idx];
        if (config.lazyAllocate || config.flexibleIdleAgentCount != 0) {
            idleAgentInfoMap[idx].push_back(iter->second);
        }
    }
    unordered_set<string> toRemoveIdleAgents;
    for (auto iter = idleAgentInfoMap.begin(); iter != idleAgentInfoMap.end(); iter++) {
        size_t idx = iter->first;
        auto& config = (*_agentGroupConfig)[idx];
        auto& idleAgentVec = iter->second;
        if (config.flexibleIdleAgentCount != 0 && idleAgentVec.size() > config.flexibleIdleAgentCount) {
            std::sort(idleAgentVec.begin(), idleAgentVec.end(),
                      [](const AgentRoleInfoPtr& lft, const AgentRoleInfoPtr& rht) {
                          return lft->agentRoleNodeIdx < rht->agentRoleNodeIdx;
                      });
        }
        for (size_t i = config.flexibleIdleAgentCount; i < idleAgentVec.size(); i++) {
            size_t agentRoleIdx = idleAgentVec[i]->agentRoleNodeIdx;
            const auto& agentRoleName = _agentRoleNameGroups[idx][agentRoleIdx];
            if (config.flexibleIdleAgentCount != 0) {
                toRemoveIdleAgents.insert(agentRoleName);
            } else {
                assert(config.lazyAllocate);
                bool alreadyAllocated =
                    (oldRoleTargetMap && oldRoleTargetMap->find(agentRoleName) != oldRoleTargetMap->end());
                if (!alreadyAllocated) {
                    toRemoveIdleAgents.insert(agentRoleName);
                }
            }
        }
    }
    std::shared_ptr<AgentRoleTargetMap> roleTargetMap(new AgentRoleTargetMap);
    for (auto iter = agentRolePlan->begin(); iter != agentRolePlan->end();) {
        auto& agentRoleName = iter->first;
        size_t idx = iter->second->agentConfigGroupIdx;
        if (toRemoveIdleAgents.find(agentRoleName) != toRemoveIdleAgents.end()) {
            agentRolePlan->erase(iter++);
        } else {
            auto [agentReady, agentIdentifier] = scheduler->getAgentIdentifier(agentRoleName);
            auto& record = (*roleTargetMap)[agentRoleName];
            record.first = agentIdentifier;
            record.second.assign(iter->second->targetRoles.begin(), iter->second->targetRoles.end());
            if (idx < _assignAgentCntVec.size()) {
                _assignAgentCntVec[idx]++;
            }
            if (!agentIdentifier.empty() && idx < _readyAgentCntVec.size()) {
                _readyAgentCntVec[idx]++;
            }
            iter++;
        }
    }
    return roleTargetMap;
}

void AgentRolePlanMaker::extractInheritRoleMap(
    const std::shared_ptr<AgentRoleInfoMap>& agentRolePlan,
    std::map<std::string, std::pair<std::string, std::string>>& inheritRoleMap) const
{
    std::shared_ptr<AgentRoleTargetMap> oldRoleTargetMap;
    if (_getAgentHistoryFunc) {
        oldRoleTargetMap = _getAgentHistoryFunc();
    }
    if (!oldRoleTargetMap) {
        return;
    }
    for (auto iter = oldRoleTargetMap->begin(); iter != oldRoleTargetMap->end(); iter++) {
        auto& agentRole = iter->first;
        if (agentRolePlan->find(agentRole) == agentRolePlan->end()) {
            BS_LOG(DEBUG, "agent role [%s] is useless in latest agent group config!", agentRole.c_str());
            continue;
        }
        auto [identifier, items] = iter->second;
        for (auto& item : items) {
            inheritRoleMap[item] = make_pair(agentRole, identifier);
        }
    }
}

void AgentRolePlanMaker::rewriteAgentRoleArguments(const std::string& roleName, RolePlan& rolePlan) const
{
    assert(rolePlan.processInfos.size() >= 1);
    assert(rolePlan.processInfos[0].cmd == "build_service_worker");
    rolePlan.processInfos[0].args.push_back(make_pair("-w", "${HIPPO_PROC_WORKDIR}"));
    rolePlan.processInfos[0].args.push_back(make_pair("-s", _buildId.appname()));
    rolePlan.processInfos[0].args.push_back(make_pair("-m", _adminServiceName));
    rolePlan.processInfos[0].args.push_back(make_pair("-z", _zkRoot));
    rolePlan.processInfos[0].args.push_back(make_pair("-r", roleName));
    rolePlan.processInfos[0].args.push_back(make_pair("-a", _amonitorPort));
    rolePlan.properties["useSpecifiedRestartCountLimit"] = "yes";
}

bool AgentRolePlanMaker::calculateAgentNodeIndex(const autil::RangeVec& ranges, const std::string& roleName,
                                                 size_t& index) const
{
    assert(!ranges.empty());
    if (ranges.size() == 1) {
        index = 0;
        return true;
    }
    proto::PartitionId pid;
    if (!proto::ProtoUtil::roleIdToPartitionId(roleName, _buildId.appname(), pid)) {
        BS_LOG(ERROR, "parse roleName [%s] to partitionId fail, buildId [%s]", roleName.c_str(),
               _buildId.ShortDebugString().c_str());
        return false;
    }
    index = calculateAgentNodeIndex(ranges, roleName, pid.range().from(), pid.range().to());
    return true;
}

size_t AgentRolePlanMaker::calculateAgentNodeIndex(const autil::RangeVec& ranges, const string& roleName, uint32_t from,
                                                   uint32_t to)
{
    assert(from >= 0 && to <= autil::RangeUtil::MAX_PARTITION_RANGE && from <= to);
    autil::PartitionRange range;
    range.first = from;
    range.second = to;
    auto iter = std::upper_bound(ranges.begin(), ranges.end(), range,
                                 [](auto& lft, auto& rht) { return lft.first < rht.first; });
    if (iter == ranges.end()) {
        return ranges.size() - 1;
    }
    size_t idx = std::distance(ranges.begin(), iter);
    assert(idx > 0 && idx < ranges.size());

    size_t maxOverlap = 0;
    size_t cursor = idx - 1; // change to index which first less than from
    vector<size_t> ret;
    ret.push_back(cursor);
    for (; cursor < ranges.size(); cursor++) {
        if (ranges[cursor].first > to) {
            break;
        }
        size_t overlap = min(ranges[cursor].second, range.second) - max(ranges[cursor].first, range.first) + 1;
        if (overlap < maxOverlap) {
            continue;
        }
        if (overlap > maxOverlap) {
            maxOverlap = overlap;
            ret.clear();
            ret.push_back(cursor);
            continue;
        }
        assert(overlap == maxOverlap);
        ret.push_back(cursor);
    }
    assert(!ret.empty());
    size_t randomCursor = roleName.length() % ret.size();
    return ret[randomCursor];
}

bool AgentRolePlanMaker::checkAgentResourceLimit(const AppPlanPtr& appPlan, const AgentRoleInfoMapPtr& agentRolePlan,
                                                 const string& agentRoleName, const string& targetRoleName,
                                                 const AgentConfig& config)
{
    if (!config.exclusive && config.resourceLimitParams.empty()) { // no resource limits
        return true;
    }
    assert(agentRolePlan);
    auto it = agentRolePlan->find(agentRoleName);
    assert(it != agentRolePlan->end());
    auto& agentPlan = it->second->plan;
    auto& roleNames = it->second->targetRoles;
    if (config.exclusive && !roleNames.empty()) {
        BS_INTERVAL_LOG(100, INFO, "agent role [%s] will ignore target role [%s] for agent is exclusive.",
                        agentRoleName.c_str(), targetRoleName.c_str());
        return false;
    }
    for (auto& param : config.resourceLimitParams) {
        int32_t amountQuota = (int32_t)(getResourceAmount(agentPlan, param.resourceName) * param.oversellFactor);
        int32_t expectTotalAmount = 0;
        for (auto& role : roleNames) {
            auto tmpIter = appPlan->rolePlans.find(role);
            assert(tmpIter != appPlan->rolePlans.end());
            expectTotalAmount += getResourceAmount(tmpIter->second, param.resourceName);
        }

        auto tmpIter = appPlan->rolePlans.find(targetRoleName);
        assert(tmpIter != appPlan->rolePlans.end());
        expectTotalAmount += getResourceAmount(tmpIter->second, param.resourceName);
        if (expectTotalAmount > amountQuota) {
            BS_INTERVAL_LOG(100, INFO, "agent role [%s] will ignore target role [%s] for over resource [%s] limits.",
                            agentRoleName.c_str(), targetRoleName.c_str(), param.resourceName.c_str());
            return false;
        }
    }
    return true;
}

size_t AgentRolePlanMaker::getResourceAmount(const RolePlan& plan, const string& name)
{
    for (auto& resource : plan.slotResources) {
        auto resPtr = resource.find(name);
        if (resPtr != nullptr) {
            return resPtr->amount;
        }
    }
    return 0;
};

bool AgentRolePlanMaker::allowAssignedToGlobalAgentNodes() const
{
    if (_agentGroupConfig == nullptr) {
        return false;
    }
    return _agentGroupConfig->enableGlobalAgentNodes();
}

const std::string& AgentRolePlanMaker::getTargetGlobalAgentGroupId() const
{
    if (_agentGroupConfig == nullptr) {
        static std::string emptyStr;
        return emptyStr;
    }
    return _agentGroupConfig->getTargetGlobalAgentGroupId();
}

void AgentRolePlanMaker::getScheduleInfo(AgentRolePlanMaker::AgentGroupScheduleInfo& info) const
{
    if (_agentGroupConfig == nullptr || _agentGroupConfig->size() != _candidateCntVec.size() ||
        _agentGroupConfig->size() != _roleMatchCntVec.size() || _agentGroupConfig->size() != _idleAgentCntVec.size() ||
        _agentGroupConfig->size() != _readyAgentCntVec.size() ||
        _agentGroupConfig->size() != _assignAgentCntVec.size()) {
        return;
    }

    std::string keyPrefix = _isGlobalAgent
                                ? _buildId.datatable()
                                : _buildId.appname() + "." + autil::StringUtil::toString(_buildId.generationid()) +
                                      "." + _buildId.datatable();
    for (size_t i = 0; i < _agentGroupConfig->size(); i++) {
        std::string key = keyPrefix + "." + (*_agentGroupConfig)[i].identifier;
        AgentGroupMetricData& metricData = info[key];
        metricData.buildId = _buildId;
        metricData.groupId = (*_agentGroupConfig)[i].identifier;
        metricData.totalAgentNodeCnt = (*_agentGroupConfig)[i].agentNodeCount;
        metricData.assignAgentNodeCnt = _assignAgentCntVec[i];
        metricData.idleAgentNodeCnt = _idleAgentCntVec[i];
        metricData.readyAgentNodeCnt = _readyAgentCntVec[i];
        metricData.totalRoleCnt = _candidateCntVec[i];
        metricData.assignRoleCnt = _roleMatchCntVec[i];
        metricData.isGlobalAgent = _isGlobalAgent;
        metricData.isDynamicMapping = (*_agentGroupConfig)[i].dynamicRoleMapping;
    }
}

}} // namespace build_service::admin
