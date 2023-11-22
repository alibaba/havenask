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
#include "build_service/admin/AppPlanMaker.h"

#include <assert.h>
#include <ext/alloc_traits.h>
#include <ostream>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/RangeUtil.h"
#include "autil/Regex.h"
#include "autil/Span.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/admin/WorkerTable.h"
#include "build_service/proto/RoleNameGenerator.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/ErrorLogCollector.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace build_service::util;
using namespace build_service::proto;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, AppPlanMaker);

const string AppPlanMaker::BUILD_SERVICE_WORKER_NAME = "build_service_worker";
const string AppPlanMaker::ROLE_PATTERN = "_bs_role";

AppPlanMaker::AppPlanMaker(const string& appName, const string& heartbeatType)
    : _heartbeatType(heartbeatType)
    , _appName(appName)
    , _enableGangRegion(false)
{
}

AppPlanMaker::~AppPlanMaker() {}

GroupRole AppPlanMaker::createGroupRole(WorkerNodeBasePtr node, bool enableGangRegion)
{
    const PartitionId& partId = node->getPartitionId();
    string roleName = RoleNameGenerator::generateRoleName(partId);
    GroupRole role(roleName, RoleNameGenerator::generateGroupName(partId), node->getRoleType());
    if (enableGangRegion) {
        role.gangRegionId = RoleNameGenerator::generateGangIdentifier(partId);
    }
    if (partId.step() == BUILD_STEP_INC) {
        if ((role.type == ROLE_BUILDER || role.type == ROLE_MERGER || role.type == ROLE_TASK) &&
            node->isHighQuality()) {
            role.isHighQuality = true;
        }
    } else if (partId.step() == BUILD_STEP_FULL) {
        if ((role.type == ROLE_BUILDER || role.type == ROLE_MERGER || role.type == ROLE_PROCESSOR ||
             role.type == ROLE_TASK) &&
            node->isHighQuality()) {
            role.isHighQuality = true;
        }
    }

    return role;
}

bool AppPlanMaker::loadConfig(const string& configFile)
{
    string fileContent;
    if (!fslib::util::FileUtil::readFile(configFile, fileContent)) {
        string errorMsg = "Init AppPlanMaker failed : Read file[" + configFile + "] failed.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    try {
        FromJsonString(*this, fileContent);
    } catch (const autil::legacy::ExceptionBase& e) {
        stringstream ss;
        ss << "Invalid json file[" << configFile << "], content[" << fileContent << "], exception[" << string(e.what())
           << "]";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    std::shared_ptr<GroupRolePairs> candidatesPtr;
    _candidateGuard.set(candidatesPtr);
    cleanCache();
    return true;
}

void AppPlanMaker::resetWorkerPackageList(const std::vector<hippo::PackageInfo>& packageList)
{
    _packageInfos = packageList;
}
void AppPlanMaker::cleanCache()
{
    autil::ScopedLock lock(_cacheMutex);
    _rolePlanCache.clear();
    _agentRolePlanCache.clear();
}

bool AppPlanMaker::isWorkerTableUpdated(const WorkerTable* workerTable) const
{
    struct CandidateChecker {
        CandidateChecker(const GroupRolePairs& candidates, bool enableGangRegion)
            : _isUpdated(false)
            , _enableGangRegion(enableGangRegion)
            , _candidates(candidates)
            , _cursor(0)
        {
        }
        ~CandidateChecker() {}
        bool operator()(WorkerNodeBasePtr node)
        {
            assert(node);
            if (_cursor == _candidates.size()) {
                _isUpdated = true;
                return false;
            }
            GroupRole role = AppPlanMaker::createGroupRole(node, _enableGangRegion);
            string roleName = role.roleName;
            auto iter = _candidates.find(roleName);
            if (iter == _candidates.end()) {
                _isUpdated = true;
                return false;
            }
            if (role != iter->second) {
                _isUpdated = true;
                return false;
            }
            _cursor++;
            return true;
        }
        bool isCandidateUpdated() const
        {
            if (_cursor != _candidates.size()) {
                return true;
            }
            return _isUpdated;
        }
        bool _isUpdated;
        bool _enableGangRegion;
        const GroupRolePairs& _candidates;
        size_t _cursor;
    };
    std::shared_ptr<GroupRolePairs> candidatesPtr;
    _candidateGuard.get(candidatesPtr);
    if (!candidatesPtr) {
        return true;
    }
    CandidateChecker checker(*candidatesPtr, _enableGangRegion);
    workerTable->forEachActiveNode(checker);
    return checker.isCandidateUpdated();
}

void AppPlanMaker::prepare(const WorkerTable* workerTable)
{
    assert(workerTable);
    if (!workerTable) {
        return;
    }
    if (!isWorkerTableUpdated(workerTable)) {
        return;
    }

    struct CandidateGenerator {
        CandidateGenerator(bool enableGang) : _enableGangRegion(enableGang) {}
        ~CandidateGenerator() {}
        bool operator()(WorkerNodeBasePtr node)
        {
            GroupRole role = AppPlanMaker::createGroupRole(node, _enableGangRegion);
            candidates.insert(make_pair(role.roleName, role));
            return true;
        }
        GroupRolePairs candidates;

    private:
        bool _enableGangRegion;
    };
    CandidateGenerator generator(_enableGangRegion);
    workerTable->forEachActiveNode(generator);
    std::shared_ptr<GroupRolePairs> candidatesPtr;
    candidatesPtr.reset(new GroupRolePairs);
    candidatesPtr->swap(generator.candidates);
    _candidateGuard.set(candidatesPtr);
}

AppPlanPtr AppPlanMaker::makeAppPlan()
{
    std::shared_ptr<GroupRolePairs> candidatesPtr;
    _candidateGuard.get(candidatesPtr);

    if (!candidatesPtr) {
        return AppPlanPtr();
    }
    return makeAppPlan(*candidatesPtr);
}

std::shared_ptr<GroupRolePairs> AppPlanMaker::getCandidates()
{
    std::shared_ptr<GroupRolePairs> candidatesPtr;
    _candidateGuard.get(candidatesPtr);
    return candidatesPtr;
}

AppPlanPtr AppPlanMaker::makeAppPlan(const GroupRolePairs& groupRoles)
{
    AppPlanPtr plan(new AppPlan);
    MetaTagAppender appender(_appName, _enableGangRegion);
    appender.init(groupRoles);
    auto iter = groupRoles.begin();

    autil::ScopedLock lock(_cacheMutex);
    while (iter != groupRoles.end()) {
        const string& roleName = iter->first;
        const GroupRole& groupRole = iter->second;
        const string& groupName = groupRole.groupName;
        const auto& it = _rolePlanCache.find(groupName);
        if (it != _rolePlanCache.end()) {
            RolePlan& rolePlan = it->second;
            appender.append(rolePlan, groupRole);
            RolePlan tmpRolePlan = rolePlan;
            // high quality resource is temporary, should not be in rolePlan cache.
            CheckAndSetHighQuality(groupRole, tmpRolePlan);
            plan->rolePlans.insert(make_pair(roleName, tmpRolePlan));
            ++iter;
            continue;
        }
        RolePlan rolePlan = _defaultRolePlan;
        mergeRoleResource(groupName, _groupResourceMap, rolePlan);
        mergeRoleCustomize(groupName, _customizedRolePlans, rolePlan);
        mergeRoleProcess(rolePlan, groupRole, _heartbeatType);
        checkAndRewriteRolePlan(rolePlan);
        appender.append(rolePlan, groupRole);
        _rolePlanCache.insert(make_pair(groupName, rolePlan));
        CheckAndSetHighQuality(groupRole, rolePlan);
        plan->rolePlans[roleName] = rolePlan;
        ++iter;
    }
    return plan;
}

void AppPlanMaker::mergeRoleResource(const string& groupName, const GroupResourceMap& groupResourceMap,
                                     RolePlan& rolePlan)
{
    size_t lastMatchLen = 0;
    hippo::SlotResource slotResource;
    for (GroupResourceMap::const_iterator it = groupResourceMap.begin(); it != groupResourceMap.end(); ++it) {
        if (groupName.find(it->first) == 0 && it->first.size() > lastMatchLen) {
            slotResource.resources = it->second;
            lastMatchLen = it->first.size();
        }
    }

    if (lastMatchLen > 0) {
        rolePlan.slotResources.push_back(slotResource);
    }
}

void AppPlanMaker::mergeRoleCustomize(const string& groupName, const RolePlanVector& customizedRolePlans,
                                      RolePlan& rolePlan)
{
    size_t i = 0;
    for (; i < customizedRolePlans.size(); i++) {
        string rolePattern;
        JsonMap::const_iterator it = customizedRolePlans[i].find(ROLE_PATTERN);
        if (customizedRolePlans[i].end() == it) {
            continue;
        }
        try {
            FromJson(rolePattern, it->second);
            if (Regex::match(groupName, rolePattern)) {
                mergeRolePlan(rolePlan, customizedRolePlans[i]);
                break;
            }
        } catch (const autil::legacy::ExceptionBase& e) {
            string errorMsg = "invalid role pattern, exception[" + string(e.what()) + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            continue;
        }
    }
}

void AppPlanMaker::mergeRoleProcess(RolePlan& rolePlan, const GroupRole& groupRole, const string& heartbeatType) const
{
    assert(rolePlan.processInfos.size() >= 1 && "process info for admin worker.");
    rolePlan.packageInfos = _packageInfos;
    rolePlan.count = 1;
    rolePlan.processInfos[0].isDaemon = true;
    rolePlan.processInfos[0].cmd = BUILD_SERVICE_WORKER_NAME;
    rolePlan.processInfos[0].name = BUILD_SERVICE_WORKER_NAME;
    rolePlan.processInfos[0].args = argFilter(rolePlan.processInfos[0].args);
    string leaderLeaseTimeout;
    if (EnvUtil::getEnvWithoutDefault("leader_lease_timeout", leaderLeaseTimeout)) {
        rolePlan.processInfos[0].args.push_back({"--leader_lease_timeout", leaderLeaseTimeout});
    }
    string leaderSyncInterval;
    if (EnvUtil::getEnvWithoutDefault("leader_sync_interval", leaderSyncInterval)) {
        rolePlan.processInfos[0].args.push_back({"--leader_sync_interval", leaderSyncInterval});
    }
    if (heartbeatType == "rpc") {
        rolePlan.processInfos[0].args.push_back({"-d", heartbeatType});
    }
    setEnvIfNotExist(rolePlan, "WF_PREEMPTIVE_LEADER", "true");
}

void AppPlanMaker::CheckAndSetHighQuality(const GroupRole& role, RolePlan& plan)
{
    if (role.isHighQuality) {
        for (auto& slotResource : plan.slotResources) {
            if (NULL == slotResource.find("HighQuality")) {
                hippo::Resource resource("HighQuality", 0, hippo::Resource::PREFER_TEXT);
                slotResource.add(resource);
            }
        }
    }
}

// cpu & mem are required, is empty, will use 2core & 5G
void AppPlanMaker::checkAndRewriteRolePlan(RolePlan& plan)
{
    if (plan.slotResources.size() == 0) {
        hippo::SlotResource emptyResource;
        plan.slotResources.push_back(emptyResource);
    }
    for (auto& slotResource : plan.slotResources) {
        if (NULL == slotResource.find("cpu")) {
            hippo::Resource resource("cpu", 200);
            slotResource.add(resource);
        }
        if (NULL == slotResource.find("mem")) {
            hippo::Resource resource("mem", 5120);
            slotResource.add(resource);
        }
    }

    auto slotResource = plan.slotResources[0];
    auto* cpuResource = slotResource.find("cpu");
    assert(cpuResource);
    setEnvIfNotExist(plan, "BS_ROLE_CPU_QUOTA", std::to_string(cpuResource->amount));
    auto* memResource = slotResource.find("mem");
    assert(memResource);
    setEnvIfNotExist(plan, "BS_ROLE_MEM_QUOTA", std::to_string(memResource->amount));
}

void AppPlanMaker::setEnvIfNotExist(RolePlan& rolePlan, const std::string& envKey, const std::string& envValue)
{
    for (auto& envPair : rolePlan.processInfos[0].envs) {
        if (envPair.first == envKey) {
            return;
        }
    }
    rolePlan.processInfos[0].envs.push_back(make_pair(envKey, envValue));
    return;
}

void AppPlanMaker::Jsonize(JsonWrapper& json)
{
    json.Jsonize("packages", _packageInfos, _packageInfos);
    json.Jsonize("role_default", _defaultRolePlan, _defaultRolePlan);
    json.Jsonize("role_customize", _customizedRolePlans, _customizedRolePlans);
    json.Jsonize("role_resource", _groupResourceMap, _groupResourceMap);
    json.Jsonize("enable_gang_region", _enableGangRegion, _enableGangRegion);
}

void AppPlanMaker::mergeRolePlan(RolePlan& rolePlan, const JsonMap& other)
{
    Any any = ToJson(rolePlan);
    JsonMap& jsonMap = *AnyCast<JsonMap>(&any);
    for (JsonMap::const_iterator it = other.begin(); it != other.end(); ++it) {
        jsonMap[it->first] = it->second;
    }
    FromJson(rolePlan, (Any)jsonMap);
}

bool AppPlanMaker::validate(const vector<string>& clusters, const map<string, vector<string>>& mergeNames,
                            const vector<string>& dataTables) const
{
    if (_defaultRolePlan.processInfos.empty()) {
        string errorMsg = "processInfo is empty";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    vector<string> allRoles = getAllRoles(clusters, mergeNames, dataTables);
    for (size_t i = 0; i < allRoles.size(); i++) {
        RolePlan rolePlan;
        mergeRoleResource(allRoles[i], _groupResourceMap, rolePlan);
        mergeRoleCustomize(allRoles[i], _customizedRolePlans, rolePlan);
        if (rolePlan.slotResources.empty()) {
            string errorMsg = "slot resource not set for role[" + allRoles[i] + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }
    return true;
}

void AppPlanMaker::prepareGroupRolePairs(GroupRolePairs& groupRoles)
{
    std::shared_ptr<GroupRolePairs> candidates(new GroupRolePairs);
    candidates->swap(groupRoles);
    _candidateGuard.set(candidates);
}

vector<string> AppPlanMaker::getAllRoles(const vector<string>& clusters, const map<string, vector<string>>& mergeNames,
                                         const vector<string>& dataTables)
{
    vector<string> allRoles;
    for (size_t i = 0; i < dataTables.size(); i++) {
        allRoles.push_back("processor." + dataTables[i] + ".full");
        allRoles.push_back("processor." + dataTables[i] + ".inc");
    }
    for (size_t i = 0; i < clusters.size(); i++) {
        allRoles.push_back("builder." + clusters[i] + ".full");
        allRoles.push_back("builder." + clusters[i] + ".inc");
    }
    for (const auto& [clusterName, clusterMergeNames] : mergeNames) {
        for (size_t i = 0; i < clusterMergeNames.size(); i++) {
            allRoles.push_back("merger." + clusterName + "." + clusterMergeNames[i]);
        }
    }
    return allRoles;
}

vector<PairType> AppPlanMaker::argFilter(const vector<PairType>& args)
{
    vector<PairType> resultArgs;
    for (size_t i = 0; i < args.size(); ++i) {
        if (args[i].first == "-l") {
            resultArgs.push_back(args[i]);
        }
        if (args[i].first == "--amonitor_service_name") {
            resultArgs.push_back(args[i]);
        }
        if (args[i].first == "--beeper_config_path") {
            resultArgs.push_back(args[i]);
        }
    }
    return resultArgs;
}

std::pair<std::string, RolePlan> AppPlanMaker::createAgentRolePlan(const proto::BuildId& id,
                                                                   const std::string& configName, size_t nodeCount,
                                                                   size_t idx, bool useDynamicMapping)
{
    if (useDynamicMapping) {
        // rewrite nodeCount = 65536, will make range.from == range.to == idx
        nodeCount = autil::RangeUtil::MAX_PARTITION_RANGE + 1;
    }
    proto::PartitionId pid;
    proto::RoleNameGenerator::generateAgentNodePartitionId(id, _appName, configName, nodeCount, idx, pid);
    string roleId = proto::RoleNameGenerator::generateRoleName(pid);

    autil::ScopedLock lock(_cacheMutex);
    auto iter = _agentRolePlanCache.find(roleId);
    if (iter != _agentRolePlanCache.end()) {
        return {roleId, iter->second};
    }

    string groupName = proto::RoleNameGenerator::generateGroupName(pid);
    RolePlan rolePlan = makeRoleForAgent(roleId, groupName);
    auto ret = make_pair(roleId, rolePlan);
    _agentRolePlanCache.insert(ret);
    return ret;
}

RolePlan AppPlanMaker::makeRoleForAgent(const std::string& roleName, const std::string& groupName)
{
    RolePlan rolePlan = _defaultRolePlan;
    mergeRoleResource(groupName, _groupResourceMap, rolePlan);
    mergeRoleCustomize(groupName, _customizedRolePlans, rolePlan);

    GroupRole groupRole(roleName, groupName, ROLE_AGENT);
    mergeRoleProcess(rolePlan, groupRole, "rpc");
    checkAndRewriteRolePlan(rolePlan);
    return rolePlan;
}

}} // namespace build_service::admin
