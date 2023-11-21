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
#include "build_service/admin/AgentSimpleMasterScheduler.h"

#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <functional>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/ZlibCompressor.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/admin/HippoProtoHelper.h"
#include "build_service/common/PathDefine.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/proto/RoleNameGenerator.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Monitor.h"
#include "fslib/util/FileUtil.h"
#include "hippo/HippoUtil.h"
#include "hippo/proto/Common.pb.h"
#include "kmonitor/client/MetricLevel.h"
#include "master_framework/Plan.h"
#include "worker_framework/LeaderElector.h"
#include "worker_framework/WorkerState.h"

using namespace std;
using namespace build_service::common;
using namespace build_service::proto;
using namespace master_framework::master_base;
using namespace worker_framework;

namespace {
static std::string getIdentifier(const hippo::SlotInfo& slotInfo)
{
    if (slotInfo.slotId.slaveAddress.empty() || slotInfo.slotId.id == 0) {
        return "";
    }
    return WorkerNodeBase::getIdentifier(hippo::HippoUtil::getIp(slotInfo), slotInfo.slotId.id);
}

static std::string getIdentifier(const hippo::proto::AssignedSlot& slotInfo)
{
    if (slotInfo.id().slaveaddress().empty() || slotInfo.id().id() == 0) {
        return "";
    }
    return WorkerNodeBase::getIdentifier(hippo::HippoUtil::getIp(slotInfo), slotInfo.id().id());
}

} // namespace

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, AgentSimpleMasterScheduler);

AgentSimpleMasterScheduler::AgentSimpleMasterScheduler(
    const std::string& zkRoot, cm_basic::ZkWrapper* zkWrapper,
    const std::shared_ptr<master_framework::simple_master::SimpleMasterScheduler>& scheduler,
    kmonitor_adapter::Monitor* monitor)
    : _zkRoot(zkRoot)
    , _zkStatus(zkWrapper, fslib::util::FileUtil::joinFilePath(_zkRoot, PathDefine::ZK_AGENT_BLACK_LIST_FILE))
    , _workerTable(new WorkerTable(zkRoot, zkWrapper))
    , _scheduler(scheduler)
    , _lastClearTimestamp(-1)
    , _lastSyncSlotTimestamp(0)
    , _needSyncBlackList(false)
    , _totalCpuAmount(0)
    , _totalMemAmount(0)
{
    if (!_scheduler) {
        _scheduler.reset(new master_framework::simple_master::SimpleMasterScheduler);
    }

    if (monitor) {
        _setPlanLatency = monitor->registerGaugeMetric("agent.setPlanLatency", kmonitor::FATAL);
        _agentNodeCount = monitor->registerGaugeMetric("agent.agentNodeCount", kmonitor::FATAL);
        _blackListNodeCount = monitor->registerGaugeMetric("agent.blackListNodeCount", kmonitor::FATAL);
        _useAgentRoleCount = monitor->registerGaugeMetric("agent.useAgentRoleCount", kmonitor::FATAL);
        _reclaimingAgentNodeCount = monitor->registerGaugeMetric("agent.reclaimingAgentNodeCount", kmonitor::FATAL);

        _cpuAmountMetric = monitor->registerGaugeMetric("resource.allocatedCpuAmount", kmonitor::FATAL);
        _memAmountMetric = monitor->registerGaugeMetric("resource.allocatedMemAmount", kmonitor::FATAL);
    }
}

AgentSimpleMasterScheduler::~AgentSimpleMasterScheduler()
{
    _loopThread.reset();
    DELETE_AND_SET_NULL(_workerTable);
}

bool AgentSimpleMasterScheduler::init(const std::string& hippoZkRoot, worker_framework::LeaderChecker* leaderChecker,
                                      const std::string& applicationId)
{
    _appName = applicationId;
    if (_appName.empty()) {
        BS_LOG(ERROR, "invalid app name [%s]", _appName.c_str());
        return false;
    }
    if (!_workerTable->startHeartbeat("rpc")) {
        BS_LOG(ERROR, "start rpc heartbeat fail for hybrid scheduler.");
        return false;
    }

    string agentNodeRoot = PathDefine::getAgentNodeRoot(_zkRoot);
    bool dirExist = false;
    if (!fslib::util::FileUtil::isDir(agentNodeRoot, dirExist)) {
        BS_LOG(ERROR, "check whether %s is dir failed.", agentNodeRoot.c_str());
        return false;
    }
    if (!dirExist && !fslib::util::FileUtil::mkDir(agentNodeRoot)) {
        string errorMsg = "mkDir [" + agentNodeRoot + "] failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    recoverBlackListRoleMap();
    _loopThread =
        autil::LoopThread::createLoopThread(bind(&AgentSimpleMasterScheduler::workLoop, this), 1 * 1000 * 1000,
                                            /*name = */ "AgentScheduler", /*strictMode = */ true); // 1s
    if (!_loopThread) {
        return false;
    }
    return _scheduler->init(hippoZkRoot, leaderChecker, applicationId);
}

bool AgentSimpleMasterScheduler::declareAgentRole(const std::string& agentRole, const AgentRoleInfoPtr& agentInfo)
{
    if (!agentInfo) {
        BS_LOG(WARN, "invalid agent info for agent role [%s]", agentRole.c_str());
        return false;
    }
    proto::PartitionId pid;
    if (!proto::ProtoUtil::roleIdToPartitionId(agentRole, _appName, pid)) {
        BS_LOG(WARN, "invalid agent role name [%s]", agentRole.c_str());
        return false;
    }
    auto& rolePlan = agentInfo->plan;
    string heartbeatType;
    if (!rolePlan.getArg("build_service_worker", "-d", heartbeatType) || heartbeatType != "rpc") {
        BS_LOG(WARN, "invalid agent role plan [%s] for role [%s], arguments(-d rpc) is necessary.",
               autil::legacy::ToJsonString(rolePlan, true).c_str(), agentRole.c_str());
        return false;
    }

    autil::ScopedLock lock(_lock);
    auto iter = _agentRoles.find(agentRole);
    if (iter == _agentRoles.end()) {
        BS_LOG(INFO, "declare new agent role [%s]", agentRole.c_str());
        _agentRoles[agentRole] = agentInfo;
    } else {
        iter->second = agentInfo;
    }
    return true;
}

void AgentSimpleMasterScheduler::removeAgentRole(const std::string& agentRole)
{
    autil::ScopedLock lock(_lock);
    BS_LOG(INFO, "remove agent role [%s]", agentRole.c_str());
    _agentRoles.erase(agentRole);
    _inBlackListAgentInfo.erase(agentRole);
}

void AgentSimpleMasterScheduler::setAppPlan(const master_framework::simple_master::AppPlan& appPlan)
{
    kmonitor_adapter::ScopeLatencyReporter latencyReporter(_setPlanLatency);
    autil::ScopedLock lock(_lock);
    enableClearUselessAgentZkNodes();
    if (_agentRoles.empty()) {
        _targetRoleMap.clear();
        _needSyncBlackList = !_blackListRoleMap.empty();
        _blackListRoleMap.clear();
        _inBlackListAgentInfo.clear();
        _workerTable->clearAllNodes();
        _scheduler->setAppPlan(appPlan);
        return;
    }

    updateTargetRoleMap();
    master_framework::simple_master::AppPlan rewritePlan;

    // 1. update workerTable for agent nodes
    std::vector<proto::WorkerNodeBasePtr> validAgentNodes;
    std::map<std::string, std::vector<std::string>> inPlanTargetRoleMap;
    validAgentNodes.reserve(_agentRoles.size());
    for (auto& agentRole : _agentRoles) {
        assert(agentRole.second);
        auto& agentRoleName = agentRole.first;
        auto& agentRolePlan = agentRole.second->plan;
        auto& targetRoleNames = agentRole.second->targetRoles;
        rewritePlan.rolePlans[agentRoleName] = agentRolePlan;
        inPlanTargetRoleMap[agentRoleName].reserve(targetRoleNames.size());
        proto::WorkerNodes& agentNodes = _workerTable->getWorkerNodes(agentRoleName);
        if (agentNodes.empty()) {
            // add new agent node
            proto::PartitionId pid;
            proto::ProtoUtil::roleIdToPartitionId(agentRoleName, _appName, pid);
            proto::AgentNodePtr agentNode(new proto::AgentNode(pid));
            agentNodes.push_back(agentNode);
        }
        validAgentNodes.push_back(agentNodes[0]);
    }

    assert(validAgentNodes.size() <= _workerTable->getWorkerNodesMap().size());
    if (validAgentNodes.size() != _workerTable->getWorkerNodesMap().size()) {
        // remove useless agent nodes in workerTable
        _workerTable->clearAllNodes();
        for (auto& node : validAgentNodes) {
            string roleId;
            proto::ProtoUtil::partitionIdToRoleId(node->getPartitionId(), roleId);
            proto::WorkerNodes& agentNodes = _workerTable->getWorkerNodes(roleId);
            agentNodes.push_back(node);
        }
    }
    _workerTable->syncNodesStatus();

    // 2. rewrite app plan, use agent node & remove target roles for agent
    rewritePlan.packageInfos = appPlan.packageInfos;
    rewritePlan.prohibitedIps = appPlan.prohibitedIps;
    for (auto& roleInfo : appPlan.rolePlans) {
        auto iter = _targetRoleMap.find(roleInfo.first);
        if (iter == _targetRoleMap.end() || isInBlackListUnsafe(roleInfo.first, iter->second)) {
            rewritePlan.rolePlans[roleInfo.first] = roleInfo.second;
            continue;
        }

        auto& agentRolePlan = _agentRoles[iter->second]->plan;
        if (!checkPackageInfoMatch(agentRolePlan.packageInfos, roleInfo.second.packageInfos)) {
            BS_LOG(WARN,
                   "package info not match between target role [%s] and agent role [%s], "
                   "ignore using agent node.",
                   roleInfo.first.c_str(), iter->second.c_str());
            rewritePlan.rolePlans[roleInfo.first] = roleInfo.second;
            continue;
        }
        inPlanTargetRoleMap[iter->second].push_back(roleInfo.first);
    }

    // 3. generate target for agent node
    size_t useAgentRoleCount = 0;
    for (auto& agentRole : _agentRoles) {
        proto::WorkerNodeBasePtr node = _workerTable->getWorkerNodes(agentRole.first)[0];
        proto::AgentNode* agentNode = dynamic_cast<proto::AgentNode*>(node.get());
        assert(agentNode);
        proto::AgentTarget target;
        if (!agentRole.second->configPath.empty()) {
            target.set_configpath(agentRole.second->configPath);
        }

        bool isGlobalAgent = agentRole.second->isGlobalAgent;
        target.set_isglobalagent(isGlobalAgent);
        for (auto& globalConfig : agentRole.second->globalTargetConfigs) {
            target.add_globalconfigs(globalConfig);
        }
        auto iter = inPlanTargetRoleMap.find(agentRole.first);
        if (iter != inPlanTargetRoleMap.end()) {
            for (auto& roleName : iter->second) {
                auto targetRole = target.add_targetroles();
                targetRole->set_rolename(roleName);
                auto it = appPlan.rolePlans.find(roleName);
                assert(it != appPlan.rolePlans.end());
                targetRole->set_processinfo(autil::legacy::ToJsonString(it->second.processInfos, true));
                if (agentRole.second->globalTargetRoleConfigMap) {
                    auto cIter = agentRole.second->globalTargetRoleConfigMap->find(roleName);
                    if (cIter != agentRole.second->globalTargetRoleConfigMap->end()) {
                        targetRole->set_configpath(cIter->second);
                    }
                }
                ++useAgentRoleCount;
            }
        }
        agentNode->setTargetStatus(target);
    }
    _scheduler->setAppPlan(rewritePlan);
    REPORT_KMONITOR_METRIC(_agentNodeCount, _agentRoles.size());
    REPORT_KMONITOR_METRIC(_useAgentRoleCount, useAgentRoleCount);
    REPORT_KMONITOR_METRIC(_blackListNodeCount, _blackListRoleMap.size());
}

std::vector<hippo::SlotInfo> AgentSimpleMasterScheduler::getRoleSlots(const std::string& roleName)
{
    autil::ScopedLock lock(_lock);
    auto ret = _scheduler->getRoleSlots(roleName);
    if (!ret.empty()) {
        return ret;
    }

    auto iter = _targetRoleMap.find(roleName);
    if (iter == _targetRoleMap.end() || isInBlackListUnsafe(roleName, iter->second)) {
        return ret;
    }
    // get agent slot & rename
    ret = _scheduler->getRoleSlots(iter->second);
    for (auto& slot : ret) {
        slot.role = roleName;
    }
    return ret;
}

void AgentSimpleMasterScheduler::syncSlotInfosUnsafe(std::map<std::string, SlotInfos>& roleSlots)
{
    _scheduler->getAllRoleSlots(roleSlots);
    updateResourceStatistic(roleSlots);
    fillAgentSlotInfo(roleSlots);
    fillAgentIdentifier(roleSlots);
    _lastSyncSlotTimestamp = autil::TimeUtility::currentTimeInSeconds();
}

void AgentSimpleMasterScheduler::getAllRoleSlots(std::map<std::string, SlotInfos>& roleSlots)
{
    autil::ScopedLock lock(_lock);
    syncSlotInfosUnsafe(roleSlots);
    for (auto& agentRole : _agentRoles) {
        auto iter = roleSlots.find(agentRole.first);
        if (iter == roleSlots.end()) {
            continue;
        }
        // rewrite target role slots
        SlotInfos slotInfos = iter->second;
        for (auto& targetRole : agentRole.second->targetRoles) {
            if (roleSlots.find(targetRole) != roleSlots.end()) {
                continue;
            }
            if (isInBlackListUnsafe(targetRole, agentRole.first)) {
                continue;
            }
            for (auto& slot : slotInfos) {
                slot.role = targetRole;
            }
            roleSlots[targetRole] = slotInfos;
        }
    }
}

void AgentSimpleMasterScheduler::releaseSlots(const std::vector<hippo::SlotId>& slots,
                                              const hippo::ReleasePreference& pref)
{
    std::set<hippo::SlotId> dedupSlots;
    for (auto& slot : slots) {
        dedupSlots.insert(slot);
    }
    if (slots.size() == dedupSlots.size()) {
        _scheduler->releaseSlots(slots, pref);
        return;
    }
    std::vector<hippo::SlotId> toReleaseSlots(dedupSlots.begin(), dedupSlots.end());
    _scheduler->releaseSlots(toReleaseSlots, pref);
}

void AgentSimpleMasterScheduler::updateTargetRoleMap()
{
    autil::ScopedLock lock(_lock);
    std::map<std::string, std::string> targetRoleMap;
    for (auto& agentInfo : _agentRoles) {
        for (auto& targetRole : agentInfo.second->targetRoles) {
            targetRoleMap[targetRole] = agentInfo.first;
        }
    }
    _targetRoleMap.swap(targetRoleMap);
    if (refreshBlackListInfo()) {
        _needSyncBlackList = true;
        refreshInBlackListAgentInfo();
    }
}

bool AgentSimpleMasterScheduler::refreshBlackListInfo()
{
    bool change = false;
    int64_t currentTs = autil::TimeUtility::currentTimeInSeconds();
    auto iter = _blackListRoleMap.begin();
    while (iter != _blackListRoleMap.end()) {
        auto agentIter = _targetRoleMap.find(iter->first);
        if (agentIter == _targetRoleMap.end()) {
            // remove useless role in black list
            iter = _blackListRoleMap.erase(iter);
            change = true;
            continue;
        }

        auto& blackList = iter->second;
        for (auto blIter = blackList.begin(); blIter != blackList.end();) {
            int64_t inBlackListTs = blIter->timestamp;
            int64_t gap = currentTs - inBlackListTs;
            auto tmpIter = _agentRoles.find(blIter->agentRole);
            if (tmpIter == _agentRoles.end()) {
                AUTIL_LOG(INFO, "remove target role [%s] useless black agent node [%s] from blacklist",
                          iter->first.c_str(), blIter->agentRole.c_str());
                blIter = blackList.erase(blIter);
                change = true;
                continue;
            }

            if (gap >= tmpIter->second->inBlackListTimeout) {
                // remove timeout role in black list
                AUTIL_LOG(INFO,
                          "remove timeout target role [%s] black agent node [%s] from blacklist, "
                          "gap [%ld] over timeout [%ld]",
                          iter->first.c_str(), blIter->agentRole.c_str(), gap, tmpIter->second->inBlackListTimeout);
                blIter = blackList.erase(blIter);
                change = true;
                continue;
            }

            bool isReclaim = false;
            std::string currentSlotAddress;
            if (blIter->isReclaim && getAgentNodeSlotInfo(blIter->agentRole, isReclaim, currentSlotAddress) &&
                !isReclaim && !currentSlotAddress.empty() && currentSlotAddress != blIter->slotAddress) {
                // remove reclaimed role in black list when successfully changed to new slot
                AUTIL_LOG(INFO,
                          "remove target role [%s] reclaim black agent node [%s] from blacklist, "
                          "agent node change slot from [%s] to [%s]",
                          iter->first.c_str(), blIter->agentRole.c_str(), blIter->slotAddress.c_str(),
                          currentSlotAddress.c_str());
                blIter = blackList.erase(blIter);
                change = true;
                continue;
            }
            blIter++;
        }
        if (blackList.empty()) {
            iter = _blackListRoleMap.erase(iter);
            change = true;
            continue;
        }
        ++iter;
    }
    return change;
}

void AgentSimpleMasterScheduler::fillAgentIdentifier(const std::map<std::string, SlotInfos>& roleSlots)
{
    if (roleSlots.empty()) {
        return;
    }
    _agentIdentifiers.clear();
    for (auto [agentRoleName, slotInfos] : roleSlots) {
        std::string identifier;
        for (const auto& slotInfo : slotInfos) {
            if (slotInfo.reclaiming) {
                continue;
            }
            identifier = getIdentifier(slotInfo);
        }
        if (!identifier.empty()) {
            _agentIdentifiers[agentRoleName] = identifier;
        }
        AUTIL_LOG(DEBUG, "debug xiaohao agent [%s] identifier [%s]", agentRoleName.c_str(), identifier.c_str());
    }
}

void AgentSimpleMasterScheduler::fillAgentSlotInfo(const std::map<std::string, SlotInfos>& roleSlots)
{
    if (roleSlots.empty()) {
        return;
    }
    struct SetSlotInfoFunctor {
        SetSlotInfoFunctor(const std::map<std::string, SlotInfos>& roleSlots) : _roleSlots(roleSlots) {}
        ~SetSlotInfoFunctor() {}

        bool operator()(proto::WorkerNodeBasePtr node)
        {
            auto roleName = proto::RoleNameGenerator::generateRoleName(node->getPartitionId());
            auto it = _roleSlots.find(roleName);
            if (it == _roleSlots.end()) {
                return true;
            }
            HippoProtoHelper::setSlotInfo(it->second, node.get());
            proto::AgentNode* agentNode = static_cast<proto::AgentNode*>(node.get());
            const auto& partitionInfo = agentNode->getPartitionInfo();
            std::vector<std::string> slotAddrs;
            bool isReclaim = false;
            for (const auto& slotInfo : partitionInfo.slotinfos()) {
                if (slotInfo.reclaim()) {
                    isReclaim = true;
                    slotAddrs.push_back(slotInfo.id().slaveaddress());
                }
            }
            if (isReclaim) {
                BS_LOG(WARN, "trigger reclaim agent node. select reclaim node[%s][%s], will release soon.",
                       roleName.c_str(), autil::StringUtil::toString(slotAddrs, ";").c_str());
                _reclaimAgents.emplace_back(roleName);
            }
            return true;
        }

        const std::vector<std::string>& getReclaimAgents() const { return _reclaimAgents; }

    private:
        const std::map<std::string, SlotInfos>& _roleSlots;
        std::vector<std::string> _reclaimAgents;
    };

    SetSlotInfoFunctor setSlotInfo(roleSlots);
    _workerTable->forEachActiveNode(setSlotInfo);
    REPORT_KMONITOR_METRIC(_reclaimingAgentNodeCount, setSlotInfo.getReclaimAgents().size());

    // add reclaim agent role to blackList
    for (auto& agent : setSlotInfo.getReclaimAgents()) {
        auto iter = _agentRoles.find(agent);
        if (iter == _agentRoles.end()) {
            continue;
        }
        for (auto& role : iter->second->targetRoles) {
            if (!iter->second->isDynamicMapping) {
                continue;
            }
            // when dynamicMapping, reclaim agent will add role to blackList, trigger change agent node
            std::string agentRoleName;
            if (findAgentRoleName(role, agentRoleName) && agentRoleName == agent) {
                auto tmp = roleSlots.find(agent);
                assert(tmp != roleSlots.end());
                addTargetRoleToBlackList(role, tmp->second);
            }
        }
    }
}

void AgentSimpleMasterScheduler::clearUselessZkNodes()
{
    if (_lastClearTimestamp < 0) {
        // wait until timer inited
        return;
    }

    auto currentTimeInSec = autil::TimeUtility::currentTimeInSeconds();
    if (currentTimeInSec - _lastClearTimestamp < CLEAR_USELESS_ZK_NODE_INTERVAL) {
        return;
    }

    _lastClearTimestamp = currentTimeInSec;
    string agentNodeRoot = PathDefine::getAgentNodeRoot(_zkRoot);
    std::vector<std::string> fileList;
    fslib::util::FileUtil::listDir(agentNodeRoot, fileList);
    std::vector<std::string> toDelList;
    {
        autil::ScopedLock lock(_lock);
        for (const auto& fileName : fileList) {
            proto::PartitionId pid;
            proto::ProtoUtil::roleIdToPartitionId(fileName, _appName, pid);
            if (!pid.has_role() || pid.role() != proto::ROLE_AGENT) {
                continue;
            }
            if (_agentRoles.find(fileName) != _agentRoles.end()) {
                continue;
            }
            toDelList.push_back(fileName);
        }
    }
    for (auto& delFile : toDelList) {
        std::string rolePath = fslib::util::FileUtil::joinFilePath(agentNodeRoot, delFile);
        if (!fslib::util::FileUtil::remove(rolePath)) {
            BS_LOG(WARN, "remove agent worker %s failed", rolePath.c_str());
        } else {
            BS_LOG(INFO, "remove useless agent worker %s", rolePath.c_str());
        }
    }
}

bool AgentSimpleMasterScheduler::getFreshBlackListData(std::string& data)
{
    autil::ScopedLock lock(_lock);
    if (!_needSyncBlackList) {
        return false;
    }
    data = autil::legacy::ToJsonString(_blackListRoleMap);
    _needSyncBlackList = false;
    return true;
}

bool AgentSimpleMasterScheduler::checkPackageInfoMatch(const PackageInfos& lft, const PackageInfos& rht) const
{
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

bool AgentSimpleMasterScheduler::findAgentRoleName(const string& targetRole, string& agentRole) const
{
    autil::ScopedLock lock(_lock);
    auto iter = _targetRoleMap.find(targetRole);
    if (iter == _targetRoleMap.end() || isInBlackListUnsafe(targetRole, iter->second)) {
        return false;
    }
    agentRole = iter->second;
    return true;
}

void AgentSimpleMasterScheduler::removeUselessAgentNodes(const std::set<proto::BuildId>& inUseBuildIds)
{
    autil::ScopedLock lock(_lock);
    for (auto iter = _agentRoles.begin(); iter != _agentRoles.end();) {
        proto::BuildId id;
        string configName;
        size_t partCnt = 0;
        size_t idx = 0;
        if (!proto::RoleNameGenerator::parseAgentRoleName(iter->first, _appName, id, configName, partCnt, idx)) {
            BS_LOG(WARN, "invalid agent role name [%s]", iter->first.c_str());
            iter = _agentRoles.erase(iter);
            continue;
        }
        if (inUseBuildIds.find(id) == inUseBuildIds.end()) {
            BS_LOG(INFO, "remove agent role [%s] for useless buildId [%s] ", iter->first.c_str(),
                   id.ShortDebugString().c_str());
            iter = _agentRoles.erase(iter);
            continue;
        }
        iter++;
    }
}

void AgentSimpleMasterScheduler::getAgentRoleNames(const proto::BuildId& tid, std::set<std::string>& agentRolesSet)
{
    proto::PartitionId pid;
    proto::RoleNameGenerator::generateAgentNodePartitionId(tid, _appName, "default", 1, 0, pid);
    auto roleName = proto::RoleNameGenerator::generateRoleName(pid);
    auto pos = roleName.find(".agent.");
    assert(pos != string::npos);
    auto prefix = roleName.substr(0, pos);

    autil::ScopedLock lock(_lock);
    auto iter = _agentRoles.upper_bound(prefix);
    for (; iter != _agentRoles.end(); iter++) {
        proto::BuildId id;
        string configName;
        size_t partCnt = 0;
        size_t idx = 0;
        if (!proto::RoleNameGenerator::parseAgentRoleName(iter->first, _appName, id, configName, partCnt, idx)) {
            BS_LOG(WARN, "invalid agent role name [%s]", iter->first.c_str());
            continue;
        }
        if (id == tid) {
            agentRolesSet.insert(iter->first);
            continue;
        }
        if (iter->first.find(prefix) == string::npos) {
            break;
        }
    }
}

void AgentSimpleMasterScheduler::getAgentRoleNames(std::vector<std::string>& names)
{
    names.clear();
    autil::ScopedLock lock(_lock);
    for (auto iter = _agentRoles.begin(); iter != _agentRoles.end(); iter++) {
        names.push_back(iter->first);
    }
}

void AgentSimpleMasterScheduler::enableClearUselessAgentZkNodes()
{
    if (_lastClearTimestamp < 0) {
        _lastClearTimestamp = autil::TimeUtility::currentTimeInSeconds();
    }
}

void AgentSimpleMasterScheduler::workLoop()
{
    updateAgentNodeInfos();
    clearUselessZkNodes();
    serializeBlackListData();
}

void AgentSimpleMasterScheduler::serializeBlackListData()
{
    std::string content;
    bool needFlush = getFreshBlackListData(content);
    if (!needFlush) {
        return;
    }
    std::string finalStatusStr;
    autil::ZlibCompressor compressor;
    compressor.compress(content, finalStatusStr);
    if (WorkerState::EC_FAIL == _zkStatus.write(finalStatusStr)) {
        BS_LOG(WARN, "update agent black list file [%s] failed", content.c_str());
        _needSyncBlackList = true;
    }
}

void AgentSimpleMasterScheduler::updateAgentNodeInfos()
{
    autil::ScopedLock lock(_lock);
    if (_workerTable->getWorkerNodesMap().empty()) {
        return;
    }

    auto currentTimeInSec = autil::TimeUtility::currentTimeInSeconds();
    if (currentTimeInSec - _lastSyncSlotTimestamp > 10) {
        BS_LOG(INFO, "trigger force sync agent slot info.");
        std::map<std::string, SlotInfos> roleSlots;
        syncSlotInfosUnsafe(roleSlots);
    }
    _workerTable->syncNodesStatus();
}

std::string AgentSimpleMasterScheduler::addTargetRoleToBlackList(const string& roleName,
                                                                 const std::vector<hippo::SlotInfo>& roleSlotInfos)
{
    autil::ScopedLock lock(_lock);
    std::string agentRoleName;
    if (!findAgentRoleName(roleName, agentRoleName)) {
        return "";
    }

    bool isReclaim = false;
    std::string slotAddr;
    getAgentNodeSlotInfo(agentRoleName, isReclaim, slotAddr);

    std::vector<std::string> targetSlotAddrs;
    for (const auto& slotInfo : roleSlotInfos) {
        targetSlotAddrs.push_back(getIdentifier(slotInfo));
    }
    std::string targetSlotAddress = autil::StringUtil::toString(targetSlotAddrs, ";");
    if (slotAddr != targetSlotAddress) {
        AUTIL_LOG(WARN, "current agent slot [%s] with slotAddress [%s] is not target slot [%s]", agentRoleName.c_str(),
                  slotAddr.c_str(), targetSlotAddress.c_str());
        return "";
    }

    _needSyncBlackList = true;
    BlackAgentNode node;
    node.agentRole = agentRoleName;
    node.timestamp = autil::TimeUtility::currentTimeInSeconds();
    node.isReclaim = isReclaim;
    node.slotAddress = slotAddr;
    _inBlackListAgentInfo[agentRoleName] = node.timestamp;

    AUTIL_LOG(INFO, "add role [%s] to agent node black list, target agent role [%s] isReclaim [%s] slotAddress [%s].",
              roleName.c_str(), agentRoleName.c_str(), node.isReclaim ? "true" : "false", node.slotAddress.c_str());
    auto iter = _blackListRoleMap.find(roleName);
    if (iter == _blackListRoleMap.end()) {
        _blackListRoleMap[roleName].emplace_back(node);
        return agentRoleName;
    }

    auto& blackList = iter->second;
    for (auto& item : blackList) {
        if (item.agentRole == agentRoleName) {
            item.timestamp = node.timestamp;
            item.isReclaim = node.isReclaim;
            item.slotAddress = node.slotAddress;
            return agentRoleName;
        }
    }
    blackList.emplace_back(node);
    return agentRoleName;
}

bool AgentSimpleMasterScheduler::getInBlackListAgentRoles(const std::string& targetRole,
                                                          std::set<std::string>& inBlackListAgentRoles) const
{
    autil::ScopedLock lock(_lock);
    auto iter = _blackListRoleMap.find(targetRole);
    if (iter == _blackListRoleMap.end()) {
        return false;
    }
    for (auto& item : iter->second) {
        inBlackListAgentRoles.insert(item.agentRole);
    }
    return true;
}

bool AgentSimpleMasterScheduler::isInBlackListUnsafe(const std::string& targetRole,
                                                     const std::string& targetAgentRole) const
{
    auto iter = _blackListRoleMap.find(targetRole);
    if (iter == _blackListRoleMap.end()) {
        return false;
    }
    for (auto& item : iter->second) {
        if (item.agentRole == targetAgentRole) {
            return true;
        }
    }
    return false;
}

bool AgentSimpleMasterScheduler::getAgentNodeSlotInfo(const std::string& agentRole, bool& isReclaim,
                                                      std::string& slotAddress) const
{
    isReclaim = false;
    proto::WorkerNodes& agentNodes = _workerTable->getWorkerNodes(agentRole);
    if (agentNodes.empty()) {
        return false;
    }
    proto::AgentNode* agentNode = static_cast<proto::AgentNode*>(agentNodes[0].get());
    assert(agentNode);
    const auto& partitionInfo = agentNode->getPartitionInfo();
    std::vector<std::string> slotAddrs;
    for (const auto& slotInfo : partitionInfo.slotinfos()) {
        if (slotInfo.reclaim()) {
            isReclaim = true;
        }
        slotAddrs.push_back(getIdentifier(slotInfo));
    }
    slotAddress = autil::StringUtil::toString(slotAddrs, ";");
    return true;
}

void AgentSimpleMasterScheduler::recoverBlackListRoleMap()
{
    std::string content;
    WorkerState::ErrorCode ec = _zkStatus.read(content);
    if (WorkerState::EC_FAIL == ec) {
        BS_LOG(ERROR, "read generation state failed, ec[%d].", int(ec));
        return;
    }

    autil::ZlibCompressor compressor;
    string decompressedStr;
    if (!compressor.decompress(content, decompressedStr)) {
        BS_LOG(ERROR, "decompress black list string failed [%s]", content.c_str());
        return;
    }

    try {
        autil::legacy::FromJsonString(_blackListRoleMap, decompressedStr);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "invalid json string %s", decompressedStr.c_str());
    }
    refreshInBlackListAgentInfo();
}

bool AgentSimpleMasterScheduler::isInBlackList(const std::string& targetRole, const std::string& targetAgentRole) const
{
    autil::ScopedLock lock(_lock);
    return isInBlackListUnsafe(targetRole, targetAgentRole);
}

void AgentSimpleMasterScheduler::refreshInBlackListAgentInfo()
{
    std::unordered_map<std::string, int64_t> inBlackListAgentInfo;
    auto iter = _blackListRoleMap.begin();
    while (iter != _blackListRoleMap.end()) {
        auto& blackList = iter->second;
        for (auto blIter = blackList.begin(); blIter != blackList.end(); blIter++) {
            int64_t inBlackListTs = blIter->timestamp;
            auto tmpIter = inBlackListAgentInfo.find(blIter->agentRole);
            if (tmpIter == inBlackListAgentInfo.end()) {
                inBlackListAgentInfo.insert(make_pair(blIter->agentRole, inBlackListTs));
                continue;
            }
            if (tmpIter->second < inBlackListTs) {
                tmpIter->second = inBlackListTs;
                continue;
            }
        }
        ++iter;
    }
    _inBlackListAgentInfo.swap(inBlackListAgentInfo);
}

bool AgentSimpleMasterScheduler::getAgentNodeInBlackListTimestamp(const std::string& agentRole,
                                                                  int64_t& timestamp) const
{
    autil::ScopedLock lock(_lock);
    auto iter = _inBlackListAgentInfo.find(agentRole);
    if (iter == _inBlackListAgentInfo.end()) {
        return false;
    }
    timestamp = iter->second;
    return true;
}

bool AgentSimpleMasterScheduler::hasValidTargetRoles(const std::string& agentRole) const
{
    autil::ScopedLock lock(_lock);
    auto iter = _agentRoles.find(agentRole);
    if (iter == _agentRoles.end()) {
        return false;
    }
    for (auto& role : iter->second->targetRoles) {
        std::string agentRoleName;
        if (findAgentRoleName(role, agentRoleName) && agentRoleName == agentRole) {
            return true;
        }
    }
    return false;
}

std::pair<bool, std::string> AgentSimpleMasterScheduler::getAgentIdentifier(const std::string& agentRoleName) const
{
    autil::ScopedLock lock(_lock);
    if (_agentIdentifiers.empty()) {
        return {false, ""};
    }
    auto iter = _agentIdentifiers.find(agentRoleName);
    if (iter != _agentIdentifiers.end()) {
        return {true, iter->second};
    }
    return {true, ""};
}

bool AgentSimpleMasterScheduler::isAgentServiceReady(const std::string& agentRoleName) const
{
    autil::ScopedLock lock(_lock);
    auto [synced, expectedIdentifier] = getAgentIdentifier(agentRoleName);
    if (!synced) {
        AUTIL_LOG(WARN, "agent [%s] not synced but need schedule, regard not ready", agentRoleName.c_str());
        return false;
    }
    const auto& nodes = _workerTable->getWorkerNodes(agentRoleName);
    if (nodes.size() == 0) {
        AUTIL_LOG(INFO, "agent [%s] is not ready, node count is 0", agentRoleName.c_str());
        return false;
    }
    assert(nodes.size() == 1);
    proto::AgentNode* agentNode = static_cast<proto::AgentNode*>(nodes[0].get());
    auto [current, identifier] = agentNode->getCurrentStatusAndIdentifier();
    if (identifier.empty()) {
        AUTIL_LOG(INFO, "agent [%s] is not ready, current identifier is empty", agentRoleName.c_str());
        return false;
    }
    if (expectedIdentifier != identifier) {
        AUTIL_LOG(INFO, "agent [%s] is not ready, expected identifier [%s] vs currnet identifier [%s]",
                  agentRoleName.c_str(), expectedIdentifier.c_str(), identifier.c_str());
        return false;
    }
    if (current.has_serviceready()) {
        bool ready = current.serviceready();
        AUTIL_LOG(INFO, "agent [%s] ready [%d]", agentRoleName.c_str(), ready);
        return ready;
    }
    AUTIL_LOG(INFO, "agent [%s] has not serviceready info, not ready", agentRoleName.c_str());
    return false;
}

void AgentSimpleMasterScheduler::updateResourceStatistic(const std::map<std::string, SlotInfos>& roleSlots)
{
    int64_t cpuAmount = 0;
    int64_t memAmount = 0;
    for (auto iter = roleSlots.begin(); iter != roleSlots.end(); iter++) {
        const SlotInfos& slots = iter->second;
        for (const auto& slot : slots) {
            auto resource = slot.slotResource.find("cpu");
            if (resource) {
                cpuAmount += resource->amount;
            }
            resource = slot.slotResource.find("mem");
            if (resource) {
                memAmount += resource->amount;
            }
        }
    }
    _totalCpuAmount = cpuAmount;
    _totalMemAmount = memAmount;
    REPORT_KMONITOR_METRIC(_cpuAmountMetric, cpuAmount);
    REPORT_KMONITOR_METRIC(_memAmountMetric, memAmount);
}

void AgentSimpleMasterScheduler::getTotalResourceAmount(int64_t& totalCpuAmount, int64_t& totalMemAmount) const
{
    totalCpuAmount = _totalCpuAmount;
    totalMemAmount = _totalMemAmount;
}

}} // namespace build_service::admin
