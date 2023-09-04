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
#include "master/Role.h"
#include "carbon/Log.h"
#include "common/Recorder.h"
#include "master/SignatureGenerator.h"
#include "master/SlotMapper.h"
#include "master/ReplicaNodesAdjuster.h"
#include "master/SlotUtil.h"
#include "master/Util.h"
#include "autil/StringUtil.h"
#include <cmath>

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, Role);

#define DEBUG_SHOW_INT(v) CARBON_LOG(DEBUG, #v ":%d", v)

Role::Role() {
    _latestVersion = INVALID_VERSION;
    _idGeneratorPtr.reset(new IdGenerator());
}

Role::Role(const groupid_t &groupId, const roleid_t &roleId, const roleguid_t &roleGUID) {
    _groupId = groupId;
    _roleId = roleId;
    _roleGUID = roleGUID;
    _idGeneratorPtr.reset(new IdGenerator());
}

Role::~Role() {
}

Role::Role(const Role &rhs) {
    *this = rhs;
}

Role& Role::operator = (const Role& rhs) {
    if (this == &rhs) {
        return *this;
    }

    _groupId = rhs._groupId;
    _roleId = rhs._roleId;
    _roleGUID = rhs._roleGUID;
    _globalPlan = rhs._globalPlan;
    _versionedPlans = rhs._versionedPlans;
    _latestVersion = rhs._latestVersion;
    // deep copy replica nodes
    _replicaNodes.resize(rhs._replicaNodes.size());
    for (size_t i = 0; i < rhs._replicaNodes.size(); i++) {
        ReplicaNodePtr nodePtr(new ReplicaNode(*(rhs._replicaNodes[i])));
        _replicaNodes[i] = nodePtr;
    }

    _scheduleParams = rhs._scheduleParams;
    _slotInfos = rhs._slotInfos;
    _hippoAdapterPtr = rhs._hippoAdapterPtr;
    _healthCheckerPtr = rhs._healthCheckerPtr;
    _serviceSwitchPtr = rhs._serviceSwitchPtr;
    _brokenRecoverQuotaPtr = rhs._brokenRecoverQuotaPtr;
    _healthCheckerManagerPtr = rhs._healthCheckerManagerPtr;
    _serviceSwitchManagerPtr = rhs._serviceSwitchManagerPtr;
    _roleExecutorPtr = rhs._roleExecutorPtr;
    _replicaNodeCreatorPtr = rhs._replicaNodeCreatorPtr;
    _idGeneratorPtr = rhs._idGeneratorPtr;
    return *this;
}

bool Role::init(const HippoAdapterPtr &hippoAdapterPtr,
                const HealthCheckerManagerPtr &healthCheckerManagerPtr,
                const ServiceSwitchManagerPtr &serviceSwitchManagerPtr,
                bool recover)
{
    _hippoAdapterPtr = hippoAdapterPtr;
    _healthCheckerManagerPtr = healthCheckerManagerPtr;
    _serviceSwitchManagerPtr = serviceSwitchManagerPtr;
    _roleExecutorPtr.reset(new RoleExecutor(_hippoAdapterPtr));
    _brokenRecoverQuotaPtr.reset(new BrokenRecoverQuota());
    _replicaNodeCreatorPtr.reset(new ReplicaNodeCreator(_roleGUID,
                    _brokenRecoverQuotaPtr, _idGeneratorPtr));
    if (recover) {
        if (_groupId.empty()){
            _groupId = _roleGUID.substr(0, _roleGUID.rfind("." + _roleId));
        }
        for (auto &p : _versionedPlans) {
            p.second.resourceChecksum = p.second.plan.resourcePlan.getChecksum();
        }
        return recoverReplicaNodes();
    }
    return true;
}

void Role::setPlan(const version_t &version, const RolePlan &rolePlan) {
    /*
     * setPlan 每轮调度都会执行，对version在不在_versionedPlans中需要区分处理
     * 如果version已存在，只需更新些期望全局生效的内容，instanceId、resourcetag
     * 等都不重新生成，避免admin升级时这些字段发生变更，引发所有机器的直接变更。
     * 所有这些字段的更新都要走rolling，即admin升级自动引发一次rolling或者等待用户
     * 下一次更新才实际发生变化。
     */
    _globalPlan = rolePlan.global;
    string resourceTag;
    string newChecksum = rolePlan.version.resourcePlan.getChecksum();
    if (_versionedPlans.find(version) == _versionedPlans.end()) {
        // new target
        resourceTag = SignatureGenerator::genRoleResourceTag(
                _roleGUID, rolePlan.version.resourcePlan);
        ExtVersionedPlan extVersionedPlan;
        extVersionedPlan.plan = rolePlan.version;
        extVersionedPlan.availableCountBase = rolePlan.global.count;
        extVersionedPlan.resourceTag = resourceTag;
        extVersionedPlan.resourceChecksum = newChecksum;
        string extraSuffix = "";
        if (rolePlan.version.restartAfterResourceChange) {
            extraSuffix = newChecksum;
        }
        SlotUtil::rewriteProcInstanceId(&(extVersionedPlan.plan.launchPlan),
                extraSuffix);
        SlotUtil::rewritePackageCheckSum(&(extVersionedPlan.plan.launchPlan));
        extVersionedPlan.processVersion =
            SlotUtil::extractProcessVersion(extVersionedPlan.plan.launchPlan);
        _versionedPlans[version] = extVersionedPlan;
    } else {
        // existed target
        _versionedPlans[version].plan.updateBoardcastField(rolePlan.version);
        _versionedPlans[version].availableCountBase = rolePlan.global.count;
    }
    
    _latestVersion = version;
    if (rolePlan.global.count != 0) {
        for (ExtVersionedPlanMap::iterator it = _versionedPlans.begin();
             it != _versionedPlans.end(); ++it)
        {
            it->second.availableCountBase = rolePlan.global.count;
        }
    }
}

void Role::setEmptyPlan(const version_t &version) {
    _versionedPlans[version] = _versionedPlans[_latestVersion];
    _latestVersion = version;
    _versionedPlans[_latestVersion].availableCountBase = 0;
    _globalPlan.count = 0;
}

void Role::update() {
    updateServiceConfigs();
    updateBrokenRecoverQuotaConfig();
    
    WorkerNodeVect allWorkerNodes;
    getAllWorkerNodes(&allWorkerNodes);
    updateSlotInfos(allWorkerNodes);
    updateHealthAndServiceInfos(allWorkerNodes);
}

void Role::schedule(const ScheduleParams &scheduleParams) {
    if (!isWorking()) {
        CARBON_LOG(WARN, "HealthChecker and ServiceSwitch is not working, "
                   "not scheduling role [%s] this round.",
                   _roleGUID.c_str());
        return;
    }
    
    CARBON_LOG(DEBUG, "schedule role:%s", _roleId.c_str());
    _scheduleParams = scheduleParams;
    assignSlots();
    ajustReplicaNodes();
    setFinalPlan();
    scheduleAllReplicaNodes();
}

bool Role::requireVirtualIP() const { // only find in latest version plan
    const auto& it = _versionedPlans.find(_latestVersion);
    if (it == _versionedPlans.end()) return false;
    for (const auto& res : it->second.plan.resourcePlan.resources) {
        if (res.find("ip") != NULL) return true;
    }
    return false;
}

void Role::assignSlots() {
    map<tag_t, WorkerNodeVect> workerNodeMap;
    for (size_t i = 0; i < _replicaNodes.size(); i++) {
        const ReplicaNodePtr &replicaNodePtr = _replicaNodes[i];
        WorkerNodeVect workerNodes;
        replicaNodePtr->getWorkerNodes(&workerNodes);
        for (size_t j = 0; j < workerNodes.size(); j++) {
            const WorkerNodePtr &workerNodePtr = workerNodes[j];
            version_t version = workerNodePtr->getCurVersion();
            if (_versionedPlans.find(version) == _versionedPlans.end()) {
                CARBON_LOG(ERROR, "no resource tag about version:[%s].",
                        version.c_str());
                continue;
            }
            tag_t tag = _versionedPlans[version].resourceTag;
            workerNodeMap[tag].push_back(workerNodePtr);
        }
    }

    SlotMapper::mapping(_slotInfos, workerNodeMap);
}

void Role::ajustReplicaNodes() {
    ReplicaNodesAdjuster adjuster(_replicaNodeCreatorPtr,
                                  _replicaNodes,
                                  _globalPlan,
                                  _versionedPlans,
                                  _latestVersion,
                                  _scheduleParams,
                                  _roleGUID);
    adjuster.adjust();
    _replicaNodes = adjuster.getReplicaNodes();
}

void Role::scheduleAllReplicaNodes() {
    for (size_t i = 0; i < _replicaNodes.size(); i++) {
        ReplicaNodePtr replicaNodePtr = _replicaNodes[i];
        version_t version = replicaNodePtr->getVersion();
        ExtVersionedPlanMap::iterator it = _versionedPlans.find(version);
        if (it == _versionedPlans.end()) {
            CARBON_LOG(WARN, "not find versioned plan for version:[%s], role:[%s].",
                       version.c_str(), _roleId.c_str());
        } else {
            replicaNodePtr->setPlan(version, it->second);
        }
        bool smoothRecover = getSmoothRecoverFlag(_globalPlan);
        replicaNodePtr->setSmoothRecover(smoothRecover);
        replicaNodePtr->schedule();
    }
}

bool Role::getSmoothRecoverFlag(const GlobalPlan &globalPlan) {
    const auto &properties = globalPlan.properties;
    const auto &it = properties.find(
            GLOBAL_PLAN_PROP_SMOOTH_RECOVER);
    if (it == properties.end()) {
        return true;
    }

    string flagStr = it->second;
    StringUtil::toLowerCase(flagStr);
    if (flagStr == "false") {
        return false;
    }
    return true;
}

int32_t Role::getTargetLatestCount() const {
    double latestRatio = (double)_globalPlan.latestVersionRatio / 100.0;
    return (int32_t)ceil(_globalPlan.count * latestRatio);
}

void Role::getResourceTags(set<tag_t> *tags) const {
    ExtVersionedPlanMap::const_iterator it = _versionedPlans.begin();
    for (; it != _versionedPlans.end(); it++)
    {
        tags->insert(it->second.resourceTag);
    }
}

void Role::getVersionedPlan(VersionedPlanMap *versionedPlanMap) const {
    versionedPlanMap->clear();
    ExtVersionedPlanMap::const_iterator it = _versionedPlans.begin();
    for (; it != _versionedPlans.end(); it++) {
        (*versionedPlanMap)[it->first] = it->second.plan;
    }
}

void Role::updateSlotInfos(const WorkerNodeVect &workerNodes) {
    set<tag_t> tags;
    getResourceTags(&tags);
    SlotInfoMap slotInfos = _hippoAdapterPtr->getSlotsByTags(tags);
    for (size_t i = 0; i < workerNodes.size(); i++) {
        const WorkerNodePtr &workerNodePtr = workerNodes[i];
        const SlotId &slotId = workerNodePtr->getSlotId();
        if (!SlotUtil::isEmptySlot(slotId)) {
            SlotInfo slotInfo = slotInfos[slotId];
            workerNodePtr->updateSlotInfo(slotInfo);
        }
    }

    _slotInfos.swap(slotInfos);
}

const ReplicaNodeVect& Role::getReplicaNodes() const {
    return _replicaNodes;
}

void Role::updateHealthInfos(const HealthCheckerPtr &healthCheckerPtr,
                             const WorkerNodeVect &workerNodes)
{
    if (!healthCheckerPtr) {
        CARBON_LOG(ERROR, "update health infos failed. roleGUID:[%s]",
                   _roleGUID.c_str());
        return;
    }
    
    HealthInfoMap healthInfos = healthCheckerPtr->getHealthInfos();
    CARBON_LOG(DEBUG, "healthInfos:[%s].", ToJsonString(healthInfos, true).c_str());
    for (size_t i = 0; i < workerNodes.size(); i++) {
        const WorkerNodePtr &workerNodePtr = workerNodes[i];
        nodeid_t nodeUID = workerNodePtr->getId();
        CARBON_LOG(DEBUG, "update health info, workernode:[%s].",
                   nodeUID.c_str());
        workerNodePtr->updateHealthInfo(healthInfos[nodeUID]);
        CARBON_LOG(DEBUG, "update worker node health status, node:[%s], "
                   "status:[%s].", nodeUID.c_str(),
                   ToJsonString(healthInfos[nodeUID]).c_str());
    }
}

void Role::updateHealthAndServiceInfos(const WorkerNodeVect &workerNodes) {
    HealthCheckerPtr healthCheckerPtr = getHealthChecker();
    updateHealthInfos(healthCheckerPtr, workerNodes);
    ServiceSwitchPtr serviceSwitchPtr = getServiceSwitch();
    updateServiceInfos(serviceSwitchPtr, workerNodes);

    if (healthCheckerPtr != NULL) {
        healthCheckerPtr->update(workerNodes);
    }
    if (serviceSwitchPtr != NULL) {
        serviceSwitchPtr->update(workerNodes);
    }
}

void Role::updateServiceInfos(const ServiceSwitchPtr &serviceSwitchPtr,
                              const WorkerNodeVect &workerNodes)
{
    if (serviceSwitchPtr == NULL) {
        CARBON_LOG(DEBUG, "serviceSwitchPtr is NULL");
        return;
    }

    ServiceInfoMap serviceInfos = serviceSwitchPtr->getServiceInfos();

    CARBON_LOG(DEBUG, "serviceInfos size:%lu", serviceInfos.size());
    ServiceInfo unAvailServiceInfo;
    unAvailServiceInfo.status = SVT_UNAVAILABLE;

    for (size_t i = 0; i < workerNodes.size(); i++) {
        const WorkerNodePtr &workerNodePtr = workerNodes[i];
        nodeid_t nodeId = workerNodePtr->getId();
        ServiceInfoMap::iterator it = serviceInfos.find(nodeId);
        if (it == serviceInfos.end()) {
            workerNodePtr->updateServiceInfo(unAvailServiceInfo);
            continue;
        } else {
            workerNodePtr->updateServiceInfo(it->second);
        }
    }
}

version_t Role::selectOldVersion() const {
    ExtVersionedPlanMap::const_iterator it = _versionedPlans.begin();
    for (; it != _versionedPlans.end(); it++) {
        if (it->first != _latestVersion) {
            return it->first;
        }
    }
    return INVALID_VERSION;
}

map<version_t, int32_t> Role::getOldVerCountMap() const {
    map<version_t, int32_t> oldVerCountMap;
    for (ExtVersionedPlanMap::const_iterator it = _versionedPlans.begin();
         it != _versionedPlans.end(); it++)
    {
        if (it->first != _latestVersion) {
            oldVerCountMap[it->first] = 0;
        }
    }

    WorkerNodeVect workerNodes;
    for (size_t i = 0; i < _replicaNodes.size(); i++) {
        _replicaNodes[i]->getWorkerNodes(&workerNodes);
    }
    
    for (size_t i = 0; i < workerNodes.size(); i++) {
        if (workerNodes[i]->getCurVersion() != _latestVersion) {
            oldVerCountMap[workerNodes[i]->getCurVersion()] += 1;
            oldVerCountMap[workerNodes[i]->getNextVersion()] += 1;
        }
    }
    
    return oldVerCountMap;
}

void Role::removeVersionPlan(version_t version) {
    CARBON_LOG(INFO, "remove old version %s in role %s, latestVersion:%s",
               version.c_str(), _roleGUID.c_str(), _latestVersion.c_str());
    if (version == _latestVersion) {
        CARBON_LOG(INFO, "version to remove is equal to latestVersion, not remove.");
        return;
    }
    _versionedPlans.erase(version);
}

void Role::getVerAvailablePercent(
        PercentMap *verAvailablePercent) const
{
    verAvailablePercent->clear();
    map<string, int32_t> versionCount;
    
    for (ExtVersionedPlanMap::const_iterator vIt = _versionedPlans.begin();
         vIt != _versionedPlans.end(); vIt++)
    {
        versionCount[vIt->first] = 0;
    }
    ReplicaNodeVect::const_iterator nIt = _replicaNodes.begin();
    for (; nIt != _replicaNodes.end(); nIt++) {
        string key = _roleGUID + "." + (*nIt)->getId() + ".available";
        string value = to_string((*nIt)->isAvailable());
        if (common::Recorder::diff(key, value)) {
            CARBON_LOG(INFO, "role %s check replica node %s available:%d",
                       _roleGUID.c_str(), (*nIt)->getId().c_str(), (*nIt)->isAvailable());
        }
        if ((*nIt)->isAvailable()) {
            versionCount[(*nIt)->getVersion()]++;
        }
    }

    map<string, int32_t>::iterator it = versionCount.begin();
    for (; it != versionCount.end(); it++) {
        ExtVersionedPlanMap::const_iterator vIt = _versionedPlans.find(it->first);
        assert(vIt != _versionedPlans.end());
        int32_t percentBaseCount = vIt->second.availableCountBase;
        if (percentBaseCount == 0) {
            CARBON_LOG(INFO, "[%s] calc available percent skip version [%s]",
                       _roleGUID.c_str(), it->first.c_str());
            continue;
        }

        int32_t availableCount = it->second;
        (*verAvailablePercent)[it->first] = availableCount * 100 / percentBaseCount;
    }
}

HealthCheckerPtr Role::getHealthChecker() {
    const HealthCheckerConfig &config = _globalPlan.healthCheckerConfig;
    string healthCheckerId = SignatureGenerator::genHealthCheckerId(_roleGUID, config);
    if (_healthCheckerPtr != NULL && healthCheckerId != _healthCheckerPtr->getId()) {
        _healthCheckerPtr.reset();
    }
        
    if (_healthCheckerPtr == NULL) {
        _healthCheckerPtr = _healthCheckerManagerPtr->getHealthChecker(
                healthCheckerId, config);
    }
    
    return _healthCheckerPtr;
}

ServiceSwitchPtr Role::getServiceSwitch() {
    if (_serviceSwitchPtr) {
        return _serviceSwitchPtr;
    }

    _serviceSwitchPtr = _serviceSwitchManagerPtr->getServiceSwitch(_roleGUID);
    if (_serviceSwitchPtr == NULL) {
        CARBON_LOG(ERROR, "create service switch failed. roleguid:[%s]",
                   _roleGUID.c_str());
    }
    return _serviceSwitchPtr;
}

void Role::updateServiceConfigs() {
    ServiceSwitchPtr serviceSwitchPtr = getServiceSwitch();
    if (serviceSwitchPtr) {
        serviceSwitchPtr->updateConfigs(filterServiceConfigs(_globalPlan.serviceConfigs));
    }
}

#define IS_REQUIRE_VIP(t) (t == ST_ARMORY || t == ST_SKYLINE)

std::vector<ServiceConfig> Role::filterServiceConfigs(const std::vector<ServiceConfig>& oldConfigs) {
    bool requireIP = requireVirtualIP();
    std::vector<ServiceConfig> serviceConfigs;
    std::copy_if(oldConfigs.begin(), oldConfigs.end(), std::back_inserter(serviceConfigs),
            [requireIP](const ServiceConfig& conf) { return !IS_REQUIRE_VIP(conf.type) || requireIP; });
    return serviceConfigs;
}

void Role::getAllWorkerNodes(WorkerNodeVect *workerNodes) const {
    for (size_t i = 0; i < _replicaNodes.size(); i++) {
        _replicaNodes[i]->getWorkerNodes(workerNodes);
    }
}

void Role::execute() {
    WorkerNodeVect allWorkerNodes;
    getAllWorkerNodes(&allWorkerNodes);
    ScheduleInfo scheduleInfo;
    scheduleInfo.count = _globalPlan.count;
    scheduleInfo.minHealthCount = _scheduleParams.minHealthCount;
    _roleExecutorPtr->execute(_groupId, _roleGUID, allWorkerNodes,
                              _versionedPlans, _latestVersion, _slotInfos, scheduleInfo);
}

void Role::setFinalPlan() {
    for (size_t i = 0; i < _replicaNodes.size(); i++) {
        _replicaNodes[i]->setFinalPlan(_latestVersion,
                _versionedPlans[_latestVersion]);
    }
}

void Role::fillRoleStatus(RoleStatus *status) const {
    status->roleId = _roleId;
    status->globalPlan = _globalPlan;
    status->latestVersion = _latestVersion;
    status->nodes.resize(_replicaNodes.size());
    getVersionedPlan(&status->versionedPlans);
    status->userDefVersion = versionToUserDefVersion(_latestVersion);
    status->readyForCurVersion = isCompleted();
    for (size_t i = 0; i < _replicaNodes.size(); i++) {
        _replicaNodes[i]->fillReplicaNodeStatus(&status->nodes[i]);
    }
}

string Role::versionToUserDefVersion(version_t version) const {
    auto iter = _versionedPlans.find(version);
    if (iter != _versionedPlans.end()) {
        return iter->second.plan.userDefVersion;
    }
    return "";
}

bool Role::isCompleted() const {
    if (_versionedPlans.size() != 1) {
        return false;
    }
    if ((int32_t)_replicaNodes.size() != _globalPlan.count) {
        return false;
    }
    for (size_t i = 0; i < _replicaNodes.size(); i++) {
        if (_replicaNodes[i]->getVersion() != _latestVersion) {
            return false;
        }
        if (!_replicaNodes[i]->isCompleted()) {
            return false;
        }
    }
    return true;
}

bool Role::isStopped() const {
    return _replicaNodes.size() == 0 && _globalPlan.count == 0;
}

void Role::stop() {
    _globalPlan.count = 0;
    _versionedPlans[_latestVersion].availableCountBase = 0;
}

void Role::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("roleId", _roleId, _roleId);
    json.Jsonize("groupId", _groupId, _groupId);
    json.Jsonize("roleGUID", _roleGUID, _roleGUID);
    json.Jsonize("globalPlan", _globalPlan, _globalPlan);
    json.Jsonize("versionedPlans", _versionedPlans, _versionedPlans);
    json.Jsonize("latestVersion", _latestVersion, _latestVersion);
    json.Jsonize("replicaNodes", _replicaNodes);
    json.Jsonize("idGenerator", _idGeneratorPtr);
}

bool Role::recoverReplicaNodes() {
    assert(_replicaNodeCreatorPtr != NULL);
    for (size_t i = 0; i < _replicaNodes.size(); i++) {
        if (!_replicaNodes[i]->recover(
                        _latestVersion, _versionedPlans,
                        _replicaNodeCreatorPtr->getBrokenRecoverQuota(),
                        _replicaNodeCreatorPtr->getWorkerNodeCreator(
                                _replicaNodes[i]->getId())))
        {
            return false;
        }
    }
    return true;
}

void Role::updateBrokenRecoverQuotaConfig() {
    const BrokenRecoverQuotaConfig &config =
        _globalPlan.brokenRecoverQuotaConfig;
    _brokenRecoverQuotaPtr->updateConfig(config);
}

int32_t Role::getBaseCount(const version_t &version) const {
    ExtVersionedPlanMap::const_iterator it = _versionedPlans.find(version);
    if (it == _versionedPlans.end()) {
        CARBON_LOG(ERROR, "[%s] get basecount of not exist version [%s]",
                   _roleGUID.c_str(), version.c_str());
        return -1;
    }
    return it->second.availableCountBase;
}

bool Role::recoverServiceAdapters() {
    ServiceSwitchPtr serviceSwitchPtr = getServiceSwitch();
    if (serviceSwitchPtr) {
        return serviceSwitchPtr->recoverServiceAdapters(filterServiceConfigs(_globalPlan.serviceConfigs));
    }
    return false;
}

bool Role::isWorking() {
    HealthCheckerPtr healthCheckerPtr = getHealthChecker();
    if (healthCheckerPtr == NULL) {
        CARBON_LOG(ERROR, "get HealthChecker failed.");
        return false;
    }
    if (!healthCheckerPtr->isWorking()) {
        CARBON_LOG(WARN, "health checker is not working.");
        return false;
    }

    ServiceSwitchPtr serviceSwitchPtr = getServiceSwitch();
    if (serviceSwitchPtr == NULL) {
        CARBON_LOG(ERROR, "get ServiceSwitch failed.");
        return false;
    }
    if (!serviceSwitchPtr->isWorking()) {
        CARBON_LOG(WARN, "service switch is not working.");
        return false;
    }

    return true;
}

void Role::releaseSlot(const ReleaseSlotInfo &releaseSlotInfo) {
    CARBON_LOG(INFO, "Role %s release slot %s", _roleGUID.c_str(), ToJsonString(releaseSlotInfo).c_str());
    WorkerNodeVect allWorkerNodes;
    getAllWorkerNodes(&allWorkerNodes);
    for (auto workerNode : allWorkerNodes) {
        if (workerNode->getSlotId().id == releaseSlotInfo.carbonUnitId.hippoSlotId) {
            ReleasePreference pref;
            pref.type = ReleasePreference::RELEASE_PREF_PROHIBIT;
            pref.leaseMs = releaseSlotInfo.leaseMs;
            workerNode->releaseWithPref(pref);
            return;
        }
    }
}

END_CARBON_NAMESPACE(master);

