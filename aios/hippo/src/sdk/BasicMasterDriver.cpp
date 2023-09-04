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
#include "sdk/BasicMasterDriver.h"
#include <string>
#include "autil/StringUtil.h"
#include "autil/EnvUtil.h"
#include "util/PathUtil.h"
#include "util/CommonUtil.h"
#include "sdk/LeaderSerializerImpl.h"


using namespace std;
using namespace autil;
USE_HIPPO_NAMESPACE(common);
USE_HIPPO_NAMESPACE(util);

BEGIN_HIPPO_NAMESPACE(sdk);

HIPPO_LOG_SETUP(sdk, BasicMasterDriver);

BasicMasterDriver::BasicMasterDriver()
    : _isLeader(false)
    , _isWorking(false)
    , _slotAllocator(nullptr)
    , _processLauncher(nullptr)
    , _amLeaderChecker(nullptr)
    , _amLeaderCheckerCreator(new AmLeaderCheckerCreator())
{
    _eventTrigger = new EventTrigger;
    (*_eventTrigger) = std::bind(&BasicMasterDriver::triggerEvent, this,
            std::placeholders::_1, std::placeholders::_2,
            std::placeholders::_3);
}

BasicMasterDriver::~BasicMasterDriver() {
    detach();
    DELETE_AND_SET_NULL_HIPPO(_amLeaderCheckerCreator);
    DELETE_AND_SET_NULL_HIPPO(_eventTrigger);
}

bool BasicMasterDriver::init(const string &masterZkRoot,
                             const string &leaderElectRoot,
                             const string &appMasterAddr,
                             const string &applicationId)
{
    _appId = applicationId;
    if (!initSlotAllocator(masterZkRoot, applicationId) ||
        !initPorcessLauncher(masterZkRoot, applicationId) ||
        !initAmLeaderChecker(leaderElectRoot, appMasterAddr))
    {
        return false;
    }
    return true;
}

bool BasicMasterDriver::init(
        const string &masterZkRoot,
        worker_framework::LeaderChecker * leaderChecker,
        const string &applicationId)
{
    _appId = applicationId;
    if (!initSlotAllocator(masterZkRoot, applicationId) ||
        !initPorcessLauncher(masterZkRoot, applicationId) ||
        !initAmLeaderChecker(leaderChecker))
    {
        return false;
    }
    return true;
}

bool BasicMasterDriver::initAmLeaderChecker(const std::string &leaderElectRoot,
                         const std::string &appMasterAddr)
{
    _amLeaderChecker = _amLeaderCheckerCreator->createAmLeaderChecker(NULL);
    if (!_amLeaderChecker || !_amLeaderChecker->createPrivateLeaderChecker(
                    leaderElectRoot, LEADER_ELECTION_INTERVAL,
                    AM_LEADER_ELECTION_BASE_NAME, appMasterAddr))
    {
        return false;
    }
    return true;
}

bool BasicMasterDriver::initAmLeaderChecker(worker_framework::LeaderChecker *leaderChecker)
{
    _amLeaderChecker =
        _amLeaderCheckerCreator->createAmLeaderChecker(leaderChecker);
    return _amLeaderChecker != nullptr;
}

bool BasicMasterDriver::start() {
    if (_workingThreadPtr) {
        return true;
    }
    _workingThreadPtr = LoopThread::createLoopThread(
            std::bind(&BasicMasterDriver::workLoop, this),
            SDK_DRIVER_SCHEDULE_INTERVAL_US);
    if (!_workingThreadPtr) {
        HIPPO_LOG(ERROR, "sdk working thread start failed");
        return false;
    }
    return true;
}

bool BasicMasterDriver::stop() {
    if (!_workingThreadPtr) {
        return true;
    }
    detach();
    return true;
}

void BasicMasterDriver::detach() {
    HIPPO_LOG(INFO, "detach");
    if (!_workingThreadPtr) {
        return;
    }
    _workingThreadPtr->stop();
    _workingThreadPtr.reset();
}

void BasicMasterDriver::triggerEvent(hippo::MasterDriverEvent event,
                                     const vector<hippo::SlotInfo*> *slotInfos,
                                     string message)
{
    HIPPO_LOG(INFO, "trigger event:[%ld], msg:[%s].",
              event, message.c_str());
    if (slotInfos) {
        for (size_t i = 0; i < slotInfos->size(); ++i) {
            SlotInfo *info = (*slotInfos)[i];
            HIPPO_LOG(INFO, "slot info, role:%s, slave:%s, slotid:%d, "
                      "declareTime:%ld, recaliming:%d, status:%d",
                      info->role.c_str(), info->slotId.slaveAddress.c_str(),
                      info->slotId.id, info->slotId.declareTime,
                      info->reclaiming, info->slaveStatus.status);
        }
    }

    hippo::EventMessage em;
    em.driver = this;
    em.applicationId = _appId;
    em.slotInfos = slotInfos;
    em.message = message;


    autil::ScopedLock lock(_mutex);
    FuncMapType::const_iterator ite = _functions.find(uint64_t(event));

    if (_functions.end() == ite) {
        return ;
    } else if (ite->second) {
        ite->second(event, em);
    }
}

void BasicMasterDriver::registerEvent(uint64_t events,
        const hippo::EventFunctionType &function)
{
    for (uint32_t i = 0; i < FUNCTIONARRAYSIZE; ++i) {
        uint64_t mask = (1ull << i);
        if (events & mask) {
            autil::ScopedLock lock(_mutex);
            _functions[mask] = function;
        }
    }
}

void BasicMasterDriver::clearEvent(hippo::MasterDriverEvent events)
{
    for (uint32_t i = 0; i < FUNCTIONARRAYSIZE; ++i) {
        uint64_t mask = (1ull << i);
        autil::ScopedLock lock(_mutex);
        _functions.erase(mask);
    }
}

std::string BasicMasterDriver::getApplicationId() const {
    return _appId;
}

void BasicMasterDriver::setApplicationId(const string &appId){
    _appId = appId;
}

int64_t BasicMasterDriver::getAppChecksum() const {
    int64_t checksumValue = autil::EnvUtil::getEnv<int64_t>(HIPPO_ENV_APP_CHECKSUM, 0);
    return checksumValue;
}

bool BasicMasterDriver::isConnected() const {
    return true;
}

bool BasicMasterDriver::isWorking() const {
    return _isWorking;
}

hippo::MasterInfo BasicMasterDriver::getMasterInfo() const {
    return _masterInfo;
}

void BasicMasterDriver::updateRoleRequest(const string &role,
        const vector<hippo::SlotResource> &options,
        const vector<hippo::Resource> &declare,
        const string &queue,
        const groupid_t &groupId,
        hippo::ResourceAllocateMode allocateMode,
        int32_t count,
        Priority priority,
        std::map<std::string, std::string> metaTags,
        hippo::CpusetMode cpusetMode)
{
    RoleRequest request;
    request.options = options;
    request.declare = declare;
    request.queue = queue;
    request.groupId = groupId;
    request.allocateMode = allocateMode;
    request.count = count;
    request.priority = priority;
    request.metaTags = metaTags;
    request.cpusetMode = cpusetMode;
    _slotAllocator->updateResourceRequest(role, request);
}

void BasicMasterDriver::updateRoleRequest(const std::string &role,
        const RoleRequest &request)
{
    _slotAllocator->updateResourceRequest(role, request);
}

void BasicMasterDriver::releaseSlot(const hippo::SlotId &slotId,
                                    const hippo::ReleasePreference &pref,
                                    int32_t reserveTime)
{
    _slotAllocator->releaseSlot(slotId, pref, reserveTime);
}

void BasicMasterDriver::releaseRoleSlots(
        const string &role,
        const ReleasePreference &pref,
        int32_t reserveTime)
{
    _slotAllocator->releaseRoleSlots(role, pref, reserveTime);
}

void BasicMasterDriver::getSlots(vector<hippo::SlotInfo> *slots) const {
    _slotAllocator->getSlots(slots);
}

void BasicMasterDriver::getSlotsByRole(const string &role,
                                       vector<hippo::SlotInfo> *slots) const
{
    _slotAllocator->getSlotsByRole(role, slots);
}

void BasicMasterDriver::getReclaimingSlots(vector<hippo::SlotInfo> *slots) const {
    _slotAllocator->getReclaimingSlots(slots);
}

void BasicMasterDriver::setRoleProcess(const std::string &role,
                                       const hippo::ProcessContext &context,
                                       const std::string &scope)
{
    _processLauncher->setRoleProcessContext(role, context, scope);
}

void BasicMasterDriver::clearRoleProcess(const std::string &role) {
    _processLauncher->clearRoleProcessContext(role);
}

bool BasicMasterDriver::updatePackages(
        const hippo::SlotId &slotId,
        const vector<hippo::PackageInfo> &packages)
{
    SlotInfo slotInfo;
    if (!_slotAllocator->getSlotInfo(slotId, &slotInfo)) {
        HIPPO_LOG(ERROR, "failed to get slotinfo for slot%d@%s",
                  slotId.id, slotId.slaveAddress.c_str());
        return false;
    }
    return _processLauncher->updatePackages(slotInfo, packages);
}

bool BasicMasterDriver::isPackageReady(const hippo::SlotId &slotId) {
    SlotInfo slotInfo;
    if (!_slotAllocator->getSlotInfo(slotId, &slotInfo)) {
        HIPPO_LOG(ERROR, "failed to get slotinfo for slot%d@%s",
                  slotId.id, slotId.slaveAddress.c_str());
        return false;
    }
    return _processLauncher->isPackageReady(slotInfo);
}

bool BasicMasterDriver::updatePreDeployPackages(
        const hippo::SlotId &slotId,
        const vector<hippo::PackageInfo> &packages)
{
    SlotInfo slotInfo;
    if (!_slotAllocator->getSlotInfo(slotId, &slotInfo)) {
        HIPPO_LOG(ERROR, "failed to get slotinfo for slot%d@%s",
                  slotId.id, slotId.slaveAddress.c_str());
        return false;
    }
    return _processLauncher->updatePreDeployPackages(slotInfo, packages);
}

bool BasicMasterDriver::isPreDeployPackageReady(const hippo::SlotId &slotId) {
    SlotInfo slotInfo;
    if (!_slotAllocator->getSlotInfo(slotId, &slotInfo)) {
        HIPPO_LOG(ERROR, "failed to get slotinfo for slot%d@%s",
                  slotId.id, slotId.slaveAddress.c_str());
        return false;
    }
    return _processLauncher->isPreDeployPackageReady(slotInfo);
}

bool BasicMasterDriver::updateDatas(const hippo::SlotId &slotId,
                                    const vector<hippo::DataInfo> &datas,
                                    bool force)
{
    SlotInfo slotInfo;
    if (!_slotAllocator->getSlotInfo(slotId, &slotInfo)) {
        HIPPO_LOG(ERROR, "failed to get slotinfo for slot%d@%s",
                  slotId.id, slotId.slaveAddress.c_str());
        return false;
    }
    return _processLauncher->updateDatas(slotInfo, datas, force);
}

bool BasicMasterDriver::isDataReady(
        const hippo::SlotId &slotId,
        const std::vector<std::string> &dataNames)
{
    SlotInfo slotInfo;
    if (!_slotAllocator->getSlotInfo(slotId, &slotInfo)) {
        HIPPO_LOG(ERROR, "failed to get slotinfo for slot%d@%s",
                  slotId.id, slotId.slaveAddress.c_str());
        return false;
    }
    return _processLauncher->isDataReady(slotInfo, dataNames);
}

bool BasicMasterDriver::restartProcess(const hippo::SlotId &slotId,
                                       const vector<string> &processNames)
{
    SlotInfo slotInfo;
    if (!_slotAllocator->getSlotInfo(slotId, &slotInfo)) {
        HIPPO_LOG(ERROR, "failed to get slotinfo for slot%d@%s",
                  slotId.id, slotId.slaveAddress.c_str());
        return false;
    }
    return _processLauncher->restartProcess(slotInfo, processNames);
}

bool BasicMasterDriver::isProcessReady(
        const hippo::SlotId &slotId, const vector<string> &processNames)
{
    SlotInfo slotInfo;
    if (!_slotAllocator->getSlotInfo(slotId, &slotInfo)) {
        HIPPO_LOG(ERROR, "failed to get slotinfo for slot%d@%s",
                  slotId.id, slotId.slaveAddress.c_str());
        return false;
    }
    return _processLauncher->isProcessReady(slotInfo, processNames);
}

LeaderSerializer* BasicMasterDriver::createLeaderSerializer(
        const string &zookeeperRoot, const string &fileName,
        const string &backupRoot)
{
    if (!_amLeaderChecker) {
        return NULL;
    }
    string zkHost = PathUtil::getHostFromZkPath(zookeeperRoot);
    string zkBasePath = PathUtil::getPathFromZkPath(zookeeperRoot);
    if (zkBasePath.empty()) {
        HIPPO_LOG(ERROR, "illegal zookeeper root: %s", zookeeperRoot.c_str());
        return NULL;
    }
    return new LeaderSerializerImpl(fileName, new LeaderState(
                    _amLeaderChecker->getLeaderChecker(),
                    _amLeaderChecker->getZkWrapper(), zkBasePath, backupRoot));
}

bool BasicMasterDriver::resetSlot(const hippo::SlotId &slotId) {
    return _processLauncher->resetSlot(slotId);
}

void BasicMasterDriver::setSlotProcess(
        const vector<SlotInfo> &slotVec,
        const ProcessContext &context,
        const string &scope)
{
    _processLauncher->setSlotProcessContext(slotVec, context, scope);
}

void BasicMasterDriver::setSlotProcess(
        const std::vector<std::pair<std::vector<SlotInfo>, hippo::ProcessContext> > &slotLaunchPlans,
        const std::string &scope)
{
    _processLauncher->setSlotProcessContext(slotLaunchPlans, scope);
}

void BasicMasterDriver::getRoleNames(set<string> *roleNames) const {
    _slotAllocator->getRoleNames(roleNames);
}

hippo::ErrorInfo BasicMasterDriver::getLastErrorInfo() const {
    return _slotAllocator->getLastErrorInfo();
}

void BasicMasterDriver::workLoop() {
    beforeLoop();

    if (!checkLeader()) {
        HIPPO_LOG(ERROR, "not leader");
        _isWorking = false;
        return;
    }
    if (_slotAllocator->allocate()) {
        HIPPO_LOG(INFO, "allocate success");
        _isWorking = true;
    } else {
        _isWorking = false;
    }
    afterLoop();
}

bool BasicMasterDriver::checkLeader() {
    bool curIsLeader = _amLeaderChecker->isLeader();
    if (!_isLeader && !curIsLeader) {
        return false;
    }
    if (_isLeader && !curIsLeader) {
        _isLeader = false;
        triggerEvent(EVENT_AM_NOLONGER_LEADER, NULL, "nolonger leader");
        HIPPO_LOG(INFO, "nolonger leader");
        return false;
    }
    if (!_isLeader && curIsLeader) {
        if (isConnected()) {
            _isLeader = true;
            triggerEvent(EVENT_AM_BECOME_LEADER, NULL, "become leader");
            HIPPO_LOG(INFO, "become leader");
        } else {
            HIPPO_LOG(INFO, "connected faield, not leader");
            _isLeader = false;
            return false;
        }
    }
    return true;
}

END_HIPPO_NAMESPACE(sdk);
