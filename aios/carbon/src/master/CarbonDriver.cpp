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
#include "master/CarbonDriver.h"
#include "master/ANetTransportSingleton.h"
#include "master/BufferedHippoAdapter.h"
#include "master/Flag.h"
#include "common/PathUtil.h"
#include "carbon/config.h"
#include "fslib/fslib.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
USE_CARBON_NAMESPACE(common);
USE_CARBON_NAMESPACE(monitor);
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, CarbonDriver);

CarbonDriver::CarbonDriver(const string &mode) {
    _mode = mode;
    _initRet = false;
    _withBuffer = false;
}

CarbonDriver::~CarbonDriver() {
    _routerPtr.reset();
    _groupManagerPtr.reset();
    _groupPlanManagerPtr.reset();
    _serializerCreatorPtr.reset();
    _hippoAdapterPtr.reset();
    _sysConfManagerPtr.reset();
    ANetTransportSingleton::stop();
}


bool CarbonDriver::init(const string &applicationId,
                        const string &hippoZk,
                        const string &carbonZk,
                        const string &backupPath,
                        worker_framework::LeaderElector *leaderElector,
                        bool isNewStart,
                        uint32_t amonitorPort, bool withBuffer, bool k8sMode)
{
    CARBON_LOG(INFO, "init carbon driver, applicationId:[%s], hippoZk:[%s], "
               "carbonZk:[%s], isNewStart:[%d], k8sMode:[%d]",
               applicationId.c_str(), hippoZk.c_str(),
               carbonZk.c_str(), (int32_t)isNewStart, k8sMode);

    if (!ANetTransportSingleton::init()) {
        CARBON_LOG(ERROR, "init anet transport failed");
        return false;
    }

    {
        CarbonInfo tmpCarbonInfo;
        tmpCarbonInfo.appId = applicationId;
        tmpCarbonInfo.carbonZk = carbonZk;
        tmpCarbonInfo.hippoZk = hippoZk;
        tmpCarbonInfo.releaseVersion = CARBON_MASTER_VERSION;
        tmpCarbonInfo.buildVersion = CARBON_BUILD_VERSION;
        tmpCarbonInfo.mode = _mode;
        tmpCarbonInfo.isInited = true;
        GlobalVariable::setCarbonInfo(tmpCarbonInfo);
    }

    string serializePath = PathUtil::joinPath(carbonZk, SERIALIZE_DIR);
    _serializerCreatorPtr.reset(new SerializerCreator(leaderElector,
                    serializePath, backupPath));
    _sysConfManagerPtr.reset(new SysConfigManager(_serializerCreatorPtr));
    _withBuffer = withBuffer;
    _k8sMode = k8sMode;
    if (!_k8sMode) {
        if (!initHippoAdapter(applicationId, hippoZk, leaderElector)) {
            return false;
        }
    }
    
    if (!initMonitor(applicationId, amonitorPort)) {
        return false;
    }
    
    if (!initGroupManager(applicationId)) {
        return false;
    }

    bool succ = false;
    if (isNewStart) {
        succ = newStart(serializePath);
    } else {
        if (!_k8sMode) {
            succ = recover();
        } else {
            CARBON_LOG(INFO, "carbon master is on k8s mode, no need to recover");
            succ = true;
        }
    }
    
    if (succ) {
        if (!_k8sMode) {
            succ = writeAppChecksum();
        }
    }

    _initRet = succ;
    return succ;
}

bool CarbonDriver::isInited() const {
    return _initRet;
}

void CarbonDriver::setGlobalConfig(const GlobalConfig& config, ErrorInfo* errorInfo) {
    if (_withBuffer) {
        ((BufferedHippoAdapter*)_hippoAdapterPtr.get())->setConfig(config.bufferConfig, errorInfo);
    }
    _sysConfManagerPtr->setConfig(config.sysConfig, errorInfo);
}

GlobalConfig CarbonDriver::getGlobalConfig() const {
    GlobalConfig config;
    if (_withBuffer) {
        config.bufferConfig = ((BufferedHippoAdapter*)_hippoAdapterPtr.get())->getConfig();
    }
    config.sysConfig = _sysConfManagerPtr->getConfig();
    return config;
}

HippoAdapterPtr CarbonDriver::getBufferHippoAdapter() {
    if (!_withBuffer) return HippoAdapterPtr();
    return _hippoAdapterPtr;
}

DriverStatus CarbonDriver::getStatus(ErrorInfo* errorInfo) const {
    DriverStatus status;
    status.useBuffer = _withBuffer;
    if (_withBuffer) {
        BufferedHippoAdapter* adapter = (BufferedHippoAdapter*)_hippoAdapterPtr.get();
        status.bufferStatus = adapter->getStatus();
    }
    return status;
}

bool CarbonDriver::start() {
    CARBON_LOG(INFO, "start carbon driver.");
    if (_k8sMode) {
        CARBON_LOG(INFO, "on k8s mode, return true now.");
        return true;
    }
    if (_groupManagerPtr == NULL || _hippoAdapterPtr == NULL) {
        CARBON_LOG(ERROR, "not inited, start failed");
        return false;
    }
    
    return _hippoAdapterPtr->start() && _groupManagerPtr->start();
}

void CarbonDriver::stop() {
    CARBON_LOG(INFO, "stop carbon driver.");
    if (_groupManagerPtr) {
        _groupManagerPtr->stop();
    }

    if (_hippoAdapterPtr) {
        _hippoAdapterPtr->stop();
    }
    CarbonMonitor *monitor = CarbonMonitorSingleton::getInstance();
    monitor->stop();
}

bool CarbonDriver::recover(bool isAppRestart) {
    if (!_sysConfManagerPtr->recover()) {
        CARBON_LOG(ERROR, "recover sys config failed.");
        return false;
    }
    assert(_groupPlanManagerPtr != NULL && _groupManagerPtr != NULL);
    if (!_groupPlanManagerPtr->recover()) {
        CARBON_LOG(ERROR, "recover group plan manager failed.");
        return false;
    }

    for (auto &planPair : _groupPlanManagerPtr->getGroupPlans()) {
        VersionedGroupPlan &versionedPlan = planPair.second;
        if (Flag::getAllK8s()){
            CARBON_LOG(INFO, "recover check allk8s.");
            for (auto &rolePair: versionedPlan.plan.rolePlans){
                RolePlan &role = rolePair.second;
                if (role.global.count > 0){
                    CARBON_LOG(ERROR, "allk8s mode recover failed.");
                    return false;
                }
            }
        }
    }

    if (isAppRestart) {
        CARBON_LOG(INFO, "app is restart or new start.");
        return true;
    }
   
    if (!_hippoAdapterPtr->recover()) {
        CARBON_LOG(ERROR, "recover hippo adapter failed.");
        return false;
    }

    if (!_groupManagerPtr->recover()) {
        CARBON_LOG(ERROR, "recover group manager failed.");
        return false;
    }

    return true;
}

bool CarbonDriver::removeSerializeDir(const string &serializePath) {
    fslib::ErrorCode ec = fslib::fs::FileSystem::isExist(serializePath);
    if (ec == fslib::EC_FALSE) {
        return true;
    }
    
    string backupSerializePath = serializePath + ".bak";
    ec = fslib::fs::FileSystem::isExist(backupSerializePath);
    if (ec != fslib::EC_TRUE && ec != fslib::EC_FALSE) {
        CARBON_LOG(ERROR, "check backup serialize dir failed, "
                   "backupSerializePath:[%s], error:[%s]",
                   serializePath.c_str(),
                   fslib::fs::FileSystem::getErrorString(ec).c_str());
        return false;
    }

    if (ec == fslib::EC_TRUE) {
        ec = fslib::fs::FileSystem::remove(backupSerializePath);
        if (ec != fslib::EC_OK) {
            CARBON_LOG(ERROR, "remove backup serialize path failed, "
                       "error:[%s].",
                       fslib::fs::FileSystem::getErrorString(ec).c_str());
            return false;
        }
    }

    ec = fslib::fs::FileSystem::rename(serializePath, backupSerializePath);
    if (ec != fslib::EC_OK) {
        CARBON_LOG(ERROR, "backup carbon serialize dir failed, error:[%s].",
                   fslib::fs::FileSystem::getErrorString(ec).c_str());
        return false;
    }

    return true;
}

bool CarbonDriver::checkAppRestart(bool *isAppRestart) {
    *isAppRestart = false;
    SerializerPtr serializerPtr = _serializerCreatorPtr->create(
            APPCHECKSUM_SERIALIZE_FILE_NAME, "");
    if (serializerPtr == NULL) {
        CARBON_LOG(ERROR, "create app check sum serializer failed.");
        return false;
    }
    
    bool isExist;
    if (!serializerPtr->checkExist(isExist)) {
        CARBON_LOG(ERROR, "check app check sum serialize file exist failed.");
        return false;
    }

    if (!isExist) {
        CARBON_LOG(INFO, "app check sum does not exist.");
        *isAppRestart = true;
        return true;
    }
    
    string content;
    if (!serializerPtr->read(content)) {
        CARBON_LOG(ERROR, "read app check sum serialize file failed.");
        return false;
    }
    int64_t curAppCheckSum  = _hippoAdapterPtr->getAppChecksum();
    if (content == StringUtil::toString(curAppCheckSum)) {
        *isAppRestart = false;
    } else {
        *isAppRestart = true;
    }

    CARBON_LOG(INFO, "app check sum, cur:[%s], old:[%s].",
               StringUtil::toString(curAppCheckSum).c_str(),
               content.c_str());

    return true;
}

bool CarbonDriver::writeAppChecksum() {
    SerializerPtr serializerPtr = _serializerCreatorPtr->create(
            APPCHECKSUM_SERIALIZE_FILE_NAME, "");
    if (serializerPtr == NULL) {
        CARBON_LOG(ERROR, "create app check sum serializer failed.");
        return false;
    }
    int64_t curAppChecksum = _hippoAdapterPtr->getAppChecksum();
    string appChecksumStr = StringUtil::toString(curAppChecksum);
    if (!serializerPtr->write(appChecksumStr)) {
        CARBON_LOG(ERROR, "write app check sum failed.");
        return false;
    }

    CARBON_LOG(INFO, "write app check sum:[%s].", appChecksumStr.c_str());

    return true;
}

bool CarbonDriver::initHippoAdapter(
        const string &applicationId,
        const string &hippoZk,
        worker_framework::LeaderElector *leaderElector)
{
    if (_hippoAdapterPtr != NULL) {
        CARBON_LOG(ERROR, "init hippoAdapter failed, "
                   "hippoAdapter already exist");
        return false;
    }
    
    if (_withBuffer) { 
        _hippoAdapterPtr.reset(new BufferedHippoAdapter(_serializerCreatorPtr));
    } else {
        _hippoAdapterPtr.reset(new HippoAdapter());
    }
    if (!_hippoAdapterPtr->init(hippoZk, leaderElector, applicationId)) {
        CARBON_LOG(ERROR, "call hippoAdapter init failed");
        return false;
    }

    return true;
}

bool CarbonDriver::initGroupManager(const std::string& app) {
    SerializerPtr planSerializerPtr = _serializerCreatorPtr->create(
            GROUPS_PLAN_SERIALIZE_FILE_NAME, "", true);
    if (planSerializerPtr == NULL) {
        CARBON_LOG(ERROR, "create group plans serializer failed");
        return false;
    }
    _groupPlanManagerPtr.reset(new GroupPlanManager(planSerializerPtr));
    _groupManagerPtr.reset(new GroupManager(_hippoAdapterPtr,
                    _groupPlanManagerPtr, _serializerCreatorPtr, _extAdapterCreatorPtr));
    _routerPtr.reset(new Router(app, Flag::getRouterConf(), _groupPlanManagerPtr, _groupManagerPtr, _sysConfManagerPtr));
    return true;
}

bool CarbonDriver::initMonitor(const string &appId, uint32_t port) {
    if (port == 0) {
        CARBON_LOG(INFO, "amontior port is 0, not init monitor");
        return true;
    }
    CarbonMonitor *monitor = CarbonMonitorSingleton::getInstance();
    if (!monitor->init(DEFAULT_AMONITOR_SERVICE_NAME,
                       appId, port))
    {
        return false;
    }
    return monitor->start();
}

bool CarbonDriver::newStart(const string &serializePath) {
    CARBON_LOG(INFO, "carbon master is new started.");
    if (!removeSerializeDir(serializePath)) {
        CARBON_LOG(ERROR, "backup carbon master serialize dir failed.");
        return false;
    }

    return true;
}

bool CarbonDriver::recover() {
    CARBON_LOG(INFO, "carbon master is recovering ...");

    bool isAppRestart = false;
    if (!checkAppRestart(&isAppRestart)) {
        CARBON_LOG(ERROR, "check app restart failed.");
        return false;
    }
    
    if (!recover(isAppRestart)) {
        CARBON_LOG(ERROR, "carbon master recover failed.");
        return false;
    }

    CARBON_LOG(INFO, "carbon master recover success.");

    return true;
}

CarbonInfo CarbonDriver::getCarbonInfo() const {
    return GlobalVariable::getCarbonInfo();
}

END_CARBON_NAMESPACE(master);

