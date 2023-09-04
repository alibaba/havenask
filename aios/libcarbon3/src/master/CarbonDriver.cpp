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
#include "master/Flag.h"
#include "common/PathUtil.h"
#include "fslib/fslib.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
USE_CARBON_NAMESPACE(common);
USE_CARBON_NAMESPACE(monitor);
BEGIN_CARBON_NAMESPACE(master);

#define CARBON_MASTER_VERSION "versionString\n";
#define CARBON_BUILD_VERSION "carbon_build_version\n";

CARBON_LOG_SETUP(master, CarbonDriver);

CarbonDriver::CarbonDriver(const string &mode) {
    _mode = mode;
    _initRet = false;
}

CarbonDriver::~CarbonDriver() {
    _globalConfigManagerPtr.reset();
    _serializerCreatorPtr.reset();
    _proxyPtr.reset();
    ANetTransportSingleton::stop();
    CarbonMonitor::stop();
}


bool CarbonDriver::init(const string &applicationId,
                        const std::string &hippoZk,
                        const std::string &carbonZk,
                        const std::string &backupPath,
                        worker_framework::LeaderElector *leaderElector,
                        const std::string &proxySpec, const std::string &kmonSpec,
                        const DriverOptions& opts)
{
    CARBON_LOG(INFO, "init carbon driver, applicationId:[%s], hippoZk:[%s], carbonZk:[%s], proxySpec:[%s]",
               applicationId.c_str(), hippoZk.c_str(), carbonZk.c_str(), proxySpec.c_str());

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
    _globalConfigManagerPtr.reset(new GlobalConfigManager(_serializerCreatorPtr));

    auto schConf = Flag::getSchConf();
    _globalConfigManagerPtr->prepareConfig(schConf.routeAll, schConf.routeRegex);
    
    if (!initMonitor(applicationId, kmonSpec)) {
        return false;
    }
    if (!recover()) {
        CARBON_LOG(ERROR, "recover carbonDriver error");
        return false;
    }

    std::string retProxySpec = proxySpec;
    // overwrite by env vars
    if (!schConf.host.empty()) {
        retProxySpec = schConf.host;
    }
    if (retProxySpec.empty()) {
        CARBON_LOG(WARN, "no carbon-proxy spec provided");
    }
    DriverOptions::SchType schType = opts.schType;
    if (schConf.schType >= 0) {
        schType = (DriverOptions::SchType) schConf.schType;
    }
    _proxyPtr.reset(new ProxyClient(retProxySpec, applicationId, schType));
    _initRet = true;
    return true;
}

bool CarbonDriver::isInited() const {
    return _initRet;
}

bool CarbonDriver::start() {
    CARBON_LOG(INFO, "start carbon driver.");
    return true;
}

void CarbonDriver::stop() {
    CARBON_LOG(INFO, "stop carbon driver.");
}

bool CarbonDriver::recover(bool isAppRestart) {
    if (!_globalConfigManagerPtr->recover()) {
        CARBON_LOG(ERROR, "recover globalConfigManager error");
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

bool CarbonDriver::initMonitor(const string &appId, const std::string& kmonSpec) {
    if (!CarbonMonitor::init(kmonSpec, Flag::getCluster(), appId)) {
        return false;
    }
    return true;
}

bool CarbonDriver::recover() {
    return true;
}

CarbonInfo CarbonDriver::getCarbonInfo() const {
    return GlobalVariable::getCarbonInfo();
}

END_CARBON_NAMESPACE(master);

