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
#include "master/GlobalConfigManager.h"
#include "carbon/SerializerCreator.h"
#include "carbon/ErrorDefine.h"

using namespace autil;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, GlobalConfigManager);

GlobalConfigManager::GlobalConfigManager(carbon::master::SerializerCreatorPtr scp) {
    _serializerPtr = scp->create(GLOBAL_CONFIG_SERIALIZE_FILE_NAME, "", false);
}

GlobalConfigManager::~GlobalConfigManager() {
    if (_syncThreadPtr != NULL) {
        _syncThreadPtr.reset();
    }
}

bool GlobalConfigManager::syncFromZK() {
    std::string content;
    WHEN_FS_FILE_EXIST(_serializerPtr) {
        if (!_serializerPtr->read(content)) {
            CARBON_LOG(ERROR, "read sys config data failed");
            return false;
        }
        try {
            ScopedWriteLock lock(_rwLock);
            FromJsonString(_config, content);
            CARBON_LOG(DEBUG, "sync sys config from zk, config: %s", content.c_str());
        } catch (const autil::legacy::ExceptionBase &e) {
            CARBON_LOG(ERROR, "sync sys config from json failed. error:[%s].", e.what());
            return false;
        }
    } END_WHEN();
    return true;
}

bool GlobalConfigManager::recover() {
    if (!syncFromZK()) {
        CARBON_LOG(ERROR, "recover sys config from json failed.");
        return false;
    }
    CARBON_LOG(INFO, "recovered sys config: %s", ToJsonString(_config).c_str());
    return true;
}

bool GlobalConfigManager::setConfig(const GlobalConfig& conf, carbon::ErrorInfo* errorInfo) {
    std::string content;
    try {
        content = ToJsonString(conf);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(ERROR, "jsonize sys config failed [%s]", e.what());
        errorInfo->errorCode = carbon::EC_JSON_INVALID;
        errorInfo->errorMsg = "jsonize sys config failed";
        return false;
    }
    if (!_serializerPtr->write(content)) {
        CARBON_LOG(ERROR, "write sys config failed:%s", content.c_str());
        errorInfo->errorCode = carbon::EC_PERSIST_FAILED;
        errorInfo->errorMsg = "write sys config failed";
        return false;
    }
    {
        CARBON_LOG(INFO, "update sys config: %s", content.c_str());
        ScopedWriteLock lock(_rwLock);
        _config = conf;
    }
    return true;
}

// don't persist config here.
void GlobalConfigManager::prepareConfig(bool routeAll, const std::string& routeRegex) {
    CARBON_LOG(INFO, "prepareConfig routerAll: %d, reg: (%s)", int(routeAll), routeRegex.c_str());
    auto& routerConf = _config.router;
    if (routeAll) {
        routerConf.targetRatio = 100;
        routerConf.srcRatio = -1;
    }
    if (!routeRegex.empty()) {
        RouterRatioModel rm;
        rm.group = routeRegex;
        rm.targetRatio = 100;
        rm.srcRatio = -1;
        routerConf.starRouterList.push_back(rm);
    }
}

const GlobalConfig& GlobalConfigManager::getConfig() const {
    ScopedReadLock lock(_rwLock);
    return _config;
}

bool GlobalConfigManager::bgSyncFromZK() {
    if (_syncThreadPtr != NULL) {
        CARBON_LOG(INFO, "globalConfigManager already start backgroud sync");
        return true;
    }
    _syncThreadPtr = LoopThread::createLoopThread(
            std::bind(&GlobalConfigManager::syncFromZK, this),
            DEFAULT_GLOBAL_CONFIG_MANAGER_LOOPINTERVAL);
    if (_syncThreadPtr == NULL) {
        CARBON_LOG(ERROR, "globalConfigManager init sync loop thread failed.");
        return false;
    }
    CARBON_LOG(INFO, "globalConfigManager init start bgSyncFromZK");
    return true;
}

END_CARBON_NAMESPACE(master);

