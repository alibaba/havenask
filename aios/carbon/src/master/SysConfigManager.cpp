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
#include "master/SysConfigManager.h"
#include "master/Flag.h"

BEGIN_CARBON_NAMESPACE(master);

using namespace autil;

CARBON_LOG_SETUP(master, SysConfigManager);

SysConfigManager::SysConfigManager(SerializerCreatorPtr scp) {
    _serializerPtr = scp->create(SYSCONFIG_SERIALIZE_FILE_NAME, "", false);
    _allK8sConfig = carbon::SysConfig();
    _allK8sConfig.router.routeAll = true;
    _allK8sConfig.router.proxyRatio = 100;
    _allK8sConfig.router.locRatio = -1;
}

bool SysConfigManager::recover() {
    std::string content;
    WHEN_FS_FILE_EXIST(_serializerPtr) {
        if (!_serializerPtr->read(content)) {
            CARBON_LOG(ERROR, "read sys config data failed");
            return false;
        }
        try {
            FromJsonString(_config, content);
        } catch (const autil::legacy::ExceptionBase &e) {
            CARBON_LOG(ERROR, "recover sys config from json failed. error:[%s].", e.what());
            return false;
        }
    } END_WHEN();
    CARBON_LOG(INFO, "recovered sys config: %s", ToJsonString(_config).c_str());
    return true;
}

bool SysConfigManager::setConfig(const carbon::SysConfig& conf, ErrorInfo* errorInfo) {
    std::string content;
    try {
        content = ToJsonString(conf);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(ERROR, "jsonize sys config failed [%s]", e.what());
        errorInfo->errorCode = EC_JSON_INVALID;
        errorInfo->errorMsg = "jsonize sys config failed";
        return false;
    }
    if (!_serializerPtr->write(content)) {
        CARBON_LOG(ERROR, "write sys config failed:%s", content.c_str());
        errorInfo->errorCode = EC_PERSIST_FAILED;
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

carbon::SysConfig SysConfigManager::getConfig() const {
    if (Flag::getAllK8s()){
        return _allK8sConfig;
    }
    ScopedReadLock lock(_rwLock);
    return _config;
}

END_CARBON_NAMESPACE(master);
