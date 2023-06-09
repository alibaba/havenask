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
#include "suez/search/ConfigManager.h"

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "suez/common/InitParam.h"
#include "suez/heartbeat/HeartbeatTarget.h"
#include "suez/sdk/PathDefine.h"
#include "suez/table/TargetLayout.h"

using namespace std;

namespace suez {

const std::string ConfigManager::APP_INFO_NAME = "app_info";

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, ConfigManager);

ConfigManager::ConfigManager() {}

ConfigManager::~ConfigManager() {}

bool ConfigManager::init(const InitParam &param) {
    _deployManager = param.deployManager;
    _deployThreadPool = param.deployThreadPool;
    return true;
}

UPDATE_RESULT ConfigManager::update(const HeartbeatTarget &target) {
    auto targetMetas = generateConfigMetas(target);
    auto configItems = collectConfigItems(targetMetas);

    updateConfigMeta(configItems, targetMetas);
    bool needStopService = execDeploy(configItems);

    cancelUselessDeploy(targetMetas);

    return getResult(targetMetas, needStopService);
}

void ConfigManager::cleanResource(const HeartbeatTarget &target) {
    auto targetMetas = generateConfigMetas(target);

    TargetLayout layout;
    vector<ConfigId> toClearIds;
    for (const auto &it : _configMap) {
        const auto &id = it.first;
        if (targetMetas.count(id) == 0) {
            toClearIds.emplace_back(id);
            // Todo: remove dir when name not in target
        } else {
            if (target.cleanDisk()) {
                auto item = it.second;
                layout.addTarget(PathDefine::getLocalConfigBaseDir(item->getDirName()),
                                 id.name,
                                 autil::StringUtil::toString(id.version),
                                 item->getKeepCount());
            }
        }
    }

    for (const auto &id : toClearIds) {
        _configMap.erase(id);
    }
    layout.sync();
}

void ConfigManager::getTargetMetas(const HeartbeatTarget &target, BizMetas &bizMetas, AppMeta &appMeta) {
    auto targetMetas = generateConfigMetas(target);

    for (const auto &it : _configMap) {
        const auto &id = it.first;
        if (targetMetas.count(id) == 0) {
            continue;
        }
        if (id.type == CT_BIZ) {
            bizMetas.insert(make_pair(id.name, it.second->getCurrentMeta()));
        } else {
            appMeta = it.second->getCurrentMeta();
        }
    }
}

void ConfigManager::fillError(const SuezErrorCollector &errorCollector) {
    const auto &errorMap = errorCollector.getErrorMap();
    for (const auto &it : _configMap) {
        const auto &id = it.first;
        auto item = it.second;
        auto iter = errorMap.find(id.name);
        if (iter == errorMap.end()) {
            item->setError(ERROR_NONE);
        } else {
            item->setError(iter->second);
        }
    }
}

void ConfigManager::updateConfigMeta(const std::map<ConfigId, ConfigItemPtr> &configItems,
                                     const ConfigMetas &targetMetas) {
    for (const auto &it : configItems) {
        const auto &id = it.first;
        auto item = it.second;
        assert(targetMetas.count(id) > 0);
        const auto &target = targetMetas.find(id)->second;
        if (target.getKeepCount() != item->getKeepCount()) {
            item->setKeepCount(target.getKeepCount());
        }
        item->setCustomBizInfo(target.getCustomBizInfo());
    }
}

bool ConfigManager::execDeploy(const std::map<ConfigId, ConfigItemPtr> &configItems) {
    bool needStopService = false;
    for (const auto &it : configItems) {
        auto item = it.second;
        auto status = item->getStatus();
        if (!item->needDeploy()) {
            continue;
        }
        if (status != DS_DEPLOYING && status != DS_DEPLOYDONE) {
            if (status == DS_DISKQUOTA) {
                needStopService = true;
            }
            item->setStatus(DS_DEPLOYING);
            auto ret = pushTask(item);
            if (ret != autil::ThreadPool::ERROR_NONE) {
                item->setStatus(status);
            }
        }
    }
    return needStopService;
}

autil::ThreadPool::ERROR_TYPE ConfigManager::pushTask(ConfigItemPtr item) {
    auto fn = [item]() { item->deploy(); };
    return _deployThreadPool->pushTask(std::move(fn), false);
}

void ConfigManager::cancelUselessDeploy(const ConfigMetas &targetMetas) {
    for (const auto &it : _configMap) {
        const auto &id = it.first;
        if (targetMetas.count(id) == 0) {
            auto item = it.second;
            auto status = item->getStatus();
            if (status == DS_DEPLOYING) {
                item->cancel();
            }
        }
    }
}

UPDATE_RESULT ConfigManager::getResult(const ConfigMetas &targetMetas, bool needStopService) const {
    if (allConfigDownloaded(targetMetas)) {
        return UR_REACH_TARGET;
    }
    if (needStopService) {
        return UR_ERROR;
    }

    return UR_NEED_RETRY;
}

bool ConfigManager::allConfigDownloaded(const ConfigMetas &targetMetas) const {
    for (const auto &it : targetMetas) {
        const auto &id = it.first;
        const auto &itemIt = _configMap.find(id);
        if (itemIt != _configMap.end()) {
            if (itemIt->second->needDeploy() && !itemIt->second->deployDone()) {
                return false;
            }
        } else {
            AUTIL_LOG(ERROR, "not found config id:[%s, %ld, %d]", id.name.c_str(), id.version, id.type);
            return false;
        }
    }
    return true;
}

ConfigMetas ConfigManager::generateConfigMetas(const HeartbeatTarget &target) const {
    ConfigMetas configMetas;
    const auto &bizMetas = target.getBizMetas();
    for (const auto &it : bizMetas) {
        const auto &name = it.first;
        const auto &bizMeta = it.second;
        auto version = getVersionFromConfigPath(bizMeta.getRemoteConfigPath());
        configMetas.insert(make_pair(ConfigId(name, version, CT_BIZ), bizMeta));
    }
    const auto &appMeta = target.getAppMeta();
    if (appMeta.needDeploy()) {
        auto version = getVersionFromConfigPath(appMeta.getRemoteConfigPath());
        configMetas.insert(make_pair(ConfigId(ConfigManager::APP_INFO_NAME, version, CT_APP), appMeta));
    }

    return configMetas;
}

std::map<ConfigId, ConfigItemPtr> ConfigManager::collectConfigItems(const ConfigMetas &configMetas) {
    map<ConfigId, ConfigItemPtr> configItems;
    for (const auto &it : configMetas) {
        const auto &id = it.first;
        const auto &meta = it.second;
        configItems.insert(make_pair(id, getOrCreateConfig(id, meta)));
    }
    return configItems;
}

ConfigItemPtr ConfigManager::getOrCreateConfig(const ConfigId &id, const ConfigMeta &meta) {
    if (_configMap.find(id) == _configMap.end()) {
        _configMap[id] = std::make_shared<ConfigItem>(id, meta, _deployManager);
    }
    return _configMap[id];
}

int64_t ConfigManager::getVersionFromConfigPath(const string &configPath) const {
    const string versionStr = PathDefine::getVersionSuffix(configPath);
    int64_t version = 0;
    if (!versionStr.empty()) {
        version = autil::StringUtil::fromString<int64_t>(versionStr);
    }
    return version;
}

} // namespace suez
