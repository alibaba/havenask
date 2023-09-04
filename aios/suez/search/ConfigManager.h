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
#pragma once

#include "autil/ThreadPool.h"
#include "suez/sdk/SuezError.h"
#include "suez/sdk/SuezPartitionType.h"
#include "suez/search/ConfigItem.h"

namespace suez {

struct InitParam;
class TargetLayout;
class HeartbeatTarget;

class ConfigManager {
public:
    ConfigManager();
    virtual ~ConfigManager();

public:
    bool init(const InitParam &param);
    UPDATE_RESULT update(const HeartbeatTarget &target);
    void cleanResource(const HeartbeatTarget &target);
    void getTargetMetas(const HeartbeatTarget &target, BizMetas &bizMetas, AppMeta &appMeta);
    void fillError(const SuezErrorCollector &errorCollector);
    SuezError getGlobalError() const { return ERROR_NONE; }

private:
    void updateConfigMeta(const std::map<ConfigId, ConfigItemPtr> &configItems, const ConfigMetas &targetMetas);
    bool execDeploy(const std::map<ConfigId, ConfigItemPtr> &configItems);
    virtual autil::ThreadPool::ERROR_TYPE pushTask(ConfigItemPtr item);
    void cancelUselessDeploy(const ConfigMetas &targetMetas);
    UPDATE_RESULT getResult(const ConfigMetas &targetMetas, bool needStopService) const;
    bool allConfigDownloaded(const ConfigMetas &targetMetas) const;
    bool findLoop(const ConfigMetas &targetMetas) const;
    ConfigMetas generateConfigMetas(const HeartbeatTarget &target) const;
    std::map<ConfigId, ConfigItemPtr> collectConfigItems(const ConfigMetas &configMetas);
    virtual ConfigItemPtr getOrCreateConfig(const ConfigId &id, const ConfigMeta &meta);
    int64_t getVersionFromConfigPath(const std::string &configPath) const;

private:
    DeployManager *_deployManager;
    autil::ThreadPool *_deployThreadPool;
    std::map<ConfigId, ConfigItemPtr> _configMap; // current + target

public:
    static const std::string APP_INFO_NAME;
};

} // namespace suez
