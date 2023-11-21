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
#include "indexlib/config/OfflineConfig.h"

#include <algorithm>
#include <iosfwd>
#include <map>
#include <set>
#include <type_traits>
#include <utility>

#include "autil/legacy/exception.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/legacy/config/OfflineMergeConfig.h"

using namespace std;

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, OfflineConfig);

struct OfflineConfig::Impl {
    BuildConfig buildConfig;
    MergeConfig mergeConfig;
    std::vector<IndexTaskConfig> indexTaskConfigs;
    indexlib::file_system::LoadConfigList loadConfigList;
};

OfflineConfig::OfflineConfig() : _impl(make_unique<OfflineConfig::Impl>()) {}
OfflineConfig::OfflineConfig(const OfflineConfig& other) : _impl(make_unique<OfflineConfig::Impl>(*other._impl)) {}
OfflineConfig& OfflineConfig::operator=(const OfflineConfig& other)
{
    if (this != &other) {
        _impl = make_unique<OfflineConfig::Impl>(*other._impl);
    }
    return *this;
}
OfflineConfig::~OfflineConfig() {}

void OfflineConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("merge_config", _impl->mergeConfig, _impl->mergeConfig);
    json.Jsonize("build_config", _impl->buildConfig, _impl->buildConfig);
    json.Jsonize("index_task_configs", _impl->indexTaskConfigs, _impl->indexTaskConfigs);
    _impl->loadConfigList.Jsonize(json);
    // "load_config"
    if (json.GetMode() == FROM_JSON) {
        if (_impl->indexTaskConfigs.size() == 0) {
            std::map<std::string, indexlib::legacy::config::OfflineMergeConfig> customizeMergeConfigMap;
            json.Jsonize("customized_merge_config", customizeMergeConfigMap, customizeMergeConfigMap);
            for (const auto& [name, mergeConfig] : customizeMergeConfigMap) {
                IndexTaskConfig indexTaskConfig;
                indexTaskConfig.FillLegacyMergeConfig(name, mergeConfig);
                _impl->indexTaskConfigs.push_back(indexTaskConfig);
                if (mergeConfig.NeedReclaimTask()) {
                    IndexTaskConfig reclaimTaskConfig;
                    reclaimTaskConfig.FillLegacyReclaimConfig(name, mergeConfig);
                    _impl->indexTaskConfigs.push_back(reclaimTaskConfig);
                }
            }
        }
        for (auto& indexTaskConfig : _impl->indexTaskConfigs) {
            if (indexTaskConfig.GetTaskType() == "merge") {
                indexTaskConfig.RewriteWithDefaultMergeConfig(_impl->mergeConfig);
            }
        }

        _impl->loadConfigList.Check();
        Check();
    }
}

void OfflineConfig::Check() const
{
    std::set<std::pair<std::string, std::string>> dedupSet;
    for (const auto& indexTaskConfig : _impl->indexTaskConfigs) {
        auto taskType = indexTaskConfig.GetTaskType();
        auto taskName = indexTaskConfig.GetTaskName();
        if (taskType.empty() || taskName.empty()) {
            std::string errMsg = "invalid index_task_config with taskType [" + taskType + "] taskName [" + taskName +
                                 "], type or name should not be empty.";
            AUTIL_LOG(ERROR, "%s", errMsg.c_str());
            AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException, errMsg);
        }
        auto trigger = indexTaskConfig.GetTrigger();
        if (trigger.GetTriggerType() == Trigger::TriggerType::ERROR) {
            std::string errMsg = "invalid index_task_config with taskType [" + taskType + "] taskName [" + taskName +
                                 "], trigger is invalid.";
            AUTIL_LOG(ERROR, "%s", errMsg.c_str());
            AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException, errMsg);
        }

        if (dedupSet.find(std::pair(taskType, taskName)) != dedupSet.end()) {
            std::string errMsg =
                "index_task_config with taskType [" + taskType + "] taskName [" + taskName + "] already exist.";
            AUTIL_LOG(ERROR, "%s", errMsg.c_str());
            AUTIL_LEGACY_THROW(autil::legacy::AlreadyExistException, errMsg);
        }
        dedupSet.insert(std::pair(taskType, taskName));
    }
}

const BuildConfig& OfflineConfig::GetBuildConfig() const { return _impl->buildConfig; }
const MergeConfig& OfflineConfig::GetDefaultMergeConfig() const { return _impl->mergeConfig; }

const std::vector<IndexTaskConfig>& OfflineConfig::GetAllIndexTaskConfigs() const { return _impl->indexTaskConfigs; }

std::vector<IndexTaskConfig> OfflineConfig::GetIndexTaskConfigs(const std::string& taskType) const
{
    std::vector<IndexTaskConfig> ret;
    for (const auto& indexTaskConfig : _impl->indexTaskConfigs) {
        if (indexTaskConfig.GetTaskType() == taskType) {
            ret.push_back(indexTaskConfig);
        }
    }
    return ret;
}

std::optional<IndexTaskConfig> OfflineConfig::GetIndexTaskConfig(const std::string& taskType,
                                                                 const std::string& taskName) const
{
    for (const auto& indexTaskConfig : _impl->indexTaskConfigs) {
        if (indexTaskConfig.GetTaskType() == taskType && indexTaskConfig.GetTaskName() == taskName) {
            return std::make_optional<IndexTaskConfig>(indexTaskConfig);
        }
    }
    AUTIL_LOG(WARN, "get index task config for [%s].[%s] failed", taskType.c_str(), taskName.c_str());
    return std::nullopt;
}

const indexlib::file_system::LoadConfigList& OfflineConfig::GetLoadConfigList() const { return _impl->loadConfigList; }
indexlib::file_system::LoadConfigList& OfflineConfig::MutableLoadConfigList() { return _impl->loadConfigList; }
BuildConfig& OfflineConfig::TEST_GetBuildConfig() { return _impl->buildConfig; }
MergeConfig& OfflineConfig::TEST_GetMergeConfig() { return _impl->mergeConfig; }
void OfflineConfig::TEST_AddIndexTaskConfig(const IndexTaskConfig& indexTaskConfig)
{
    _impl->indexTaskConfigs.push_back(indexTaskConfig);
}
std::vector<IndexTaskConfig>& OfflineConfig::TEST_GetIndexTaskConfigs() { return _impl->indexTaskConfigs; }
indexlib::file_system::LoadConfigList& OfflineConfig::TEST_GetLoadConfigList() { return _impl->loadConfigList; }

} // namespace indexlibv2::config
