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
#include "indexlib/config/OnlineConfig.h"

#include "indexlib/config/BuildConfig.h"
#include "indexlib/file_system/LifecycleConfig.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"

using namespace std;

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, OnlineConfig);

struct OnlineConfig::Impl {
    BuildConfig buildConfig;
    indexlib::file_system::LifecycleConfig lifecycleConfig;
    indexlib::file_system::LoadConfigList loadConfigList;
    int64_t maxRealtimeMemoryUseMB = 8 * 1024; // 8G
    int64_t printMetricsInterval = 1200;       // 20min
    int32_t maxRealtimeDumpInterval = -1;
    bool needDeployIndex = true;      // means needReadLocalIndex
    bool needReadRemoteIndex = false; // TODO: make it always true
    bool supportAlterTableWithDefaultValue = true;
    bool isIncConsistentWithRealtime = false;
    bool allowLocatorRollback = false;
    bool EnableLocalDeployManifestChecking() const
    {
        return needDeployIndex && lifecycleConfig.EnableLocalDeployManifestChecking();
    }
    bool loadRemainFlushRealtimeIndex = false;
};

OnlineConfig::OnlineConfig() : _impl(make_unique<OnlineConfig::Impl>()) { _impl->buildConfig.InitForOnline(); }
OnlineConfig::OnlineConfig(const OnlineConfig& other) : _impl(make_unique<OnlineConfig::Impl>(*other._impl)) {}
OnlineConfig& OnlineConfig::operator=(const OnlineConfig& other)
{
    if (this != &other) {
        _impl = make_unique<OnlineConfig::Impl>(*other._impl);
    }
    return *this;
}
OnlineConfig::~OnlineConfig() {}

void OnlineConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("build_config", _impl->buildConfig, _impl->buildConfig);
    json.Jsonize("max_realtime_memory_use", _impl->maxRealtimeMemoryUseMB, _impl->maxRealtimeMemoryUseMB);
    json.Jsonize("max_realtime_dump_interval", _impl->maxRealtimeDumpInterval, _impl->maxRealtimeDumpInterval);
    json.Jsonize("print_metrics_interval", _impl->printMetricsInterval, _impl->printMetricsInterval);
    json.Jsonize("need_read_remote_index", _impl->needReadRemoteIndex, _impl->needReadRemoteIndex);
    json.Jsonize("need_deploy_index", _impl->needDeployIndex, _impl->needDeployIndex);
    json.Jsonize("support_alter_table_with_default_value", _impl->supportAlterTableWithDefaultValue,
                 _impl->supportAlterTableWithDefaultValue);
    json.Jsonize("lifecycle_config", _impl->lifecycleConfig, _impl->lifecycleConfig);
    json.Jsonize("load_remain_flush_realtime_index", _impl->loadRemainFlushRealtimeIndex,
                 _impl->loadRemainFlushRealtimeIndex);
    json.Jsonize("is_inc_consistent_with_realtime", _impl->isIncConsistentWithRealtime,
                 _impl->isIncConsistentWithRealtime);
    json.Jsonize("allow_locator_rollback", _impl->allowLocatorRollback, _impl->allowLocatorRollback);

    // "load_config"
    _impl->loadConfigList.Jsonize(json);
    if (json.GetMode() == FROM_JSON) {
        if (!_impl->needDeployIndex) {
            _impl->loadConfigList.SetLoadMode(indexlib::file_system::LoadConfig::LoadMode::REMOTE_ONLY);
            if (!_impl->needReadRemoteIndex) {
                AUTIL_LOG(WARN, "needDeployIndex is false, so needReadRemoteIndex must be true, rewrite it");
                _impl->needReadRemoteIndex = true;
            }
        } else {
            if (!_impl->needReadRemoteIndex) {
                _impl->loadConfigList.SetLoadMode(indexlib::file_system::LoadConfig::LoadMode::LOCAL_ONLY);
            }
        }
    }

    _impl->loadConfigList.Check();
    Check();
}

void OnlineConfig::Check() const {}

const BuildConfig& OnlineConfig::GetBuildConfig() const { return _impl->buildConfig; }

const indexlib::file_system::LifecycleConfig& OnlineConfig::GetLifecycleConfig() const
{
    return _impl->lifecycleConfig;
}

bool OnlineConfig::IsIncConsistentWithRealtime() const { return _impl->isIncConsistentWithRealtime; }

const indexlib::file_system::LoadConfigList& OnlineConfig::GetLoadConfigList() const { return _impl->loadConfigList; }
indexlib::file_system::LoadConfigList& OnlineConfig::MutableLoadConfigList() { return _impl->loadConfigList; }

void OnlineConfig::TEST_SetLoadConfigList(const std::string& jsonStr)
{
    FromJsonString(_impl->loadConfigList, jsonStr);
}
int64_t OnlineConfig::GetMaxRealtimeMemoryUse() const { return _impl->maxRealtimeMemoryUseMB * 1024 * 1024; }
int64_t OnlineConfig::GetPrintMetricsInterval() const { return _impl->printMetricsInterval; }
int32_t OnlineConfig::GetMaxRealtimeDumpIntervalSecond() const { return _impl->maxRealtimeDumpInterval; }
bool OnlineConfig::GetNeedReadRemoteIndex() const { return _impl->needReadRemoteIndex; }
bool OnlineConfig::GetNeedDeployIndex() const { return _impl->needDeployIndex; }
bool OnlineConfig::SupportAlterTableWithDefaultValue() const { return _impl->supportAlterTableWithDefaultValue; }
bool OnlineConfig::EnableLocalDeployManifestChecking() const { return _impl->EnableLocalDeployManifestChecking(); }
bool OnlineConfig::LoadRemainFlushRealtimeIndex() const { return _impl->loadRemainFlushRealtimeIndex; }
bool OnlineConfig::GetAllowLocatorRollback() const { return _impl->allowLocatorRollback; }

BuildConfig& OnlineConfig::TEST_GetBuildConfig() { return _impl->buildConfig; }
void OnlineConfig::TEST_SetMaxRealtimeMemoryUse(int64_t quota) { _impl->maxRealtimeMemoryUseMB = quota / 1024 / 1024; }
void OnlineConfig::TEST_SetPrintMetricsInterval(int64_t interval) { _impl->printMetricsInterval = interval; }
void OnlineConfig::TEST_SetMaxRealtimeDumpIntervalSecond(uint32_t interval)
{
    _impl->maxRealtimeDumpInterval = interval;
}
void OnlineConfig::TEST_SetNeedDeployIndex(bool needDeployIndex) { _impl->needDeployIndex = needDeployIndex; }
void OnlineConfig::TEST_SetNeedReadRemoteIndex(bool needReadRemoteIndex)
{
    _impl->needReadRemoteIndex = needReadRemoteIndex;
}
void OnlineConfig::TEST_SetLoadRemainFlushRealtimeIndex(bool loadRemainFlushRealtimeIndex)
{
    _impl->loadRemainFlushRealtimeIndex = loadRemainFlushRealtimeIndex;
}

void OnlineConfig::TEST_SetAllowLocatorRollback(bool allow) { _impl->allowLocatorRollback = allow; }

} // namespace indexlibv2::config
