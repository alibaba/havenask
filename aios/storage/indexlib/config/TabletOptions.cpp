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
#include "indexlib/config/TabletOptions.h"

#include <assert.h>
#include <limits>
#include <stddef.h>

#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/config/BackgroundTaskConfig.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/BuildOptionConfig.h"
#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/OnlineConfig.h"

namespace indexlibv2::config {

AUTIL_LOG_SETUP(indexlib.config, TabletOptions);

struct TabletOptions::Impl {
    // persistent options
    OnlineConfig onlineConfig;
    OfflineConfig offlineConfig;
    BuildOptionConfig buildOptionConfig;
    BackgroundTaskConfig backgroundTaskConfig;

    // runtime options
    std::string tabletName;
    bool isLeader = false;
    bool isOnline = true;
    bool flushLocal = true;
    bool flushRemote = false;
    bool isLegacy = false; // true for IndexPartition, false for Tablet
    bool autoMerge = true;

    MutableJson rawJson;
};

TabletOptions::TabletOptions() : _impl(std::make_unique<TabletOptions::Impl>()) {}
TabletOptions::TabletOptions(const TabletOptions& other) : _impl(std::make_unique<TabletOptions::Impl>(*other._impl)) {}
TabletOptions& TabletOptions::operator=(const TabletOptions& other)
{
    if (this != &other) {
        _impl = std::make_unique<TabletOptions::Impl>(*other._impl);
    }
    return *this;
}
TabletOptions::~TabletOptions() {}

void TabletOptions::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("online_index_config", _impl->onlineConfig, _impl->onlineConfig);
    json.Jsonize("offline_index_config", _impl->offlineConfig, _impl->offlineConfig);
    json.Jsonize("build_option_config", _impl->buildOptionConfig, _impl->buildOptionConfig);
    json.Jsonize("background_task_config", _impl->backgroundTaskConfig, _impl->backgroundTaskConfig);
    if (!_impl->backgroundTaskConfig.initFromEnv()) {
        AUTIL_LOG(ERROR, "parse back ground task config from env failed");
    }
    if (json.GetMode() == FROM_JSON) {
        [[maybe_unused]] auto r = _impl->rawJson.SetValue("", json.GetMap());
        assert(r);
    }
}

bool TabletOptions::IsRawJsonEmpty() const { return _impl->rawJson.IsEmpty(); }

Status TabletOptions::Check(const std::string& remoteRoot, const std::string& localRoot) const
{
    if (FlushRemote() && FlushLocal()) {
        AUTIL_LOG(ERROR, "flush remote and local simultaneously");
        return Status::InvalidArgs("flush remote and local simultaneously");
    }
    if (FlushRemote() && remoteRoot.empty()) {
        AUTIL_LOG(ERROR, "flush remote, but remote root null");
        return Status::InvalidArgs("flush remote, but remote root null");
    }
    if (FlushLocal() && localRoot.empty()) {
        AUTIL_LOG(ERROR, "flush local, but local root null");
        return Status::InvalidArgs("flush local, but local root null");
    }
    if (!FlushLocal() && !FlushRemote()) {
        AUTIL_LOG(INFO, "tablet open in read-only mode, cannot build");
    }
    if (IsOnline() && GetOnlineConfig().GetMaxRealtimeMemoryUse() < GetBuildConfig().GetBuildingMemoryLimit()) {
        AUTIL_LOG(ERROR,
                  "online_index_config.max_realtime_memory_use [%ld] is less than "
                  "online_index_config.build_config.building_memory_limit_mb [%ld]",
                  GetOnlineConfig().GetMaxRealtimeMemoryUse(), GetBuildConfig().GetBuildingMemoryLimit());
        return Status::InvalidArgs("memory config invalid");
    }
    return Status::OK();
}

const std::string& TabletOptions::GetTabletName() const { return _impl->tabletName; }
bool TabletOptions::IsLeader() const { return _impl->isLeader; }
bool TabletOptions::IsOnline() const { return _impl->isOnline; }
bool TabletOptions::FlushLocal() const { return _impl->flushLocal; }
bool TabletOptions::FlushRemote() const { return _impl->flushRemote; }
bool TabletOptions::IsLegacy() const { return _impl->isLegacy; }
bool TabletOptions::AutoMerge() const { return _impl->autoMerge; }
bool TabletOptions::IsReadOnly() const { return !_impl->flushRemote && !_impl->flushLocal; }

void TabletOptions::SetTabletName(const std::string& tabletName) { _impl->tabletName = tabletName; }
void TabletOptions::SetIsLeader(bool isLeader) { _impl->isLeader = isLeader; }
void TabletOptions::SetIsOnline(bool isOnline) { _impl->isOnline = isOnline; }
void TabletOptions::SetFlushLocal(bool flushLocal) { _impl->flushLocal = flushLocal; }
void TabletOptions::SetFlushRemote(bool flushRemote) { _impl->flushRemote = flushRemote; }
void TabletOptions::SetIsLegacy(bool isLegacy) { _impl->isLegacy = isLegacy; }
void TabletOptions::SetAutoMerge(bool autoMerge) { _impl->autoMerge = autoMerge; }

const OnlineConfig& TabletOptions::GetOnlineConfig() const { return _impl->onlineConfig; }
const OfflineConfig& TabletOptions::GetOfflineConfig() const { return _impl->offlineConfig; }
const BuildOptionConfig& TabletOptions::GetBuildOptionConfig() const { return _impl->buildOptionConfig; }
const BackgroundTaskConfig& TabletOptions::GetBackgroundTaskConfig() const { return _impl->backgroundTaskConfig; }
BackgroundTaskConfig& TabletOptions::MutableBackgroundTaskConfig() { return _impl->backgroundTaskConfig; }

const MergeConfig& TabletOptions::GetDefaultMergeConfig() const { return GetOfflineConfig().GetDefaultMergeConfig(); }
// MergeConfig& TabletOptions::MutableMergeConfig() { return _impl->offlineConfig.MutableMergeConfig(); }

const std::vector<IndexTaskConfig>& TabletOptions::GetAllIndexTaskConfigs() const
{
    return GetOfflineConfig().GetAllIndexTaskConfigs();
}

std::vector<IndexTaskConfig> TabletOptions::GetIndexTaskConfigs(const std::string& taskType) const
{
    return GetOfflineConfig().GetIndexTaskConfigs(taskType);
}

std::optional<IndexTaskConfig> TabletOptions::GetIndexTaskConfig(const std::string& taskType,
                                                                 const std::string& taskName) const
{
    return GetOfflineConfig().GetIndexTaskConfig(taskType, taskName);
}

bool TabletOptions::GetNeedReadRemoteIndex() const { return GetOnlineConfig().GetNeedReadRemoteIndex(); }
bool TabletOptions::GetNeedDeployIndex() const { return GetOnlineConfig().GetNeedDeployIndex(); }

const BuildConfig& TabletOptions::GetBuildConfig() const
{
    return IsOnline() ? GetOnlineConfig().GetBuildConfig() : GetOfflineConfig().GetBuildConfig();
}

const indexlib::file_system::LoadConfigList& TabletOptions::GetLoadConfigList() const
{
    return IsOnline() ? GetOnlineConfig().GetLoadConfigList() : GetOfflineConfig().GetLoadConfigList();
}

indexlib::file_system::LoadConfigList& TabletOptions::MutableLoadConfigList()
{
    return IsOnline() ? _impl->onlineConfig.MutableLoadConfigList() : _impl->offlineConfig.MutableLoadConfigList();
}
int64_t TabletOptions::GetBuildMemoryQuota() const
{
    return IsOnline() ? GetOnlineConfig().GetMaxRealtimeMemoryUse() : std::numeric_limits<int64_t>::max();
}
int64_t TabletOptions::GetPrintMetricsInterval() const
{
    return IsOnline() ? GetOnlineConfig().GetPrintMetricsInterval() : std::numeric_limits<int64_t>::max();
}
int32_t TabletOptions::GetMaxDumpIntervalSecond() const
{
    return IsOnline() ? GetOnlineConfig().GetMaxRealtimeDumpIntervalSecond()
                      : _impl->backgroundTaskConfig.GetDumpIntervalMs() / 1000;
}

OnlineConfig& TabletOptions::TEST_GetOnlineConfig() { return _impl->onlineConfig; }
OfflineConfig& TabletOptions::TEST_GetOfflineConfig() { return _impl->offlineConfig; }
BuildOptionConfig& TabletOptions::TEST_GetBuildOptionConfig() { return _impl->buildOptionConfig; }

const autil::legacy::Any* TabletOptions::GetAnyFromRawJson(const std::string& jsonPath) const noexcept
{
    static const std::string SMART_INDEX_CONFIG = "%index_config%";
    if (size_t pos = jsonPath.find(SMART_INDEX_CONFIG); pos != std::string::npos) {
        assert(pos == 0);
        auto newJsonPath = jsonPath;
        newJsonPath.replace(pos, SMART_INDEX_CONFIG.size(),
                            IsOnline() ? "online_index_config" : "offline_index_config");
        return _impl->rawJson.GetAny(newJsonPath);
    }
    return _impl->rawJson.GetAny(jsonPath);
}
MergeConfig& TabletOptions::TEST_GetMergeConfig() { return TEST_GetOfflineConfig().TEST_GetMergeConfig(); }
MutableJson& TabletOptions::TEST_GetRawJson() { return _impl->rawJson; }
} // namespace indexlibv2::config
