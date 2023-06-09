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

#include <inttypes.h>
#include <memory>
#include <optional>

#include "autil/Log.h"
#include "autil/legacy/any.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"

namespace indexlib::file_system {
class LoadConfigList;
}

namespace indexlibv2::config {

class OnlineConfig;
class OfflineConfig;
class IndexTaskConfig;
class BuildConfig;
class BuildOptionConfig;
class MergeConfig;
class BackgroundTaskConfig;

class TabletOptions : public autil::legacy::Jsonizable
{
public:
    TabletOptions();
    TabletOptions(const TabletOptions& other);
    TabletOptions& operator=(const TabletOptions& other);
    ~TabletOptions();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) final override;

    Status Check(const std::string& remoteRoot, const std::string& localRoot) const;

public: // GET ANY PATH FROM RAW JSON
    template <typename T>
    bool GetFromRawJson(const std::string& jsonPath, T* value) const noexcept;
    bool IsRawJsonEmpty() const;

public: // NO-SERIALIZE OPTIONS
    const std::string& GetTabletName() const;
    bool IsLeader() const;
    bool IsOnline() const;
    bool IsReadOnly() const;
    bool FlushLocal() const;
    bool FlushRemote() const;
    bool IsLegacy() const;
    bool AutoMerge() const;

    void SetTabletName(const std::string& tabletName);
    void SetIsLeader(bool isLeader);
    void SetIsOnline(bool isOnline);
    void SetFlushLocal(bool flushLocal);
    void SetFlushRemote(bool flushRemote);
    void SetIsLegacy(bool isLegacy);
    void SetAutoMerge(bool autoMerge);

public: // SERIALIZE OPTIONS
    const OnlineConfig& GetOnlineConfig() const;
    const OfflineConfig& GetOfflineConfig() const;
    const BuildOptionConfig& GetBuildOptionConfig() const;
    const BackgroundTaskConfig& GetBackgroundTaskConfig() const;
    BackgroundTaskConfig& MutableBackgroundTaskConfig();

public: // FASTPATH OPTIONS
    // offline only
    const MergeConfig& GetMergeConfig() const;
    MergeConfig& MutableMergeConfig();
    const std::vector<IndexTaskConfig>& GetAllIndexTaskConfigs() const;
    std::vector<IndexTaskConfig> GetIndexTaskConfigs(const std::string& taskType) const;
    std::optional<IndexTaskConfig> GetIndexTaskConfig(const std::string& taskType, const std::string& taskName) const;

    // online only
    bool GetNeedReadRemoteIndex() const;
    bool GetNeedDeployIndex() const;

    // depends on IsOnline()
    const BuildConfig& GetBuildConfig() const;
    const indexlib::file_system::LoadConfigList& GetLoadConfigList() const;
    indexlib::file_system::LoadConfigList& MutableLoadConfigList();
    int64_t GetBuildMemoryQuota() const; // built + building
    int64_t GetPrintMetricsInterval() const;
    int32_t GetMaxDumpIntervalSecond() const;

public:
    // TEST
    OnlineConfig& TEST_GetOnlineConfig();
    OfflineConfig& TEST_GetOfflineConfig();
    BuildOptionConfig& TEST_GetBuildOptionConfig();
    MergeConfig& TEST_GetMergeConfig();

private:
    const autil::legacy::Any* GetAnyFromRawJson(const std::string& jsonPath) const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

/////////////////////////////////////////////////////////////////////////
template <typename T>
inline bool TabletOptions::GetFromRawJson(const std::string& jsonPath, T* value) const noexcept
{
    auto any = GetAnyFromRawJson(jsonPath);
    if (!any) {
        return false;
    }
    try {
        autil::legacy::FromJson(*value, *any);
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "decode jsonpath[%s] failed: %s", jsonPath.c_str(), e.what());
        return false;
    }
    return true;
}

} // namespace indexlibv2::config
