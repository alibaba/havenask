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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"

namespace indexlib::file_system {
class LifecycleConfig;
class LoadConfigList;
} // namespace indexlib::file_system

namespace indexlibv2::config {

class BuildConfig;

class OnlineConfig : public autil::legacy::Jsonizable
{
public:
    OnlineConfig();
    OnlineConfig(const OnlineConfig& other);
    OnlineConfig& operator=(const OnlineConfig& other);
    ~OnlineConfig();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;

public:
    const BuildConfig& GetBuildConfig() const;
    const indexlib::file_system::LifecycleConfig& GetLifecycleConfig() const;
    const indexlib::file_system::LoadConfigList& GetLoadConfigList() const;
    indexlib::file_system::LoadConfigList& MutableLoadConfigList();
    int64_t GetMaxRealtimeMemoryUse() const;
    int64_t GetPrintMetricsInterval() const;
    int32_t GetMaxRealtimeDumpIntervalSecond() const;
    bool GetNeedReadRemoteIndex() const;
    bool GetNeedDeployIndex() const;
    bool SupportAlterTableWithDefaultValue() const;
    bool EnableLocalDeployManifestChecking() const;
    bool LoadRemainFlushRealtimeIndex() const;
    bool IsIncConsistentWithRealtime() const;
    bool GetAllowLocatorRollback() const;

public:
    BuildConfig& TEST_GetBuildConfig();
    void TEST_SetLoadConfigList(const std::string& jsonStr);
    void TEST_SetMaxRealtimeMemoryUse(int64_t quota);
    void TEST_SetPrintMetricsInterval(int64_t interval);
    void TEST_SetMaxRealtimeDumpIntervalSecond(uint32_t interval);
    void TEST_SetNeedDeployIndex(bool needDeployIndex);
    void TEST_SetNeedReadRemoteIndex(bool needReadRemoteIndex);
    void TEST_SetLoadRemainFlushRealtimeIndex(bool loadRemainFlushRealtimeIndex);
    void TEST_SetAllowLocatorRollback(bool allow);

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
