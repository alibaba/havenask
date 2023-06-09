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
#include <memory>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"

namespace indexlib::file_system {
class LoadConfigList;
}

namespace indexlibv2::config {

class BuildConfig;
class MergeConfig;
class IndexTaskConfig;

class OfflineConfig : public autil::legacy::Jsonizable
{
public:
    OfflineConfig();
    OfflineConfig(const OfflineConfig& other);
    OfflineConfig& operator=(const OfflineConfig& other);
    ~OfflineConfig();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;

public:
    const BuildConfig& GetBuildConfig() const;
    const MergeConfig& GetMergeConfig() const;
    MergeConfig& MutableMergeConfig();

    const std::vector<IndexTaskConfig>& GetAllIndexTaskConfigs() const;
    std::optional<IndexTaskConfig> GetIndexTaskConfig(const std::string& taskType, const std::string& taskName) const;
    std::vector<IndexTaskConfig> GetIndexTaskConfigs(const std::string& taskType) const;
    const indexlib::file_system::LoadConfigList& GetLoadConfigList() const;
    indexlib::file_system::LoadConfigList& MutableLoadConfigList();

public:
    MergeConfig& TEST_GetMergeConfig();
    BuildConfig& TEST_GetBuildConfig();
    std::vector<IndexTaskConfig>& TEST_GetIndexTaskConfigs();
    indexlib::file_system::LoadConfigList& TEST_GetLoadConfigList();

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
