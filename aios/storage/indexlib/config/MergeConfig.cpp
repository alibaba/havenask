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
#include "indexlib/config/MergeConfig.h"

#include <mutex>
#include <unistd.h>

#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "indexlib/config/MergeStrategyParameter.h"
#include "indexlib/file_system/package/PackageFileTagConfigList.h"
#include "indexlib/util/Singleton.h"

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, MergeConfig);

struct MergeConfigHook : indexlib::util::Singleton<MergeConfigHook> {
    std::mutex mutex;
    std::map<std::string, std::pair<MergeConfig::OptionInitHook, MergeConfig::OptionJsonHook>> optionHooks;
};

struct MergeConfig::Impl {
    std::string mergeStrategyStr = "optimize"; // OPTIMIZE_MERGE_STRATEGY_STR
    int64_t maxMergeMemoryUseMB = 40 * 1024;   // 40GB
    uint32_t mergeThreadCount = 20;
    MergeStrategyParameter mergeStrategyParameter;
    bool enablePatchFileArchive = false;
    bool enablePatchFileMeta = false;
    bool enablePackageFile = false;
    uint32_t packageFileSizeThresholdBytes = 64 * 1024 * 1024; // 64MB
    uint32_t mergePackageThreadCount = 0;
    indexlib::file_system::PackageFileTagConfigList packageFileTagConfigList; // valid when enablePackageFile == true
    std::map<std::string, std::any> hookOptions;
};

void MergeConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("merge_strategy", _impl->mergeStrategyStr, _impl->mergeStrategyStr);
    json.Jsonize("merge_strategy_params", _impl->mergeStrategyParameter, _impl->mergeStrategyParameter);
    json.Jsonize("max_merge_memory_use", _impl->maxMergeMemoryUseMB, _impl->maxMergeMemoryUseMB);
    json.Jsonize("merge_thread_count", _impl->mergeThreadCount, _impl->mergeThreadCount);
    json.Jsonize("enable_patch_file_archive", _impl->enablePatchFileArchive, _impl->enablePatchFileArchive);
    json.Jsonize("enable_patch_file_meta", _impl->enablePatchFileMeta, _impl->enablePatchFileMeta);
    json.Jsonize("enable_package_file", _impl->enablePackageFile, _impl->enablePackageFile);
    json.Jsonize("package_file_size_threshold_bytes", _impl->packageFileSizeThresholdBytes,
                 _impl->packageFileSizeThresholdBytes);
    json.Jsonize("merge_package_thread_count", _impl->mergePackageThreadCount, _impl->mergePackageThreadCount);
    _impl->packageFileTagConfigList.Jsonize(json);

    if (json.GetMode() == FROM_JSON) {
        // support legacy format
        if (json.GetMap().count("merge_strategy_params") == 0) {
            std::string legacyStr;
            json.Jsonize("merge_strategy_param", legacyStr, legacyStr);
            _impl->mergeStrategyParameter.SetLegacyString(legacyStr);
        }
    }
    for (const auto& [_, hook] : MergeConfigHook::GetInstance()->optionHooks) {
        hook.second(json, _impl->hookOptions);
    }
    Check();
}

void MergeConfig::Check() const
{
    if (_impl->maxMergeMemoryUseMB == 0) {
        AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException, "max_merge_memory_use should be greater than 0.");
    }
    static const uint32_t MAX_MERGE_THREAD_COUNT = 20;
    if (_impl->mergeThreadCount == 0 || _impl->mergeThreadCount > MAX_MERGE_THREAD_COUNT) {
        AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException,
                           "merge_thread_count should be greater than 0 and no more than " +
                               std::to_string(MAX_MERGE_THREAD_COUNT));
    }
}

MergeConfig::MergeConfig() : _impl(std::make_unique<MergeConfig::Impl>())
{
    for (const auto& [_, hook] : MergeConfigHook::GetInstance()->optionHooks) {
        hook.first(_impl->hookOptions);
    }
}
MergeConfig::MergeConfig(const MergeConfig& other) : _impl(std::make_unique<MergeConfig::Impl>(*other._impl)) {}
MergeConfig& MergeConfig::operator=(const MergeConfig& other)
{
    if (this != &other) {
        _impl = std::make_unique<MergeConfig::Impl>(*other._impl);
    }
    return *this;
}
MergeConfig::~MergeConfig() {}

const std::string& MergeConfig::GetMergeStrategyStr() const { return _impl->mergeStrategyStr; }
int64_t MergeConfig::GetMaxMergeMemoryUse() const { return _impl->maxMergeMemoryUseMB * 1024 * 1024; }
uint32_t MergeConfig::GetMergeThreadCount() const { return _impl->mergeThreadCount; }
const MergeStrategyParameter& MergeConfig::GetMergeStrategyParameter() const { return _impl->mergeStrategyParameter; }
bool MergeConfig::EnablePatchFileMeta() const { return _impl->enablePatchFileMeta; }
bool MergeConfig::EnablePatchFileArchive() const { return _impl->enablePatchFileArchive; }

bool MergeConfig::IsPackageFileEnabled() const { return _impl->enablePackageFile; }
uint32_t MergeConfig::GetPackageFileSizeThresholdBytes() const { return _impl->packageFileSizeThresholdBytes; }
uint32_t MergeConfig::GetMergePackageThreadCount() const { return _impl->mergePackageThreadCount; }
const indexlib::file_system::PackageFileTagConfigList& MergeConfig::GetPackageFileTagConfigList() const
{
    return _impl->packageFileTagConfigList;
}
void MergeConfig::RegisterOptionHook(const std::string& hookName, const OptionInitHook& initHook,
                                     const OptionJsonHook& jsonHook)
{
    std::lock_guard<std::mutex> lock(MergeConfigHook::GetInstance()->mutex);
    if (const auto& [_, success] =
            MergeConfigHook::GetInstance()->optionHooks.try_emplace(hookName, initHook, jsonHook);
        !success) {
        AUTIL_LOG(ERROR, "hook [%s] already exists", hookName.c_str());
    }
}
const std::any* MergeConfig::GetHookOption(const std::string& name) const
{
    auto it = _impl->hookOptions.find(name);
    if (it != _impl->hookOptions.end()) {
        return &it->second;
    }
    AUTIL_LOG(WARN, "hook option [%s] does not exists", name.c_str());
    return nullptr;
}
std::any* MergeConfig::TEST_GetHookOption(const std::string& name)
{
    return const_cast<std::any*>(GetHookOption(name));
}

void MergeConfig::TEST_SetMergeStrategyParameterStr(const std::string& paramStr)
{
    try {
        autil::legacy::FromJsonString(_impl->mergeStrategyParameter, paramStr);
    } catch (...) {
        _impl->mergeStrategyParameter.SetLegacyString(paramStr);
    }
}
void MergeConfig::TEST_SetMergeStrategyStr(const std::string& mergeStrategyStr)
{
    _impl->mergeStrategyStr = mergeStrategyStr;
}
void MergeConfig::TEST_SetMaxMergeMemoryUse(int64_t maxMergeMemoryUse)
{
    _impl->maxMergeMemoryUseMB = maxMergeMemoryUse / 1024 / 1024;
}
void MergeConfig::TEST_SetMergeThreadCount(uint32_t count) { _impl->mergeThreadCount = count; }
void MergeConfig::TEST_SetEnablePatchFileMeta(bool enabled) { _impl->enablePatchFileMeta = enabled; }
void MergeConfig::TEST_SetEnablePatchFileArchive(bool enabled) { _impl->enablePatchFileArchive = enabled; }
void MergeConfig::TEST_SetEnablePackageFile(bool enabled) { _impl->enablePackageFile = enabled; }
void MergeConfig::TEST_SetPackageFileSizeThresholdBytes(uint32_t size) { _impl->packageFileSizeThresholdBytes = size; }
void MergeConfig::TEST_SetMergePackageThreadCount(uint32_t count) { _impl->mergePackageThreadCount = count; }
void MergeConfig::TEST_SetPackageFileTagConfigList(const indexlib::file_system::PackageFileTagConfigList& list)
{
    _impl->packageFileTagConfigList = list;
}
void MergeConfig::TEST_SetMergeStrategyParameter(const MergeStrategyParameter& param)
{
    _impl->mergeStrategyParameter = param;
}

} // namespace indexlibv2::config
