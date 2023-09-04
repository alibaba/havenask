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

#include <any>
#include <functional>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"

namespace indexlib::file_system {
class PackageFileTagConfigList;
}
namespace indexlibv2::config {
class MergeStrategyParameter;

class MergeConfig : public autil::legacy::Jsonizable
{
public:
    MergeConfig();
    MergeConfig(const MergeConfig& other);
    MergeConfig& operator=(const MergeConfig& other);
    ~MergeConfig();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;

public:
    const std::string& GetMergeStrategyStr() const;
    int64_t GetMaxMergeMemoryUse() const;
    uint32_t GetMergeThreadCount() const;
    const MergeStrategyParameter& GetMergeStrategyParameter() const;
    bool EnablePatchFileMeta() const;
    bool EnablePatchFileArchive() const;
    bool IsPackageFileEnabled() const;
    uint32_t GetPackageFileSizeThresholdBytes() const;
    uint32_t GetMergePackageThreadCount() const;
    const indexlib::file_system::PackageFileTagConfigList& GetPackageFileTagConfigList() const;

public:
    // hook
    typedef std::function<void(std::map<std::string, std::any>&)> OptionInitHook;
    typedef std::function<void(autil::legacy::Jsonizable::JsonWrapper&, std::map<std::string, std::any>&)>
        OptionJsonHook;
    static void RegisterOptionHook(const std::string& hookName, const OptionInitHook& initHook,
                                   const OptionJsonHook& jsonHook);
    const std::any* GetHookOption(const std::string& name) const;
    std::any* TEST_GetHookOption(const std::string& name);

public:
    void TEST_SetMergeStrategyStr(const std::string& mergeStrategyStr);
    void TEST_SetMergeStrategyParameterStr(const std::string& paramStr);
    void TEST_SetMergeStrategyParameter(const MergeStrategyParameter& param);
    void TEST_SetMaxMergeMemoryUse(int64_t maxMemUseForMerge);
    void TEST_SetMergeThreadCount(uint32_t count);
    void TEST_SetEnablePatchFileMeta(bool enabled);
    void TEST_SetEnablePatchFileArchive(bool enabled);
    void TEST_SetEnablePackageFile(bool enable);
    void TEST_SetPackageFileSizeThresholdBytes(uint32_t bytes);
    void TEST_SetMergePackageThreadCount(uint32_t count);
    void TEST_SetPackageFileTagConfigList(const indexlib::file_system::PackageFileTagConfigList& list);

public:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
