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
#include "indexlib/config/merge_config_base.h"
#include "indexlib/config/module_class_config.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib::config {
class MergeConfigImpl;
typedef std::shared_ptr<MergeConfigImpl> MergeConfigImplPtr;
} // namespace indexlib::config
namespace indexlib::file_system {
class PackageFileTagConfigList;
typedef std::shared_ptr<PackageFileTagConfigList> PackageFileTagConfigListPtr;
} // namespace indexlib::file_system

namespace indexlib { namespace config {

class MergeConfig : public MergeConfigBase
{
public:
#if _GLIBCXX_USE_CXX11_ABI
    static_assert(sizeof(MergeConfigBase) == 264, "MergeConfigBase size change!");
#else
    static_assert(sizeof(MergeConfigBase) == 144, "MergeConfigBase size change!");
#endif
public:
    MergeConfig();
    MergeConfig(const MergeConfig& other);
    ~MergeConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;

    void operator=(const MergeConfig& other);

public:
    using MergeConfigBase::DEFAULT_MAX_MEMORY_USE_FOR_MERGE;
    using MergeConfigBase::DEFAULT_MERGE_STRATEGY;
    using MergeConfigBase::DEFAULT_MERGE_THREAD_COUNT;
    using MergeConfigBase::DEFAULT_UNIQ_ENCODE_PACK_ATTR_MERGE_BUFFER_SIZE;
    using MergeConfigBase::MAX_MERGE_THREAD_COUNT;

public:
    ModuleClassConfig& GetSplitSegmentConfig();
    std::string GetSplitStrategyName() const;
    uint32_t GetCheckpointInterval() const;
    bool GetEnablePackageFile() const;
    const file_system::PackageFileTagConfigList& GetPackageFileTagConfigList() const;
    void SetEnablePackageFile(bool value);
    void SetCheckpointInterval(uint32_t value);

    void SetEnableInPlanMultiSegmentParallel(bool value);
    bool EnableInPlanMultiSegmentParallel() const;
    bool IsArchiveFileEnable() const;
    void SetEnableArchiveFile(bool value) const;
    bool NeedCalculateTemperature() const;
    void SetCalculateTemperature(const bool& isCalculate);

private:
    // TODO: merge config impl
    MergeConfigImplPtr mImpl;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MergeConfig> MergeConfigPtr;
}} // namespace indexlib::config
