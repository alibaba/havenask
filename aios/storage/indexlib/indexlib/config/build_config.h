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
#ifndef __INDEXLIB_BUILD_CONFIG_H
#define __INDEXLIB_BUILD_CONFIG_H

#include <memory>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/build_config_base.h"
#include "indexlib/config/module_class_config.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, BuildConfigImpl);
namespace indexlib { namespace config {

class BuildConfig : public BuildConfigBase
{
public:
    using BuildConfigBase::BuildPhase;

public:
    BuildConfig();
    BuildConfig(const BuildConfig& other);
    ~BuildConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    bool operator==(const BuildConfig& other) const;
    void operator=(const BuildConfig& other);
    using BuildConfigBase::GetEnvBuildTotalMemUse;

public:
    using BuildConfigBase::DEFAULT_BUILD_TOTAL_MEMORY;
    using BuildConfigBase::DEFAULT_DUMP_THREADCOUNT;
    using BuildConfigBase::DEFAULT_HASHMAP_INIT_SIZE;
    using BuildConfigBase::DEFAULT_KEEP_VERSION_COUNT;
    using BuildConfigBase::DEFAULT_MAX_DOC_COUNT;
    using BuildConfigBase::DEFAULT_SHARDING_COLUMN_NUM;
    using BuildConfigBase::DEFAULT_USE_PACKAGE_FILE;
    using BuildConfigBase::MAX_DUMP_THREAD_COUNT;

public:
    std::vector<ModuleClassConfig>& GetSegmentMetricsUpdaterConfig();
    const std::map<std::string, std::string>& GetCustomizedParams() const;
    bool SetCustomizedParams(const std::string& key, const std::string& value);
    void SetAsyncDump(bool enable);
    bool EnableAsyncDump() const;

    bool GetBloomFilterParamForPkReader(uint32_t& multipleNum) const;
    void EnableBloomFilterForPkReader(uint32_t multipleNum);
    void EnablePkBuildParallelLookup();
    bool IsPkBuildParallelLookup() const;
    int64_t GetBuildingMemoryLimit() const;
    void SetBuildingMemoryLimit(int64_t memLimit);
    bool IsBatchBuildEnabled(bool isConsistentMode) const;
    void SetBatchBuild(bool isConsistentMode, bool enableBatchBuild);
    int32_t GetBatchBuildMaxThreadCount(bool isConsistentMode) const;
    void SetBatchBuildMaxThreadCount(bool isConsistentMode, int32_t maxThreadCount);
    int32_t GetBatchBuildMaxBatchSize() const;
    void SetBatchBuildMaxBatchSize(int32_t maxBatchSize);
    int64_t GetBatchBuildMaxCollectInterval() const;
    void SetBatchBuildMaxCollectInterval(int64_t maxCollectInterval);
    int64_t GetBatchBuildMaxCollectMemoryInM() const;
    void SetBatchBuildMaxCollectMemoryInM(int64_t memoryInM);

private:
    // attention: add new parameter to impl
    BuildConfigImplPtr mImpl;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildConfig);
}} // namespace indexlib::config

#endif //__INDEXLIB_BUILD_CONFIG_H
