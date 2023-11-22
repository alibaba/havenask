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
#include "indexlib/config/module_class_config.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

class BuildConfigImpl : public autil::legacy::Jsonizable
{
public:
    BuildConfigImpl();
    BuildConfigImpl(const BuildConfigImpl& other);
    ~BuildConfigImpl();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    bool operator==(const BuildConfigImpl& other) const;
    void operator=(const BuildConfigImpl& other);
    bool SetCustomizedParams(const std::string& key, const std::string& value);
    const std::map<std::string, std::string>& GetCustomizedParams() const;
    void SetAsyncDump(bool enable);
    bool EnableAsyncDump() const;

    bool GetBloomFilterParamForPkReader(uint32_t& multipleNum) const;
    void EnableBloomFilterForPkReader(uint32_t multipleNum);
    void EnablePkBuildParallelLookup();
    bool IsPkBuildParallelLookup() const;

public:
    std::vector<ModuleClassConfig> segAttrUpdaterConfig;
    std::map<std::string, std::string> customizedParams;
    bool enableAsyncDumpSegment;
    bool enableBloomFilterForPKReader;
    bool enableBuildParallelLookupPK;
    uint32_t bloomFilterMultipleNum;
    int64_t buildingMemoryLimit;

    bool enableConsistentModeBatchBuild;
    bool enableInconsistentModeBatchBuild;
    int32_t batchBuildConsistentModeMaxThreadCount;
    int32_t batchBuildInconsistentModeMaxThreadCount;
    int32_t batchBuildMaxBatchSize;
    int32_t batchBuildMaxCollectInterval;
    int64_t batchBuildMaxCollectMemoryMB;

private:
    static const uint32_t DEFAULT_BLOOM_FILTER_MULTIPLE_NUM = 5;
    static const int64_t DEFAULT_BUILDING_MEMORY_LIMIT = 6 * 1024;        // MB
    static const int64_t DEFAULT_BUILDING_MEMORY_LIMIT_IN_TEST_MODE = 64; // MB

    static constexpr int32_t DEFAULT_BATCH_BUILD_CONSISTENT_MODE_MAX_THREAD_COUNT = 4;
    static constexpr int32_t DEFAULT_BATCH_BUILD_INCONSISTENT_MODE_MAX_THREAD_COUNT = 2;
    static constexpr int32_t DEFAULT_BATCH_BUILD_MAX_BATCH_SIZE = 100;
    // batch build collect interval threshold in microseconds.
    static constexpr int64_t DEFAULT_BATCH_BUILD_MAX_COLLECT_INTERVAL_US = 100000;
    static constexpr int64_t DEFAULT_BATCH_BUILD_MAX_COLLECT_DOC_MEMORY_MB = 128; // megabytes

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<BuildConfigImpl> BuildConfigImplPtr;
}} // namespace indexlib::config
