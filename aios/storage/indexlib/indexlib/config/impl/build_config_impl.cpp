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
#include "indexlib/config/impl/build_config_impl.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, BuildConfigImpl);

BuildConfigImpl::BuildConfigImpl()
    : enableAsyncDumpSegment(false)
    , enableBloomFilterForPKReader(false)
    , enableBuildParallelLookupPK(false)
    , bloomFilterMultipleNum(DEFAULT_BLOOM_FILTER_MULTIPLE_NUM)
    , buildingMemoryLimit(DEFAULT_BUILDING_MEMORY_LIMIT)
    , enableConsistentModeBatchBuild(false)
    , enableInconsistentModeBatchBuild(false)
    , batchBuildConsistentModeMaxThreadCount(DEFAULT_BATCH_BUILD_CONSISTENT_MODE_MAX_THREAD_COUNT)
    , batchBuildInconsistentModeMaxThreadCount(DEFAULT_BATCH_BUILD_INCONSISTENT_MODE_MAX_THREAD_COUNT)
    , batchBuildMaxBatchSize(DEFAULT_BATCH_BUILD_MAX_BATCH_SIZE)
    , batchBuildMaxCollectInterval(DEFAULT_BATCH_BUILD_MAX_COLLECT_INTERVAL_US)
    , batchBuildMaxCollectMemoryMB(DEFAULT_BATCH_BUILD_MAX_COLLECT_DOC_MEMORY_MB)
{
    bool isTestMode = EnvUtil::getEnv("IS_INDEXLIB_TEST_MODE", false);
    if (isTestMode) {
        buildingMemoryLimit = DEFAULT_BUILDING_MEMORY_LIMIT_IN_TEST_MODE;
    }
}

BuildConfigImpl::BuildConfigImpl(const BuildConfigImpl& other)
    : segAttrUpdaterConfig(other.segAttrUpdaterConfig)
    , customizedParams(other.customizedParams)
    , enableAsyncDumpSegment(other.enableAsyncDumpSegment)
    , enableBloomFilterForPKReader(other.enableBloomFilterForPKReader)
    , enableBuildParallelLookupPK(other.enableBuildParallelLookupPK)
    , bloomFilterMultipleNum(other.bloomFilterMultipleNum)
    , buildingMemoryLimit(other.buildingMemoryLimit)
    , enableConsistentModeBatchBuild(other.enableConsistentModeBatchBuild)
    , enableInconsistentModeBatchBuild(other.enableInconsistentModeBatchBuild)
    , batchBuildConsistentModeMaxThreadCount(other.batchBuildConsistentModeMaxThreadCount)
    , batchBuildInconsistentModeMaxThreadCount(other.batchBuildInconsistentModeMaxThreadCount)
    , batchBuildMaxBatchSize(other.batchBuildMaxBatchSize)
    , batchBuildMaxCollectInterval(other.batchBuildMaxCollectInterval)
    , batchBuildMaxCollectMemoryMB(other.batchBuildMaxCollectMemoryMB)
{
}

BuildConfigImpl::~BuildConfigImpl() {}

void BuildConfigImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("segment_customize_metrics_updater", segAttrUpdaterConfig, segAttrUpdaterConfig);
    json.Jsonize("bloom_filter_multiple_num", bloomFilterMultipleNum, bloomFilterMultipleNum);

    if (json.GetMode() == TO_JSON) {
        json.Jsonize(CUSTOMIZED_PARAMETERS, customizedParams);
    } else {
        map<string, Any> data = json.GetMap();
        auto iter = data.find(CUSTOMIZED_PARAMETERS);
        if (iter != data.end()) {
            map<string, Any> paramMap = AnyCast<JsonMap>(iter->second);
            map<string, Any>::iterator paramIter = paramMap.begin();
            for (; paramIter != paramMap.end(); paramIter++) {
                string value;
                if (paramIter->second.GetType() == typeid(string)) {
                    value = AnyCast<string>(paramIter->second);
                } else {
                    json::ToString(paramIter->second, value, true, "");
                }
                customizedParams.insert(make_pair(paramIter->first, value));
            }
        }
        if (bloomFilterMultipleNum <= 1 || bloomFilterMultipleNum > 16) {
            AUTIL_LOG(INFO, "invalid bloom_filter_multiple_num [%u], use default [%u].", bloomFilterMultipleNum,
                      DEFAULT_BLOOM_FILTER_MULTIPLE_NUM);
            bloomFilterMultipleNum = DEFAULT_BLOOM_FILTER_MULTIPLE_NUM;
        }
    }
    json.Jsonize("enable_async_dump_segment", enableAsyncDumpSegment, enableAsyncDumpSegment);
    json.Jsonize("enable_bloom_filter_for_primary_key_reader", enableBloomFilterForPKReader,
                 enableBloomFilterForPKReader);
    json.Jsonize("enable_build_parallel_lookup_pk", enableBuildParallelLookupPK, enableBuildParallelLookupPK);
    json.Jsonize("enable_consistent_mode_batch_build", enableConsistentModeBatchBuild, enableConsistentModeBatchBuild);
    json.Jsonize("enable_inconsistent_mode_batch_build", enableInconsistentModeBatchBuild,
                 enableInconsistentModeBatchBuild);
    json.Jsonize("batch_build_consistent_mode_max_thread_count", batchBuildConsistentModeMaxThreadCount,
                 batchBuildConsistentModeMaxThreadCount);
    json.Jsonize("batch_build_inconsistent_mode_max_thread_count", batchBuildInconsistentModeMaxThreadCount,
                 batchBuildInconsistentModeMaxThreadCount);
    json.Jsonize("batch_build_max_batch_size", batchBuildMaxBatchSize, batchBuildMaxBatchSize);
    json.Jsonize("batch_build_max_collect_interval", batchBuildMaxCollectInterval, batchBuildMaxCollectInterval);
    json.Jsonize("batch_build_max_collect_memory_mb", batchBuildMaxCollectMemoryMB, batchBuildMaxCollectMemoryMB);
    json.Jsonize("building_memory_limit_mb", buildingMemoryLimit, buildingMemoryLimit);
}

const map<string, string>& BuildConfigImpl::GetCustomizedParams() const { return customizedParams; }

bool BuildConfigImpl::SetCustomizedParams(const std::string& key, const std::string& value)
{
    return customizedParams.insert(make_pair(key, value)).second;
}

void BuildConfigImpl::Check() const {}

void BuildConfigImpl::SetAsyncDump(bool enable) { enableAsyncDumpSegment = enable; }

bool BuildConfigImpl::EnableAsyncDump() const { return enableAsyncDumpSegment; }

bool BuildConfigImpl::operator==(const BuildConfigImpl& other) const
{
    return segAttrUpdaterConfig == other.segAttrUpdaterConfig && customizedParams == other.customizedParams &&
           enableAsyncDumpSegment == other.enableAsyncDumpSegment &&
           enableBloomFilterForPKReader == other.enableBloomFilterForPKReader &&
           bloomFilterMultipleNum == other.bloomFilterMultipleNum &&
           enableBuildParallelLookupPK == other.enableBuildParallelLookupPK &&
           enableConsistentModeBatchBuild == other.enableConsistentModeBatchBuild &&
           enableInconsistentModeBatchBuild == other.enableInconsistentModeBatchBuild &&
           batchBuildConsistentModeMaxThreadCount == other.batchBuildConsistentModeMaxThreadCount &&
           batchBuildInconsistentModeMaxThreadCount == other.batchBuildInconsistentModeMaxThreadCount &&
           batchBuildMaxBatchSize == other.batchBuildMaxBatchSize &&
           batchBuildMaxCollectInterval == other.batchBuildMaxCollectInterval &&
           batchBuildMaxCollectMemoryMB == other.batchBuildMaxCollectMemoryMB &&
           buildingMemoryLimit == other.buildingMemoryLimit;
}

void BuildConfigImpl::operator=(const BuildConfigImpl& other)
{
    segAttrUpdaterConfig = other.segAttrUpdaterConfig;
    customizedParams = other.customizedParams;
    enableAsyncDumpSegment = other.enableAsyncDumpSegment;
    enableBloomFilterForPKReader = other.enableBloomFilterForPKReader;
    bloomFilterMultipleNum = other.bloomFilterMultipleNum;
    enableBuildParallelLookupPK = other.enableBuildParallelLookupPK;
    enableConsistentModeBatchBuild = other.enableConsistentModeBatchBuild;
    enableInconsistentModeBatchBuild = other.enableInconsistentModeBatchBuild;
    batchBuildConsistentModeMaxThreadCount = other.batchBuildConsistentModeMaxThreadCount;
    batchBuildInconsistentModeMaxThreadCount = other.batchBuildInconsistentModeMaxThreadCount;
    batchBuildMaxBatchSize = other.batchBuildMaxBatchSize;
    batchBuildMaxCollectInterval = other.batchBuildMaxCollectInterval;
    batchBuildMaxCollectMemoryMB = other.batchBuildMaxCollectMemoryMB;
    buildingMemoryLimit = other.buildingMemoryLimit;
}

bool BuildConfigImpl::GetBloomFilterParamForPkReader(uint32_t& multipleNum) const
{
    if (!enableBloomFilterForPKReader) {
        return false;
    }
    multipleNum = bloomFilterMultipleNum;
    return true;
}

void BuildConfigImpl::EnableBloomFilterForPkReader(uint32_t multipleNum)
{
    enableBloomFilterForPKReader = true;
    bloomFilterMultipleNum = multipleNum;
    if (bloomFilterMultipleNum <= 1 || bloomFilterMultipleNum > 16) {
        AUTIL_LOG(INFO, "invalid bloom_filter_multiple_num [%u], use default [%u].", bloomFilterMultipleNum,
                  DEFAULT_BLOOM_FILTER_MULTIPLE_NUM);
        bloomFilterMultipleNum = DEFAULT_BLOOM_FILTER_MULTIPLE_NUM;
    }
}

void BuildConfigImpl::EnablePkBuildParallelLookup() { enableBuildParallelLookupPK = true; }
bool BuildConfigImpl::IsPkBuildParallelLookup() const { return enableBuildParallelLookupPK; }
}} // namespace indexlib::config
