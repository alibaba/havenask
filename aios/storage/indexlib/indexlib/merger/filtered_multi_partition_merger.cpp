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
#include "indexlib/merger/filtered_multi_partition_merger.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/index_base/meta_cache_preloader.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, FilteredMultiPartitionMerger);

FilteredMultiPartitionMerger::FilteredMultiPartitionMerger(const config::IndexPartitionOptions& options,
                                                           util::MetricProviderPtr metricProvider,
                                                           const std::string& indexPluginPath,
                                                           const DocFilterCreatorPtr& docFilterCreator,
                                                           const std::string& epochId)
    : MultiPartitionMerger(options, metricProvider, indexPluginPath, CommonBranchHinterOption::Normal(0, epochId))
    , mDocFilterCreator(docFilterCreator)
    , mTargetVersion(INVALID_VERSION)
{
}

FilteredMultiPartitionMerger::~FilteredMultiPartitionMerger() {}

bool FilteredMultiPartitionMerger::Init(const std::vector<std::string>& mergeSrc, versionid_t version,
                                        const std::string& mergeDest)
{
    if (mergeSrc.size() == 0) {
        IE_LOG(ERROR, "no src path for merge");
        return false;
    }

    if (version == INVALID_VERSION) {
        IE_LOG(ERROR, "no valid version for merge");
        return false;
    }

    mMergeSrcs = mergeSrc;
    mDestPath = mergeDest;
    mTargetVersion = version;
    OfflineConfig& offlineConfig = mOptions.GetOfflineConfig();
    offlineConfig.RebuildTruncateIndex();
    offlineConfig.RebuildAdaptiveBitmap();
    MergeConfig& mergeConfig = mOptions.GetMergeConfig();
    if (mergeConfig.mergeStrategyStr != OPTIMIZE_MERGE_STRATEGY_STR) {
        MergeConfig tmpMergeConfig;
        tmpMergeConfig.mergeStrategyStr = OPTIMIZE_MERGE_STRATEGY_STR;
        tmpMergeConfig.mergeStrategyParameter.SetLegacyString("skip-single-merged-segment=false");
        mergeConfig = tmpMergeConfig;
    } else {
        string strategyStr = mergeConfig.mergeStrategyParameter.GetLegacyString();
        if (!strategyStr.empty()) {
            strategyStr += ";";
        }
        strategyStr += "skip-single-merged-segment=false";
        mergeConfig.mergeStrategyParameter.SetLegacyString(strategyStr);
    }
    RewriteLoadConfigs(mOptions);
    mPartitionMerger =
        CreatePartitionMerger(CreateMergeSrcDirs(mMergeSrcs, mTargetVersion, mMetricProvider), mDestPath);
    return true;
}

int64_t FilteredMultiPartitionMerger::getCacheBlockSize()
{
    int64_t value = -1;
    if (autil::EnvUtil::getEnvWithoutDefault("CACHE_BLOCK_SIZE", value) && value > 0) {
        return value;
    }
    return (int64_t)2 * 1024 * 1024; // default 2M
}

void FilteredMultiPartitionMerger::RewriteLoadConfigs(IndexPartitionOptions& options)
{
    int64_t cacheBlockSize = getCacheBlockSize();
    int64_t memorySize = options.GetOfflineConfig().readerConfig.summaryCacheSize;
    int64_t cacheIOBatchSize = options.GetOfflineConfig().readerConfig.cacheIOBatchSize;
    if (memorySize < cacheBlockSize) {
        memorySize = cacheBlockSize;
    }
    IE_LOG(INFO, "memorySize: [%ld B], cacheBlockSize:[%ld B]", memorySize, cacheBlockSize);
    LoadConfigList& loadConfigList = options.GetOnlineConfig().loadConfigList;
    loadConfigList.Clear();

    if (options.GetOfflineConfig().readerConfig.useBlockCache) {
        LoadStrategyPtr indexCacheLoadStrategy;
        LoadStrategyPtr summaryCacheLoadStrategy;
        indexCacheLoadStrategy.reset(new CacheLoadStrategy(
            BlockCacheOption::LRU(memorySize, cacheBlockSize, cacheIOBatchSize), false, false, true, false));
        summaryCacheLoadStrategy.reset(new CacheLoadStrategy(
            BlockCacheOption::LRU(memorySize, cacheBlockSize, cacheIOBatchSize), false, false, true, false));

        LoadConfig indexLoadConfig;
        indexLoadConfig.SetName("indexLoadConfig");
        LoadConfig summaryLoadConfig;
        summaryLoadConfig.SetName("summaryLoadConfig");
        LoadConfig::FilePatternStringVector summaryPattern;
        summaryPattern.push_back("_SUMMARY_");
        summaryLoadConfig.SetFilePatternString(summaryPattern);
        summaryLoadConfig.SetLoadStrategyPtr(summaryCacheLoadStrategy);

        LoadConfig::FilePatternStringVector indexPattern;
        indexPattern.push_back("_KV_KEY_");
        indexPattern.push_back("_KV_VALUE_");
        indexPattern.push_back("_KKV_PKEY_");
        indexPattern.push_back("_KKV_SKEY_");
        indexPattern.push_back("_KKV_VALUE_");
        indexPattern.push_back("_INDEX_");
        indexLoadConfig.SetFilePatternString(indexPattern);
        indexLoadConfig.SetLoadStrategyPtr(indexCacheLoadStrategy);

        loadConfigList.PushFront(indexLoadConfig);
        loadConfigList.PushFront(summaryLoadConfig);
    }
}

void FilteredMultiPartitionMerger::GetFirstVersion(const file_system::DirectoryPtr& partDirectory, Version& version)
{
    VersionLoader::GetVersion(partDirectory, version, mTargetVersion);
    MetaCachePreloader::Load(partDirectory, version.GetVersionId());
}

void FilteredMultiPartitionMerger::CheckSrcPath(const vector<DirectoryPtr>& mergeDirs,
                                                const IndexPartitionSchemaPtr& schema, const PartitionMeta& partMeta,
                                                vector<Version>& versions)
{
    versions.resize(mergeDirs.size());
    for (size_t i = 0; i < mergeDirs.size(); ++i) {
        VersionLoader::GetVersion(mergeDirs[i], versions[i], mTargetVersion);
        MetaCachePreloader::Load(mergeDirs[i], versions[i].GetVersionId());
    }
}

indexlib::merger::MergeMetaPtr FilteredMultiPartitionMerger::CreateMergeMeta(int32_t parallelNum,
                                                                             int64_t timestampInSecond)
{
    DocFilterPtr docFilter = mDocFilterCreator->CreateDocFilter(mSegmentDirectory, mSchema, mOptions);
    auto partitionData = mSegmentDirectory->GetPartitionData();
    docid_t totalDocCount = 0;
    auto iter = partitionData->Begin();
    for (; iter != partitionData->End(); iter++) {
        totalDocCount += iter->GetSegmentInfo()->docCount;
    }

    auto deletionMapReader = mSegmentDirectory->GetDeletionMapReader();
    for (docid_t i = 0; i < totalDocCount; i++) {
        if (docFilter->Filter(i)) {
            deletionMapReader->Delete(i);
        }
    }
    return mPartitionMerger->CreateMergeMeta(true, parallelNum, timestampInSecond);
}

void FilteredMultiPartitionMerger::PrepareMergeMeta(int32_t parallelNum, const string& mergeMetaVersionDir)
{
    int64_t curTs = TimeUtility::currentTimeInMicroSeconds();
    MergeMetaPtr mergeMeta = CreateMergeMeta(parallelNum, curTs);
    FenceContextPtr fenceContext(file_system::FslibWrapper::CreateFenceContext(mDestPath, mBranchOption.epochId));
    mergeMeta->Store(mergeMetaVersionDir, fenceContext.get());
}

std::string FilteredMultiPartitionMerger::GetMergeMetaDir() { return mPartitionMerger->GetMergeMetaDir(); }

void FilteredMultiPartitionMerger::DoMerge(const MergeMetaPtr& mergeMeta, int32_t instanceId)
{
    mPartitionMerger->DoMerge(true, mergeMeta, instanceId);
}

void FilteredMultiPartitionMerger::EndMerge(const MergeMetaPtr& mergeMeta, versionid_t alignVersion)
{
    mPartitionMerger->EndMerge(mergeMeta, alignVersion);
}
}} // namespace indexlib::merger
