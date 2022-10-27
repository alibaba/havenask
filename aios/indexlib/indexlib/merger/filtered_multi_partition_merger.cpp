#include "indexlib/merger/filtered_multi_partition_merger.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/load_config/cache_load_strategy.h"
#include "indexlib/index_base/meta_cache_preloader.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, FilteredMultiPartitionMerger);

FilteredMultiPartitionMerger::FilteredMultiPartitionMerger(
        const config::IndexPartitionOptions& options,
        misc::MetricProviderPtr metricProvider,
        const std::string& indexPluginPath,
        const DocFilterCreatorPtr& docFilterCreator)
    : MultiPartitionMerger(options, metricProvider, indexPluginPath)
    , mDocFilterCreator(docFilterCreator)
    , mTargetVersion(INVALID_VERSION)
{}

FilteredMultiPartitionMerger::~FilteredMultiPartitionMerger() 
{
}

bool FilteredMultiPartitionMerger::Init(
        const std::vector<std::string>& mergeSrc,
        versionid_t version,
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
        tmpMergeConfig.mergeStrategyParameter.SetLegacyString(
                "skip-single-merged-segment=false");
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
    mPartitionMerger = CreatePartitionMerger(mMergeSrcs, mDestPath);
    return true;
}

int64_t FilteredMultiPartitionMerger::getCacheBlockSize() {
    char* envParam = getenv("CACHE_BLOCK_SIZE");
    if (envParam) {
        int64_t value = -1;
        if (StringUtil::fromString(envParam, value))
        {
            if (value > 0) {
                return value;
            }
        }
    }
    return (int64_t)2 * 1024 * 1024; //default 2M
}

void FilteredMultiPartitionMerger::RewriteLoadConfigs(IndexPartitionOptions& options)
{
    int64_t cacheBlockSize = getCacheBlockSize();
    int64_t cacheSize = options.GetOfflineConfig().readerConfig.summaryCacheSize;
    if (cacheSize < cacheBlockSize) {
        cacheSize = cacheBlockSize;
    }
    IE_LOG(INFO, "cacheSize: [%ld B], cacheBlockSize:[%ld B]", cacheSize, cacheBlockSize);
    LoadConfigList& loadConfigList = options.GetOnlineConfig().loadConfigList;
    loadConfigList.Clear();

    if (options.GetOfflineConfig().readerConfig.useBlockCache)
    {

        LoadStrategyPtr indexCacheLoadStrategy;
        LoadStrategyPtr summaryCacheLoadStrategy;
        indexCacheLoadStrategy.reset(
                new CacheLoadStrategy(cacheSize, cacheBlockSize,
                        false, false, false));

        summaryCacheLoadStrategy.reset(
                new CacheLoadStrategy(cacheSize, 
                        cacheBlockSize, false, false, false));

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

void FilteredMultiPartitionMerger::GetFirstVersion(
        const std::string& partPath, Version& version) {
    VersionLoader::GetVersion(partPath, version, mTargetVersion);
    MetaCachePreloader::Load(partPath, version.GetVersionId());
}

void FilteredMultiPartitionMerger::CheckSrcPath(
        const vector<string>& mergeSrc,
        const IndexPartitionSchemaPtr& schema,
        const PartitionMeta &partMeta,
        vector<Version>& versions)
{
    versions.resize(mergeSrc.size());
    for (size_t i = 0; i < mergeSrc.size(); ++i)
    {
        string partPath = FileSystemWrapper::NormalizeDir(mergeSrc[i]);
        if (!FileSystemWrapper::IsExist(partPath))
        {
            INDEXLIB_FATAL_ERROR(BadParameter, "Partition not exsit : [%s]", mergeSrc[i].c_str());
        }
        VersionLoader::GetVersion(partPath, versions[i], mTargetVersion);
        MetaCachePreloader::Load(partPath, versions[i].GetVersionId());        
    }
}

IE_NAMESPACE(merger)::MergeMetaPtr FilteredMultiPartitionMerger::CreateMergeMeta(
        int32_t parallelNum, int64_t timestampInSecond) {
    DocFilterPtr docFilter = mDocFilterCreator->CreateDocFilter(mSegmentDirectory, mSchema, mOptions);
    auto partitionData = mSegmentDirectory->GetPartitionData();
    docid_t totalDocCount = 0;
    auto iter = partitionData->Begin();
    for (; iter != partitionData->End(); iter++) {
        totalDocCount += iter->GetSegmentInfo().docCount;
    }

    auto deletionMapReader = mSegmentDirectory->GetDeletionMapReader();
    for (docid_t i = 0; i < totalDocCount; i++) {
        if (docFilter->Filter(i)) {
            deletionMapReader->Delete(i);
        }
    }
    return mPartitionMerger->CreateMergeMeta(true, parallelNum, timestampInSecond);
}

std::string FilteredMultiPartitionMerger::GetMergeMetaDir()
{ return mPartitionMerger->GetMergeMetaDir(); }

void FilteredMultiPartitionMerger::DoMerge(const MergeMetaPtr& mergeMeta, int32_t instanceId)
{ mPartitionMerger->DoMerge(true, mergeMeta, instanceId); }

void FilteredMultiPartitionMerger::EndMerge(
        const MergeMetaPtr& mergeMeta, versionid_t alignVersion)
{ mPartitionMerger->EndMerge(mergeMeta, alignVersion); }


IE_NAMESPACE_END(merger);

