#include "indexlib/index/normal/inverted_index/customized_index/customized_index_merger.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_reduce_item.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_util.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/index/segment_directory_base.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/file_system/directory.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, CustomizedIndexMerger);

CustomizedIndexMerger::CustomizedIndexMerger(
        const PluginManagerPtr& pluginManager)
    : mPluginManager(pluginManager)
{
}

CustomizedIndexMerger::~CustomizedIndexMerger() 
{
}

void CustomizedIndexMerger::Init(const IndexConfigPtr& indexConfig)
{
    mIndexConfig = indexConfig;
    mIndexReducer.reset(IndexPluginUtil::CreateIndexReducer(
                    indexConfig, mPluginManager));
    if (!mIndexReducer)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "create IndexReducer failed");
    }
    
    IndexPluginResourcePtr pluginResource =
        DYNAMIC_POINTER_CAST(IndexPluginResource,
                             mPluginManager->GetPluginResource());
    if (!pluginResource)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "empty index plugin resource!");
    }
    IndexerResourcePtr resource(new IndexerResource(pluginResource->schema,
                                                    pluginResource->options,
                                                    pluginResource->counterMap,
                                                    pluginResource->partitionMeta,
                                                    mIndexConfig->GetIndexName(),
                                                    pluginResource->pluginPath,
                                                    pluginResource->metricProvider,
                                                    IndexerUserDataPtr()));
    if (!mIndexReducer->Init(resource, mMergeHint, mTaskResources))
    {
        INDEXLIB_FATAL_ERROR(Runtime, "init indexer failed"); 
    }
}

void CustomizedIndexMerger::InnerMerge(const index::MergerResource& mergerResource,
    const SegmentMergeInfos& segMergeInfos, OutputSegmentMergeInfos outputSegMergeInfos,
    bool isSortMerge)
{
    assert(mMergeHint.GetId() != MergeItemHint::INVALID_PARALLEL_MERGE_ITEM_ID);
    const auto& partitionData = mSegmentDirectory->GetPartitionData();
    vector<IndexReduceItemPtr> reduceItems;
    SegmentMergeInfos nonEmptySegMergeInfos;
    reduceItems.reserve(segMergeInfos.size());
    nonEmptySegMergeInfos.reserve(segMergeInfos.size());
    const string& indexName = mIndexConfig->GetIndexName();

    for (const auto& segMergeInfo : segMergeInfos)
    {
        segmentid_t segmentId = segMergeInfo.segmentId;
        const auto& segData = partitionData->GetSegmentData(segmentId);
        const auto& indexDir = segData.GetIndexDirectory(indexName, false);
        if (!indexDir)
        {
            IE_LOG(WARN, "cannot find indexDir[%s] in segment[%d]",
                   indexName.c_str(), segmentId);
            continue;
        }
        IndexReduceItemPtr reduceItem = mIndexReducer->CreateReduceItem();
        if (!reduceItem->LoadIndex(indexDir))
        {
            INDEXLIB_FATAL_ERROR(Runtime,
                    "load customize index[%s] faild for segment[%d]",
                    indexName.c_str(),segmentId);
        }
        DocIdMap docIdMap(segData.GetBaseDocId(), mergerResource.reclaimMap);

        if (!reduceItem->UpdateDocId(docIdMap))
        {
            INDEXLIB_FATAL_ERROR(Runtime,
                    "update DocId of index[%s] failed for segment[%d]",
                    indexName.c_str(), segmentId); 
        }
        reduceItems.push_back(reduceItem);
        nonEmptySegMergeInfos.push_back(segMergeInfo);
    }

    PluginResourcePtr pluginResource = mPluginManager->GetPluginResource();
    ReduceResource resource(
        nonEmptySegMergeInfos, mergerResource.reclaimMap, mergerResource.isEntireDataSet);

    for (auto& info : outputSegMergeInfos)
    {
        info.directory = MakeMergeDir(info.directory, mIndexConfig, mIndexFormatOption, mMergeHint);
    }

    bool result = mIndexReducer->Reduce(reduceItems, outputSegMergeInfos, isSortMerge, resource);
    if (!result)
    {
        INDEXLIB_FATAL_ERROR(
                Runtime, "merge index[%s] failed", indexName.c_str()); 
    }
}

void CustomizedIndexMerger::Merge(const index::MergerResource& resource,
    const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    InnerMerge(resource, segMergeInfos, outputSegMergeInfos, false);
}

void CustomizedIndexMerger::SortByWeightMerge(const index::MergerResource& resource,
    const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    InnerMerge(resource, segMergeInfos, outputSegMergeInfos, true);
}

void CustomizedIndexMerger::EndMerge()
{

}

int64_t CustomizedIndexMerger::EstimateMemoryUse(
        const SegmentDirectoryBasePtr& segDir,
        const MergerResource& resource, 
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        bool isSortedMerge)
{
    vector<file_system::DirectoryPtr> indexDirs;
    SegmentMergeInfos filteredSegMergeInfos;
    GetFilteredSegments(segDir, segMergeInfos, indexDirs, filteredSegMergeInfos);
    return mIndexReducer->EstimateMemoryUse(
        indexDirs, filteredSegMergeInfos, outputSegMergeInfos, isSortedMerge);
}

OnDiskIndexIteratorCreatorPtr CustomizedIndexMerger::CreateOnDiskIndexIteratorCreator()
{
    assert(false);
    return OnDiskIndexIteratorCreatorPtr();
}

bool CustomizedIndexMerger::EnableMultiOutputSegmentParallel() const
{
    assert(mIndexReducer);
    return mIndexReducer->EnableMultiOutputSegmentParallel();
}

vector<ParallelMergeItem> CustomizedIndexMerger::CreateParallelMergeItems(
    const SegmentDirectoryBasePtr& segDir,
    const index_base::SegmentMergeInfos& inPlanSegMergeInfos, uint32_t instanceCount,
    bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr) const
{
    assert(mIndexReducer);
    vector<file_system::DirectoryPtr> indexDirs;
    SegmentMergeInfos filteredSegMergeInfos;
    GetFilteredSegments(segDir, inPlanSegMergeInfos, indexDirs, filteredSegMergeInfos);

    auto reduceTasks = mIndexReducer->CreateReduceTasks(
        indexDirs, filteredSegMergeInfos, instanceCount, isEntireDataSet, resMgr);

    vector<ParallelMergeItem> parallelMergeItems;
    for (size_t i = 0; i < reduceTasks.size(); i++)
    {
        ParallelMergeItem item(reduceTasks[i].id, reduceTasks[i].dataRatio);
        for (size_t j = 0; j < reduceTasks[i].resourceIds.size(); j++)
        {
            int32_t resId = reduceTasks[i].resourceIds[j];
            item.AddResource(resId);
        }
        parallelMergeItems.push_back(item);
    }
    return parallelMergeItems;
}

void CustomizedIndexMerger::GetFilteredSegments(const SegmentDirectoryBasePtr& segDir,
    const SegmentMergeInfos& segMergeInfos, vector<DirectoryPtr>& indexDirs,
    SegmentMergeInfos& filteredSegMergeInfos) const
{
    indexDirs.reserve(segMergeInfos.size());
    filteredSegMergeInfos.reserve(segMergeInfos.size());
    const auto& partitionData = segDir->GetPartitionData();
    const string& indexName = mIndexConfig->GetIndexName();    
    for (const auto& segMergeInfo : segMergeInfos)
    {
        segmentid_t segmentId = segMergeInfo.segmentId;
        const auto& segData = partitionData->GetSegmentData(segmentId);
        const auto& indexDir = segData.GetIndexDirectory(indexName, false);
        if (!indexDir)
        {
            IE_LOG(WARN, "cannot find indexDir[%s] in segment[%d]",
                   indexName.c_str(), segmentId);
            continue;
        }
        indexDirs.push_back(indexDir);
        filteredSegMergeInfos.push_back(segMergeInfo);
    }
}

void CustomizedIndexMerger::EndParallelMerge(
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, int32_t totalParallelCount,
    const std::vector<index_base::MergeTaskResourceVector>& instResourceVec)
{
    if (mMergeHint.GetId() >= 0)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "EndParallelMerge should use default mergeItemHint!");
    }
    assert(mIndexReducer);
    auto instanceOutputSegMergeInfos = outputSegMergeInfos;
    for (auto& instInfo : instanceOutputSegMergeInfos)
    {
        instInfo.directory = GetMergeDir(instInfo.directory, false);
    }
    mIndexReducer->EndParallelReduce(
        instanceOutputSegMergeInfos, totalParallelCount, instResourceVec);
}

IE_NAMESPACE_END(index);

