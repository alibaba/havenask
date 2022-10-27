#include "indexlib/merger/merge_work_item_creator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger_factory.h"
#include "indexlib/index/normal/summary/local_disk_summary_merger.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/util/path_util.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/merger/merge_file_system.h"
#include "indexlib/util/column_util.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/archive_folder.h"
#include <numeric>

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, MergeWorkItemCreator);

MergeWorkItemCreator::MergeWorkItemCreator(
        const IndexPartitionSchemaPtr &schema,
        const MergeConfig &mergeConfig,
        const IndexMergeMeta *mergeMeta,
        const merger::SegmentDirectoryPtr &segmentDirectory,
        const merger::SegmentDirectoryPtr &subSegmentDirectory,
        bool isSortedMerge, bool optimize,
        IndexPartitionMergerMetrics* metrics,
        const util::CounterMapPtr& counterMap,
        const config::IndexPartitionOptions& options,
        const merger::MergeFileSystemPtr& mergeFileSystem,
        const plugin::PluginManagerPtr& pluginManager)
    : mSchema(schema)
    , mMergeConfig(mergeConfig)
    , mMergeMeta(mergeMeta)
    , mSegmentDirectory(segmentDirectory)
    , mSubSegDir(subSegmentDirectory)
    , mIsSortedMerge(isSortedMerge)
    , mIsOptimize(optimize)
    , mDumpPath(mergeFileSystem->GetRootPath())
    , mMetrics(metrics)
    , mCounterMap(counterMap)
    , mOptions(options)
    , mMergeFileSystem(mergeFileSystem)
    , mPluginManager(pluginManager)
{
    assert(mMergeMeta);
    mMergeTaskResourceMgr = mMergeMeta->CreateMergeTaskResourceManager();
}

MergeWorkItemCreator::~MergeWorkItemCreator()
{
    for (size_t i = 0; i < mTruncateAttrReaderCreators.size(); ++i)
    {
        delete mTruncateAttrReaderCreators[i];
        delete mTruncateIndexWriterCreators[i];
        delete mAdaptiveBitmapWriterCreators[i];
        if (mSubAdaptiveBitmapWriterCreators[i] != NULL)
        {
            delete mSubAdaptiveBitmapWriterCreators[i];
        }
    }
    mTruncateAttrReaderCreators.clear();
    mTruncateIndexWriterCreators.clear();
    mAdaptiveBitmapWriterCreators.clear();
    mSubAdaptiveBitmapWriterCreators.clear();
}

MergeWorkItem *MergeWorkItemCreator::CreateMergeWorkItem(const MergeTaskItem& item)
{
    if (mMergeConfig.enableMergeItemCheckPoint &&
        mMergeFileSystem->HasCheckpoint(item.GetCheckPointName()))
    {
        IE_LOG(INFO, "merge task item [%s] has already been done!", item.ToString().c_str());
        if (mMetrics)
        {
            ProgressMetricsPtr itemMetric = mMetrics->RegisterMergeItem(item.mCost);
            itemMetric->SetFinish();
        }
        return NULL;
    }

    MergeWorkItem* mergeWorkItem = NULL;
    uint32_t mergePlanIdx = item.mMergePlanIdx;
    const MergePlan& mergePlan = mMergeMeta->GetMergePlan(mergePlanIdx);
    OutputSegmentMergeInfos outputSegmentMergeInfos;
    PrepareOutputSegMergeInfos(item, mergePlan, outputSegmentMergeInfos);
    ReclaimMapPtr reclaimMap = mMergeMeta->GetReclaimMap(mergePlanIdx);
    ReclaimMapPtr subReclaimMap;
    if (item.mIsSubItem)
    {
        subReclaimMap = mMergeMeta->GetSubReclaimMap(mergePlanIdx);
    }
    const auto& inPlanSegMergeInfos = item.mIsSubItem ? mergePlan.GetSubSegmentMergeInfos()
                                      : mergePlan.GetSegmentMergeInfos();
    const auto& segDir = item.mIsSubItem ? mSubSegDir : mSegmentDirectory;
    mergeWorkItem = DoCreateMergeWorkItem(mergePlan, item, inPlanSegMergeInfos, reclaimMap,
            subReclaimMap, segDir, outputSegmentMergeInfos);

    if (mergeWorkItem)
    {
        mergeWorkItem->SetIdentifier(item.ToString());
        mergeWorkItem->SetMetrics(mMetrics, item.mCost);
        if (mMergeConfig.enableMergeItemCheckPoint)
        {
            mergeWorkItem->SetCheckPointFileName(item.GetCheckPointName());
        }
    }

    if (mergeWorkItem && mCounterMap)
    {
        string counterPath = "offline.mergeTime." + item.mMergeType + "."
                             + "MergePlan[" + autil::StringUtil::toString(item.mMergePlanIdx) + "]"
                             + "[" + item.mName + "]";
        const auto& counter = mCounterMap->GetStateCounter(counterPath);
        if (!counter)
        {
            IE_LOG(ERROR, "init counter[%s] failed", counterPath.c_str());
        }
        else
        {
            mergeWorkItem->SetCounter(counter);
        }
    }
    return mergeWorkItem;
}

void MergeWorkItemCreator::PrepareOutputSegMergeInfos(
        const MergeTaskItem& item,
        const MergePlan& plan,
        index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    size_t targetSegmentCount = plan.GetTargetSegmentCount();
    auto targetVersion = mMergeMeta->GetTargetVersion();
    vector<size_t> targetSegmentIdxs;
    if (item.mTargetSegmentIdx == -1)
    {
        targetSegmentIdxs.resize(targetSegmentCount);
        std::iota(targetSegmentIdxs.begin(), targetSegmentIdxs.end(), 0);
    }
    else
    {
        targetSegmentIdxs.push_back(item.mTargetSegmentIdx);
    }
    for (size_t i = 0; i < targetSegmentIdxs.size(); ++i)
    {
        auto targetSegIndex = targetSegmentIdxs[i];
        auto targetSegId = plan.GetTargetSegmentId(targetSegIndex);
        string targetSegName = targetVersion.GetSegmentDirName(targetSegId);
        string mergeTempDir = PathUtil::JoinPath(mDumpPath, targetSegName);
        if (item.mIsSubItem)
        {
            mergeTempDir = PathUtil::JoinPath(mergeTempDir, SUB_SEGMENT_DIR_NAME);
        }
        OutputSegmentMergeInfo info;
        info.targetSegmentId = targetSegId;
        info.targetSegmentIndex = targetSegIndex;
        info.path = mergeTempDir;
        info.docCount
            = mMergeMeta->GetReclaimMap(item.mMergePlanIdx)->GetTargetSegmentDocCount(targetSegIndex);
        outputSegMergeInfos.push_back(info);
    }
}

MergeWorkItem* MergeWorkItemCreator::DoCreateMergeWorkItem(const MergePlan& plan,
    const MergeTaskItem& item, const SegmentMergeInfos& inPlanSegMergeInfos,
    const ReclaimMapPtr& mainReclaimMap, const ReclaimMapPtr& subReclaimMap,
    const merger::SegmentDirectoryPtr& segmentDir, OutputSegmentMergeInfos outputSegmentMergeInfos)
{
    const IndexPartitionSchemaPtr &schema =
        item.mIsSubItem ? mSchema->GetSubIndexPartitionSchema() : mSchema;

    MergeWorkItem* mergeWorkItem = NULL;
    Version targetVersion = mMergeMeta->GetTargetVersion();
    MergerResource resource;
    resource.reclaimMap = item.mIsSubItem ? subReclaimMap : mainReclaimMap;
    resource.targetVersion = targetVersion;
    resource.targetSegmentCount = plan.GetTargetSegmentCount();
    resource.isEntireDataSet = plan.IsEntireDataSet(targetVersion);
    resource.mainBaseDocIds = mainReclaimMap->GetTargetBaseDocIds();
    if (item.mMergeType == DELETION_MAP_TASK_NAME)
    {
        PrepareDirectory(outputSegmentMergeInfos, DELETION_MAP_DIR_NAME);
        if (mIsOptimize)
        {
            return NULL;
        }
        DeletionMapMergerPtr deletionMapMerger = CreateDeletionMapMerger();
        mergeWorkItem
            = new MergeWorkItemTyped<DeletionMapMergerPtr, MergerResource>(
                deletionMapMerger, segmentDir, resource, inPlanSegMergeInfos,
                outputSegmentMergeInfos, mMergeFileSystem, mIsSortedMerge);
    }
    else if (item.mMergeType == SUMMARY_TASK_NAME)
    {
        PrepareDirectory(outputSegmentMergeInfos, SUMMARY_DIR_NAME);
        SummaryMergerPtr summaryMerger = CreateSummaryMerger(item, schema);
        mergeWorkItem = new MergeWorkItemTyped<SummaryMergerPtr, MergerResource>(
            summaryMerger, segmentDir, resource, inPlanSegMergeInfos,
            outputSegmentMergeInfos, mMergeFileSystem, mIsSortedMerge);
    }
    else if (item.mMergeType == INDEX_TASK_NAME)
    {
        PrepareDirectory(outputSegmentMergeInfos, INDEX_DIR_NAME);
        InitCreators(outputSegmentMergeInfos);
        TruncateIndexWriterCreator* truncateIndexWriterCreator = nullptr;
        if (mMergeConfig.truncateOptionConfig)
        {
            truncateIndexWriterCreator = mTruncateIndexWriterCreators[item.mMergePlanIdx];
            assert(truncateIndexWriterCreator);
        }
        AdaptiveBitmapIndexWriterCreator *adaptiveBitmapIndexWriterCreator = nullptr;
        if (item.mIsSubItem)
        {
            adaptiveBitmapIndexWriterCreator = mSubAdaptiveBitmapWriterCreators[item.mMergePlanIdx];
        }
        else
        {
            adaptiveBitmapIndexWriterCreator = mAdaptiveBitmapWriterCreators[item.mMergePlanIdx];
        }
        IndexMergerPtr indexMerger
            = CreateIndexMerger(item, schema, truncateIndexWriterCreator, adaptiveBitmapIndexWriterCreator);
        mergeWorkItem = new MergeWorkItemTyped<IndexMergerPtr, MergerResource>(
            indexMerger, segmentDir, resource, inPlanSegMergeInfos,
            outputSegmentMergeInfos, mMergeFileSystem, mIsSortedMerge);
    }
    else
    {
        assert(item.mMergeType == ATTRIBUTE_TASK_NAME ||
               item.mMergeType == PACK_ATTRIBUTE_TASK_NAME);
        PrepareDirectory(outputSegmentMergeInfos, ATTRIBUTE_DIR_NAME);
        AttributeMergerPtr attrMerger = CreateAttributeMerger(item, schema);
        mergeWorkItem = new MergeWorkItemTyped<AttributeMergerPtr, MergerResource>(
            attrMerger, segmentDir, resource, inPlanSegMergeInfos,
            outputSegmentMergeInfos, mMergeFileSystem, mIsSortedMerge);
    }
    return mergeWorkItem;
}

void MergeWorkItemCreator::PrepareDirectory(
        OutputSegmentMergeInfos& outputSegmentMergeInfos, const string& subPath) const
{
    for (auto& outputInfo : outputSegmentMergeInfos)
    {
        outputInfo.path = PathUtil::JoinPath(outputInfo.path, subPath);
        mMergeFileSystem->MakeDirectory(outputInfo.path, true);
    }
}

DeletionMapMergerPtr MergeWorkItemCreator::CreateDeletionMapMerger() const
{
    DeletionMapMergerPtr deletionMapMergerPtr(new DeletionMapMerger);
    return deletionMapMergerPtr;
}

SummaryMergerPtr MergeWorkItemCreator::CreateSummaryMerger(
        const MergeTaskItem &item, const IndexPartitionSchemaPtr &schema) const
{
    // for compatible
    string summaryGroupName = item.mName.empty() ? DEFAULT_SUMMARYGROUPNAME : item.mName;
    assert(schema && schema->GetSummarySchema());
    const SummarySchemaPtr &summarySchema = schema->GetSummarySchema();
    return SummaryMergerPtr(new LocalDiskSummaryMerger(
                    summarySchema->GetSummaryGroupConfig(summaryGroupName),
                    item.GetParallelMergeItem(), ExtractMergeTaskResource(item)));
}

AttributeMergerPtr MergeWorkItemCreator::CreateAttributeMerger(
        const MergeTaskItem &item, const IndexPartitionSchemaPtr &schema) const
{
    const string& attrName = item.mName;
    const AttributeSchemaPtr &attrSchema = schema->GetAttributeSchema();
    assert(attrSchema);
    bool needMergePatch = !mIsOptimize;
    if (item.mMergeType == PACK_ATTRIBUTE_TASK_NAME)
    {
        const PackAttributeConfigPtr& packConfig =
            attrSchema->GetPackAttributeConfig(attrName);
        assert(packConfig);
        AttributeMergerPtr packAttrMerger(
            AttributeMergerFactory::GetInstance()->CreatePackAttributeMerger(
                packConfig, needMergePatch,
                1024 * 1024 * mMergeConfig.uniqPackAttrMergeBufferSize,
                item.GetParallelMergeItem(), ExtractMergeTaskResource(item)));
        return packAttrMerger;
    }

    AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(attrName);
    assert(attrConfig);
    AttributeMergerPtr attrMerger(
            AttributeMergerFactory::GetInstance()->CreateAttributeMerger(
                    attrConfig, needMergePatch,
                    item.GetParallelMergeItem(), ExtractMergeTaskResource(item)));
    return attrMerger;
}

IndexMergerPtr MergeWorkItemCreator::CreateIndexMerger(const MergeTaskItem& item,
        const config::IndexPartitionSchemaPtr& schema,
        index::TruncateIndexWriterCreator* truncateCreator,
        index::AdaptiveBitmapIndexWriterCreator* adaptiveCreator) const
{
    const IndexSchemaPtr &indexSchema = schema->GetIndexSchema();
    const IndexConfigPtr &indexConfig = indexSchema->GetIndexConfig(item.mName);
    IndexType indexType = indexConfig->GetIndexType();
    IndexMergerPtr indexMerger(
            IndexMergerFactory::GetInstance()->CreateIndexMerger(
                    indexType, mPluginManager));
    indexMerger->Init(indexConfig, item.GetParallelMergeItem(),
                      ExtractMergeTaskResource(item), mMergeConfig.mergeIOConfig,
                      truncateCreator, adaptiveCreator);
    if (indexConfig->IsVirtual())
    {
        return indexMerger;
    }
    return indexMerger;
}

void MergeWorkItemCreator::InitCreators(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
{
    if (mTruncateAttrReaderCreators.size() != 0 || mAdaptiveBitmapWriterCreators.size() != 0)
    {
        return;
    }
    size_t mergePlanCount = mMergeMeta->GetMergePlanCount();
    mTruncateAttrReaderCreators.resize(mergePlanCount, NULL);
    mTruncateIndexWriterCreators.resize(mergePlanCount, NULL);
    mAdaptiveBitmapWriterCreators.resize(mergePlanCount, NULL);
    mSubAdaptiveBitmapWriterCreators.resize(mergePlanCount, NULL);

    int64_t beginTimeInSecond =
        mSegmentDirectory->GetVersion().GetTimestamp() / (1000 * 1000);

    // not support rt truncate
    string globalResourcePath =
        GetGlobalResourceRoot(mDumpPath, false);
    string truncateMetaDir = FileSystemWrapper::JoinPath(globalResourcePath,
            TRUNCATE_META_DIR_NAME);
    string adaptiveBitmapDir = FileSystemWrapper::JoinPath(globalResourcePath,
            ADAPTIVE_DICT_DIR_NAME);

    for (size_t i = 0; i < mergePlanCount; ++i)
    {
        const SegmentMergeInfos &mergeInfos = mMergeMeta->GetMergeSegmentInfo(i);
        const ReclaimMapPtr &reclaimMap = mMergeMeta->GetReclaimMap(i);
        const BucketMaps &bucketMap = mMergeMeta->GetBucketMaps(i);
        if (mMergeConfig.truncateOptionConfig)
        {
            mTruncateAttrReaderCreators[i] = new TruncateAttributeReaderCreator(
                    mSegmentDirectory, mSchema->GetAttributeSchema(),
                    mergeInfos, reclaimMap);
            mTruncateIndexWriterCreators[i] = new TruncateIndexWriterCreator(mIsOptimize);
            storage::ArchiveFolderPtr truncateMetaFolder =
                mMergeFileSystem->GetArchiveFolder(truncateMetaDir, false);
            mTruncateIndexWriterCreators[i]->Init(mSchema, mMergeConfig,
                    outputSegmentMergeInfos, reclaimMap, truncateMetaFolder,
                    mTruncateAttrReaderCreators[i],
                    &bucketMap, beginTimeInSecond);
        }
        if (mIsOptimize)
        {
            storage::ArchiveFolderPtr adaptiveBitmapFolder =
                mMergeFileSystem->GetArchiveFolder(adaptiveBitmapDir, false);
            mAdaptiveBitmapWriterCreators[i] =
                new AdaptiveBitmapIndexWriterCreator(adaptiveBitmapFolder, reclaimMap,
                        outputSegmentMergeInfos);
            if (mSubSegDir)
            {
                const ReclaimMapPtr &subReclaimMap = mMergeMeta->GetSubReclaimMap(i);
                mSubAdaptiveBitmapWriterCreators[i] =
                    new AdaptiveBitmapIndexWriterCreator(adaptiveBitmapFolder, subReclaimMap,
                            outputSegmentMergeInfos);
            }
        }
    }
}

// path for truncate_meta and adaptive_bitmap_meta
// dumpRootPath: /path/to/partition/instance_i/
// globalResource path: /path/to/partition/
string MergeWorkItemCreator::GetGlobalResourceRoot(
        const string &dumpRootPath, bool isRt)
{
    string root = dumpRootPath;
    if (isRt)
    {
        root = PathUtil::GetParentDirPath(dumpRootPath);
    }
    return PathUtil::GetParentDirPath(root);
}

vector<MergeTaskResourcePtr> MergeWorkItemCreator::ExtractMergeTaskResource(
        const MergeTaskItem &item) const
{
    assert(mMergeTaskResourceMgr);
    vector<MergeTaskResourcePtr> resVec;

    vector<resourceid_t> resourceIds = item.GetParallelMergeItem().GetResourceIds();
    for (auto id : resourceIds)
    {
        MergeTaskResourcePtr resource = mMergeTaskResourceMgr->GetResource(id);
        if (!resource)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState, "get merge task resource [id:%d] fail!", id);
        }
        resVec.push_back(resource);
    }
    return resVec;
}

IE_NAMESPACE_END(merger);
