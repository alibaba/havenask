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
#include "indexlib/merger/merge_work_item_creator.h"

#include <numeric>

#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/kkv/kkv_merger_creator.h"
#include "indexlib/index/kv/kv_merger.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger_factory.h"
#include "indexlib/index/normal/source/source_group_merger.h"
#include "indexlib/index/normal/source/source_meta_merger.h"
#include "indexlib/index/normal/summary/local_disk_summary_merger.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/merger/kv_merge_work_item.h"
#include "indexlib/merger/merge_file_system.h"
#include "indexlib/util/ColumnUtil.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/metrics/KmonitorTagvNormalizer.h"
using namespace std;

using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::index::legacy;
using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergeWorkItemCreator);

MergeWorkItemCreator::MergeWorkItemCreator(
    const IndexPartitionSchemaPtr& schema, const MergeConfig& mergeConfig, const IndexMergeMeta* mergeMeta,
    const merger::SegmentDirectoryPtr& segmentDirectory, const merger::SegmentDirectoryPtr& subSegmentDirectory,
    bool isSortedMerge, bool optimize, IndexPartitionMergerMetrics* metrics, const util::CounterMapPtr& counterMap,
    const config::IndexPartitionOptions& options, const merger::MergeFileSystemPtr& mergeFileSystem,
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
    , mSampledKKVMergeMemUse(-1)
    , mMergeFileSystem(mergeFileSystem)
    , mPluginManager(pluginManager)
{
    assert(mMergeMeta);
    mMergeTaskResourceMgr = mMergeMeta->CreateMergeTaskResourceManager();
}

MergeWorkItemCreator::~MergeWorkItemCreator()
{
    for (size_t i = 0; i < mTruncateAttrReaderCreators.size(); ++i) {
        delete mTruncateAttrReaderCreators[i];
        delete mTruncateIndexWriterCreators[i];
        delete mAdaptiveBitmapWriterCreators[i];
        if (mSubAdaptiveBitmapWriterCreators[i] != NULL) {
            delete mSubAdaptiveBitmapWriterCreators[i];
        }
    }
    mTruncateAttrReaderCreators.clear();
    mTruncateIndexWriterCreators.clear();
    mAdaptiveBitmapWriterCreators.clear();
    mSubAdaptiveBitmapWriterCreators.clear();
}

MergeWorkItem* MergeWorkItemCreator::CreateMergeWorkItem(const MergeTaskItem& item)
{
    uint32_t mergePlanIdx = item.mMergePlanIdx;
    const MergePlan& mergePlan = mMergeMeta->GetMergePlan(mergePlanIdx);
    kmonitor::MetricsTags tags;
    tags.AddTag("schema_name", mSchema->GetSchemaName());
    tags.AddTag("item_identifier", util::KmonitorTagvNormalizer::GetInstance()->Normalize(item.ToString()));
    if (mMergeConfig.enableMergeItemCheckPoint && mMergeFileSystem->HasCheckpoint(item.GetCheckPointName())) {
        IE_LOG(INFO, "merge task item [%s] has already been done!", item.ToString().c_str());
        if (mMetrics) {
            ProgressMetricsPtr itemMetric = mMetrics->RegisterMergeItem(item.mCost, tags);
            itemMetric->SetFinish();
        }
        return NULL;
    }

    MergeWorkItem* mergeWorkItem = NULL;
    if (item.mMergeType == KEY_VALUE_TASK_NAME) {
        mergeWorkItem = CreateKeyValueMergeWorkItem(item);
    } else {
        OutputSegmentMergeInfos outputSegmentMergeInfos;
        PrepareOutputSegMergeInfos(item, mergePlan, outputSegmentMergeInfos);
        ReclaimMapPtr reclaimMap = mMergeMeta->GetReclaimMap(mergePlanIdx);
        ReclaimMapPtr subReclaimMap;
        if (item.mIsSubItem) {
            subReclaimMap = mMergeMeta->GetSubReclaimMap(mergePlanIdx);
        }
        const auto& inPlanSegMergeInfos =
            item.mIsSubItem ? mergePlan.GetSubSegmentMergeInfos() : mergePlan.GetSegmentMergeInfos();
        const auto& segDir = item.mIsSubItem ? mSubSegDir : mSegmentDirectory;
        mergeWorkItem = DoCreateMergeWorkItem(mergePlan, item, inPlanSegMergeInfos, reclaimMap, subReclaimMap, segDir,
                                              outputSegmentMergeInfos);
    }

    if (mergeWorkItem) {
        mergeWorkItem->SetIdentifier(item.ToString());
        mergeWorkItem->SetMetrics(mMetrics, item.mCost, tags);
        if (mMergeConfig.enableMergeItemCheckPoint) {
            mergeWorkItem->SetCheckPointFileName(item.GetCheckPointName());
        }
    }

    if (mergeWorkItem && mCounterMap) {
        string counterPath = "offline.mergeTime." + item.mMergeType + "." + "MergePlan[" +
                             autil::StringUtil::toString(item.mMergePlanIdx) + "]" + "[" + item.mName + "]";
        const auto& counter = mCounterMap->GetStateCounter(counterPath);
        if (!counter) {
            IE_LOG(ERROR, "init counter[%s] failed", counterPath.c_str());
        } else {
            mergeWorkItem->SetCounter(counter);
        }
    }
    return mergeWorkItem;
}

MergeWorkItem* MergeWorkItemCreator::CreateKeyValueMergeWorkItem(const MergeTaskItem& item)
{
    assert(item.mMergeType == KEY_VALUE_TASK_NAME);
    assert(!item.mIsSubItem);

    uint32_t mergePlanIdx = item.mMergePlanIdx;
    const MergePlan& mergePlan = mMergeMeta->GetMergePlan(mergePlanIdx);

    const SegmentTopologyInfo& topologyInfo = mergePlan.GetTargetTopologyInfo(0);
    if (topologyInfo.levelTopology != indexlibv2::framework::topo_sequence &&
        topologyInfo.levelTopology != indexlibv2::framework::topo_hash_mod) {
        assert(false);
        return NULL;
    }
    uint32_t columnIdx = topologyInfo.columnIdx;
    uint32_t columnCount = topologyInfo.columnCount;

    // Prepare DumpPath
    string targetSegName = mMergeMeta->GetTargetVersion().GetSegmentDirName(mergePlan.GetTargetSegmentId(0));
    // string mergeTempDir = util::PathUtil::JoinPath(mDumpPath, targetSegName);
    string segmentDumpPath = targetSegName;
    string indexDumpPath = util::PathUtil::JoinPath(targetSegName, INDEX_DIR_NAME);
    mMergeFileSystem->MakeDirectory(indexDumpPath);
    // Prepare SegmentDataVector
    index_base::SegmentDataVector segmentDataVec;
    const index_base::PartitionDataPtr& partitionData = mSegmentDirectory->GetPartitionData();
    const SegmentMergeInfos& inPlanSegMergeInfos = mergePlan.GetSegmentMergeInfos();
    for (size_t i = 0; i < inPlanSegMergeInfos.size(); ++i) {
        const index_base::SegmentData& segmentData = partitionData->GetSegmentData(inPlanSegMergeInfos[i].segmentId);
        if (segmentData.GetSegmentInfo()->docCount == 0) {
            // parallel inc build would produce a empty segment, which no need merge.
            continue;
        }
        uint32_t segmentColumnCount = segmentData.GetSegmentInfo()->shardCount;
        for (size_t inSegmentIdx = 0; inSegmentIdx < segmentColumnCount; ++inSegmentIdx) {
            uint32_t idx = util::ColumnUtil::TransformColumnId(inSegmentIdx, columnCount, segmentColumnCount);
            if (idx == columnIdx) {
                const index_base::SegmentData& shardingSegmentData =
                    segmentData.CreateShardingSegmentData(inSegmentIdx);
                segmentDataVec.push_back(shardingSegmentData);
            }
        }
    }

    TableType tableType = mSchema->GetTableType();
    if (tableType == tt_kv) {
        KVMergerPtr kvMerger(new KVMerger);
        kvMerger->Init(partitionData, mSchema, mMergeMeta->GetTimestamp());
        // TODO: set mergeIO config for kv merge ?
        return new KVMergeWorkItem<KVMergerPtr>(kvMerger, mMergeFileSystem, segmentDumpPath, indexDumpPath,
                                                segmentDataVec, topologyInfo);
    } else {
        assert(tableType == tt_kkv);
        bool storeTs = (!mIsOptimize || mOptions.GetOfflineConfig().fullIndexStoreKKVTs ||
                        mOptions.GetOfflineConfig().buildConfig.useUserTimestamp);

        KKVMergerPtr kkvMerger = KKVMergerCreator::Create(mMergeMeta->GetTimestamp(), mSchema, storeTs);
        kkvMerger->SetMergeIOConfig(mMergeConfig.mergeIOConfig);
        if (mSampledKKVMergeMemUse == -1) {
            // attension its heavy
            mSampledKKVMergeMemUse = kkvMerger->EstimateMemoryUse(segmentDataVec, topologyInfo);
        }
        return new KVMergeWorkItem<KKVMergerPtr>(kkvMerger, mMergeFileSystem, segmentDumpPath, indexDumpPath,
                                                 segmentDataVec, topologyInfo, mSampledKKVMergeMemUse);
    }
    return NULL;
}

void MergeWorkItemCreator::PrepareOutputSegMergeInfos(const MergeTaskItem& item, const MergePlan& plan,
                                                      index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    size_t targetSegmentCount = plan.GetTargetSegmentCount();
    auto targetVersion = mMergeMeta->GetTargetVersion();
    vector<size_t> targetSegmentIdxs;
    if (item.mTargetSegmentIdx == -1) {
        targetSegmentIdxs.resize(targetSegmentCount);
        std::iota(targetSegmentIdxs.begin(), targetSegmentIdxs.end(), 0);
    } else {
        targetSegmentIdxs.push_back(item.mTargetSegmentIdx);
    }
    for (size_t i = 0; i < targetSegmentIdxs.size(); ++i) {
        auto targetSegIndex = targetSegmentIdxs[i];
        auto targetSegId = plan.GetTargetSegmentId(targetSegIndex);
        string targetSegName = targetVersion.GetSegmentDirName(targetSegId);
        string mergeTempDir = targetSegName;
        if (item.mIsSubItem) {
            mergeTempDir = PathUtil::JoinPath(mergeTempDir, SUB_SEGMENT_DIR_NAME);
        }
        OutputSegmentMergeInfo info;
        info.targetSegmentId = targetSegId;
        info.targetSegmentIndex = targetSegIndex;
        info.path = mergeTempDir;
        info.docCount = mMergeMeta->GetReclaimMap(item.mMergePlanIdx)->GetTargetSegmentDocCount(targetSegIndex);
        info.temperatureLayer = plan.GetTargetSegmentTemperatureMeta(targetSegIndex).segTemperature;
        outputSegMergeInfos.push_back(info);
    }
}

MergeWorkItem* MergeWorkItemCreator::DoCreateMergeWorkItem(const MergePlan& plan, const MergeTaskItem& item,
                                                           const SegmentMergeInfos& inPlanSegMergeInfos,
                                                           const ReclaimMapPtr& mainReclaimMap,
                                                           const ReclaimMapPtr& subReclaimMap,
                                                           const merger::SegmentDirectoryPtr& segmentDir,
                                                           OutputSegmentMergeInfos outputSegmentMergeInfos)
{
    const IndexPartitionSchemaPtr& schema = item.mIsSubItem ? mSchema->GetSubIndexPartitionSchema() : mSchema;

    MergeWorkItem* mergeWorkItem = NULL;
    Version targetVersion = mMergeMeta->GetTargetVersion();
    MergerResource resource;
    resource.reclaimMap = item.mIsSubItem ? subReclaimMap : mainReclaimMap;
    resource.targetVersion = targetVersion;
    resource.targetSegmentCount = plan.GetTargetSegmentCount();
    resource.isEntireDataSet = plan.IsEntireDataSet(targetVersion);
    resource.mainBaseDocIds = mainReclaimMap->GetTargetBaseDocIds();
    if (item.mMergeType == DELETION_MAP_TASK_NAME) {
        PrepareDirectory(outputSegmentMergeInfos, DELETION_MAP_DIR_NAME);
        if (mIsOptimize) {
            return NULL;
        }
        DeletionMapMergerPtr deletionMapMerger = CreateDeletionMapMerger();
        mergeWorkItem = new MergeWorkItemTyped<DeletionMapMergerPtr, MergerResource>(
            deletionMapMerger, segmentDir, resource, inPlanSegMergeInfos, outputSegmentMergeInfos, mMergeFileSystem,
            mIsSortedMerge);
    } else if (item.mMergeType == SUMMARY_TASK_NAME) {
        PrepareDirectory(outputSegmentMergeInfos, SUMMARY_DIR_NAME);
        SummaryMergerPtr summaryMerger = CreateSummaryMerger(item, schema);
        mergeWorkItem = new MergeWorkItemTyped<SummaryMergerPtr, MergerResource>(
            summaryMerger, segmentDir, resource, inPlanSegMergeInfos, outputSegmentMergeInfos, mMergeFileSystem,
            mIsSortedMerge);
    } else if (item.mMergeType == SOURCE_TASK_NAME) {
        assert(!item.mIsSubItem);
        PrepareDirectory(outputSegmentMergeInfos, SOURCE_DIR_NAME);
        if (item.mName == SourceMetaMerger::MERGE_TASK_NAME) {
            SourceMetaMergerPtr metaMerger(new SourceMetaMerger);
            mergeWorkItem = new MergeWorkItemTyped<SourceMetaMergerPtr, MergerResource>(
                metaMerger, segmentDir, resource, inPlanSegMergeInfos, outputSegmentMergeInfos, mMergeFileSystem,
                mIsSortedMerge);
        } else {
            SourceGroupMergerPtr groupMerger = CreateSourceGroupMerger(item, schema);
            mergeWorkItem = new MergeWorkItemTyped<SourceGroupMergerPtr, MergerResource>(
                groupMerger, segmentDir, resource, inPlanSegMergeInfos, outputSegmentMergeInfos, mMergeFileSystem,
                mIsSortedMerge);
        }
    } else if (item.mMergeType == INDEX_TASK_NAME) {
        PrepareDirectory(outputSegmentMergeInfos, INDEX_DIR_NAME);
        InitCreators(outputSegmentMergeInfos);
        index::legacy::TruncateIndexWriterCreator* truncateIndexWriterCreator = nullptr;
        if (mMergeConfig.truncateOptionConfig) {
            truncateIndexWriterCreator = mTruncateIndexWriterCreators[item.mMergePlanIdx];
            assert(truncateIndexWriterCreator);
        }
        AdaptiveBitmapIndexWriterCreator* adaptiveBitmapIndexWriterCreator = nullptr;
        if (item.mIsSubItem) {
            adaptiveBitmapIndexWriterCreator = mSubAdaptiveBitmapWriterCreators[item.mMergePlanIdx];
        } else {
            adaptiveBitmapIndexWriterCreator = mAdaptiveBitmapWriterCreators[item.mMergePlanIdx];
        }
        IndexMergerPtr indexMerger =
            CreateIndexMerger(item, schema, truncateIndexWriterCreator, adaptiveBitmapIndexWriterCreator);
        mergeWorkItem = new MergeWorkItemTyped<IndexMergerPtr, MergerResource>(
            indexMerger, segmentDir, resource, inPlanSegMergeInfos, outputSegmentMergeInfos, mMergeFileSystem,
            mIsSortedMerge);
    } else {
        assert(item.mMergeType == ATTRIBUTE_TASK_NAME || item.mMergeType == PACK_ATTRIBUTE_TASK_NAME);
        PrepareDirectory(outputSegmentMergeInfos, ATTRIBUTE_DIR_NAME);
        AttributeMergerPtr attrMerger = CreateAttributeMerger(item, schema);
        mergeWorkItem = new MergeWorkItemTyped<AttributeMergerPtr, MergerResource>(
            attrMerger, segmentDir, resource, inPlanSegMergeInfos, outputSegmentMergeInfos, mMergeFileSystem,
            mIsSortedMerge);
    }
    return mergeWorkItem;
}

void MergeWorkItemCreator::PrepareDirectory(OutputSegmentMergeInfos& outputSegmentMergeInfos,
                                            const string& subPath) const
{
    for (auto& outputInfo : outputSegmentMergeInfos) {
        outputInfo.path = PathUtil::JoinPath(outputInfo.path, subPath);
        mMergeFileSystem->MakeDirectory(outputInfo.path);
    }
}

DeletionMapMergerPtr MergeWorkItemCreator::CreateDeletionMapMerger() const
{
    DeletionMapMergerPtr deletionMapMergerPtr(new DeletionMapMerger);
    return deletionMapMergerPtr;
}

SummaryMergerPtr MergeWorkItemCreator::CreateSummaryMerger(const MergeTaskItem& item,
                                                           const IndexPartitionSchemaPtr& schema) const
{
    // for compatible
    string summaryGroupName = item.mName.empty() ? index::DEFAULT_SUMMARYGROUPNAME : item.mName;
    assert(schema && schema->GetSummarySchema());
    const SummarySchemaPtr& summarySchema = schema->GetSummarySchema();
    return SummaryMergerPtr(new LocalDiskSummaryMerger(summarySchema->GetSummaryGroupConfig(summaryGroupName),
                                                       item.GetParallelMergeItem(), ExtractMergeTaskResource(item)));
}

SourceGroupMergerPtr MergeWorkItemCreator::CreateSourceGroupMerger(const MergeTaskItem& item,
                                                                   const IndexPartitionSchemaPtr& schema) const
{
    groupid_t srcGroupId = SourceGroupMerger::GetGroupIdByMergeTaskName(item.mName);
    const SourceSchemaPtr& sourceSchema = schema->GetSourceSchema();
    assert(sourceSchema);
    const SourceGroupConfigPtr& groupConfig = sourceSchema->GetGroupConfig(srcGroupId);
    return SourceGroupMergerPtr(new SourceGroupMerger(groupConfig));
}

AttributeMergerPtr MergeWorkItemCreator::CreateAttributeMerger(const MergeTaskItem& item,
                                                               const IndexPartitionSchemaPtr& schema) const
{
    const string& attrName = item.mName;
    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    assert(attrSchema);
    bool needMergePatch = !mIsOptimize;
    if (item.mMergeType == PACK_ATTRIBUTE_TASK_NAME) {
        const PackAttributeConfigPtr& packConfig = attrSchema->GetPackAttributeConfig(attrName);
        assert(packConfig);
        AttributeMergerPtr packAttrMerger(AttributeMergerFactory::GetInstance()->CreatePackAttributeMerger(
            packConfig, needMergePatch, 1024 * 1024 * mMergeConfig.uniqPackAttrMergeBufferSize,
            item.GetParallelMergeItem(), ExtractMergeTaskResource(item)));
        return packAttrMerger;
    }

    AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(attrName);
    assert(attrConfig);
    AttributeMergerPtr attrMerger(AttributeMergerFactory::GetInstance()->CreateAttributeMerger(
        attrConfig, needMergePatch, item.GetParallelMergeItem(), ExtractMergeTaskResource(item)));
    return attrMerger;
}

IndexMergerPtr MergeWorkItemCreator::CreateIndexMerger(const MergeTaskItem& item,
                                                       const config::IndexPartitionSchemaPtr& schema,
                                                       index::legacy::TruncateIndexWriterCreator* truncateCreator,
                                                       AdaptiveBitmapIndexWriterCreator* adaptiveCreator) const
{
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    const IndexConfigPtr& indexConfig = indexSchema->GetIndexConfig(item.mName);
    InvertedIndexType indexType = indexConfig->GetInvertedIndexType();

    const index_base::PartitionDataPtr& partitionData = mSegmentDirectory->GetPartitionData();
    format_versionid_t binaryFormatVersionId =
        partitionData->GetIndexFormatVersion().GetInvertedIndexBinaryFormatVersion();
    if (indexConfig->GetIndexFormatVersionId() > binaryFormatVersionId) {
        INDEXLIB_FATAL_ERROR(Schema, "format_version_id [%d] for index [%s] is bigger than expected max version id[%d]",
                             indexConfig->GetIndexFormatVersionId(), indexConfig->GetIndexName().c_str(),
                             binaryFormatVersionId);
    }

    IndexMergerPtr indexMerger(IndexMergerFactory::GetInstance()->CreateIndexMerger(indexType, mPluginManager));
    indexMerger->Init(indexConfig, item.GetParallelMergeItem(), ExtractMergeTaskResource(item),
                      mMergeConfig.mergeIOConfig, truncateCreator, adaptiveCreator);
    if (indexConfig->IsVirtual()) {
        return indexMerger;
    }
    return indexMerger;
}

void MergeWorkItemCreator::InitCreators(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
{
    if (mTruncateAttrReaderCreators.size() != 0 || mAdaptiveBitmapWriterCreators.size() != 0) {
        return;
    }
    size_t mergePlanCount = mMergeMeta->GetMergePlanCount();
    mTruncateAttrReaderCreators.resize(mergePlanCount, NULL);
    mTruncateIndexWriterCreators.resize(mergePlanCount, NULL);
    mAdaptiveBitmapWriterCreators.resize(mergePlanCount, NULL);
    mSubAdaptiveBitmapWriterCreators.resize(mergePlanCount, NULL);

    int64_t beginTimeInSecond = mSegmentDirectory->GetVersion().GetTimestamp() / (1000 * 1000);
    PrepareFolders();
    for (size_t i = 0; i < mergePlanCount; ++i) {
        const SegmentMergeInfos& mergeInfos = mMergeMeta->GetMergeSegmentInfo(i);
        const ReclaimMapPtr& reclaimMap = mMergeMeta->GetReclaimMap(i);
        const BucketMaps& bucketMap = mMergeMeta->GetBucketMaps(i);
        if (mMergeConfig.truncateOptionConfig) {
            mTruncateAttrReaderCreators[i] = new index::legacy::TruncateAttributeReaderCreator(
                mSegmentDirectory, mSchema->GetAttributeSchema(), mergeInfos, reclaimMap);
            mTruncateIndexWriterCreators[i] = new index::legacy::TruncateIndexWriterCreator(mIsOptimize);
            mTruncateIndexWriterCreators[i]->Init(mSchema, mMergeConfig, outputSegmentMergeInfos, reclaimMap,
                                                  mTruncateMetaFolder, mTruncateAttrReaderCreators[i], &bucketMap,
                                                  beginTimeInSecond);
        }
        if (mIsOptimize) {
            mAdaptiveBitmapWriterCreators[i] =
                new AdaptiveBitmapIndexWriterCreator(mAdaptiveBitmapFolder, reclaimMap, outputSegmentMergeInfos);
            if (mSubSegDir) {
                const ReclaimMapPtr& subReclaimMap = mMergeMeta->GetSubReclaimMap(i);
                mSubAdaptiveBitmapWriterCreators[i] =
                    new AdaptiveBitmapIndexWriterCreator(mAdaptiveBitmapFolder, subReclaimMap, outputSegmentMergeInfos);
            }
        }
    }
}

vector<MergeTaskResourcePtr> MergeWorkItemCreator::ExtractMergeTaskResource(const MergeTaskItem& item) const
{
    assert(mMergeTaskResourceMgr);
    vector<MergeTaskResourcePtr> resVec;

    vector<resourceid_t> resourceIds = item.GetParallelMergeItem().GetResourceIds();
    for (auto id : resourceIds) {
        MergeTaskResourcePtr resource = mMergeTaskResourceMgr->GetResource(id);
        if (!resource) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "get merge task resource [id:%d] fail!", id);
        }
        resVec.push_back(resource);
    }
    return resVec;
}

void MergeWorkItemCreator::PrepareFolders()
{
    string globalResourcePath = PathUtil::GetParentDirPath(mDumpPath);
    mTruncateMetaFolder = mMergeFileSystem->CreateLocalArchiveFolder(TRUNCATE_META_DIR_NAME, false);
    mAdaptiveBitmapFolder = mMergeFileSystem->CreateLocalArchiveFolder(ADAPTIVE_DICT_DIR_NAME, false);
    auto truncateMetaFolderLogicalFs = mTruncateMetaFolder->GetRootDirectory()->GetFileSystem();
    auto ret = FslibWrapper::IsExist(PathUtil::JoinPath(globalResourcePath, TRUNCATE_META_DIR_NAME));
    THROW_IF_FS_ERROR(ret.ec, "Check IsExist of Path [%s] failed",
                      PathUtil::JoinPath(globalResourcePath, TRUNCATE_META_DIR_NAME).c_str());
    if (ret.result) {
        THROW_IF_FS_ERROR(
            truncateMetaFolderLogicalFs->MountDir(globalResourcePath, TRUNCATE_META_DIR_NAME, "", FSMT_READ_ONLY, true),
            "mount dir[%s] failed", TRUNCATE_META_DIR_NAME);
    }
    auto adaptiveBitmapFolderLogicalFs = mAdaptiveBitmapFolder->GetRootDirectory()->GetFileSystem();
    ret = FslibWrapper::IsExist(PathUtil::JoinPath(globalResourcePath, ADAPTIVE_DICT_DIR_NAME));
    THROW_IF_FS_ERROR(ret.ec, "Check IsExist of Path [%s] failed",
                      PathUtil::JoinPath(globalResourcePath, ADAPTIVE_DICT_DIR_NAME).c_str());
    if (ret.result) {
        THROW_IF_FS_ERROR(adaptiveBitmapFolderLogicalFs->MountDir(globalResourcePath, ADAPTIVE_DICT_DIR_NAME, "",
                                                                  FSMT_READ_ONLY, true),
                          "mount dir[%s] failed", ADAPTIVE_DICT_DIR_NAME);
    }
    // to give effect to meta files mounted forward
    mTruncateMetaFolder->ReOpen().GetOrThrow();
    mAdaptiveBitmapFolder->ReOpen().GetOrThrow();
}
}} // namespace indexlib::merger
