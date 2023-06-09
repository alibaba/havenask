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
#include "indexlib/merger/merge_task_item_creator.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger_factory.h"
#include "indexlib/index/normal/framework/index_merger.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger_factory.h"
#include "indexlib/index/normal/source/source_group_merger.h"
#include "indexlib/index/normal/source/source_meta_merger.h"
#include "indexlib/index/normal/summary/local_disk_summary_merger.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"
#include "indexlib/merger/merge_meta.h"
#include "indexlib/merger/merge_plan.h"
#include "indexlib/merger/merge_task_item.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/plugin/plugin_manager.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::index_base;
using namespace indexlib::plugin;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergeTaskItemCreator);

MergeTaskItemCreator::MergeTaskItemCreator(IndexMergeMeta* meta, const SegmentDirectoryPtr& segDir,
                                           const SegmentDirectoryPtr& subSegDir, const IndexPartitionSchemaPtr& schema,
                                           const PluginManagerPtr& pluginManager, const MergeConfig& mergeConfig,
                                           MergeTaskResourceManagerPtr& resMgr)
    : mMergeMeta(meta)
    , mSegDir(segDir)
    , mSubSegDir(subSegDir)
    , mSchema(schema)
    , mMergeConfig(mergeConfig)
    , mResMgr(resMgr)
    , mPluginManager(pluginManager)
{
    assert(mMergeMeta);
}

MergeTaskItemCreator::~MergeTaskItemCreator() {}

template <typename MergerType>
vector<ParallelMergeItem>
MergeTaskItemCreator::DoCreateParallelMergeItemByMerger(const MergerType& mergerPtr, size_t planIdx, bool isSub,
                                                        bool isEntireDataSet, size_t instanceCount) const
{
    const auto& plan = mMergeMeta->GetMergePlan(planIdx);
    const auto& inPlanSegMergeInfos = isSub ? plan.GetSubSegmentMergeInfos() : plan.GetSegmentMergeInfos();
    return mergerPtr->CreateParallelMergeItems(isSub ? mSubSegDir : mSegDir, inPlanSegMergeInfos, instanceCount,
                                               isEntireDataSet, mResMgr);
}

void MergeTaskItemCreator::InitSummaryMergeTaskIdentify(const IndexPartitionSchemaPtr& schema, bool isSubSchema,
                                                        uint32_t instanceCount,
                                                        vector<MergeItemIdentify>& mergeItemIdentifys)
{
    if (!schema->GetSummarySchema() || !schema->NeedStoreSummary()) {
        return;
    }
    const SummarySchemaPtr& summarySchema = schema->GetSummarySchema();
    for (summarygroupid_t id = 0; id < summarySchema->GetSummaryGroupConfigCount(); ++id) {
        const SummaryGroupConfigPtr& summaryGroupConfig = summarySchema->GetSummaryGroupConfig(id);
        if (!summaryGroupConfig->NeedStoreSummary()) {
            continue;
        }

        SummaryMergerPtr summaryMerger = CreateSummaryMerger(summarySchema, summaryGroupConfig->GetGroupName());
        bool enableInPlanMultiSegmentParallel = summaryMerger->EnableMultiOutputSegmentParallel();
        if (!mMergeConfig.EnableInPlanMultiSegmentParallel()) {
            enableInPlanMultiSegmentParallel = false;
        }

        auto generateParallelItem = [this, isSubSchema, summaryMerger, instanceCount](size_t planIdx,
                                                                                      bool isEntireDataSet) {
            return DoCreateParallelMergeItemByMerger(summaryMerger, planIdx, isSubSchema, isEntireDataSet,
                                                     instanceCount);
        };
        MergeItemIdentify identify(SUMMARY_TASK_NAME, summaryGroupConfig->GetGroupName(), isSubSchema,
                                   generateParallelItem, enableInPlanMultiSegmentParallel);
        mergeItemIdentifys.push_back(identify);
    }
}

SummaryMergerPtr MergeTaskItemCreator::CreateSummaryMerger(const SummarySchemaPtr& summarySchema,
                                                           const string& summaryGroupName) const
{
    return SummaryMergerPtr(new LocalDiskSummaryMerger(summarySchema->GetSummaryGroupConfig(summaryGroupName)));
}

void MergeTaskItemCreator::InitSourceMergeTaskIdentify(const IndexPartitionSchemaPtr& schema, bool isSubSchema,
                                                       uint32_t instanceCount,
                                                       vector<MergeItemIdentify>& mergeItemIdentifys)
{
    const SourceSchemaPtr& sourceSchema = schema->GetSourceSchema();
    if (!sourceSchema) {
        return;
    }
    assert(!isSubSchema);
    SourceMetaMergerPtr metaMerger(new SourceMetaMerger);
    auto generateParallelItem = [this, isSubSchema, metaMerger, instanceCount](size_t planIdx, bool isEntireDataSet) {
        return DoCreateParallelMergeItemByMerger(metaMerger, planIdx, isSubSchema, isEntireDataSet, instanceCount);
    };
    MergeItemIdentify metaIdentify(SOURCE_TASK_NAME, SourceMetaMerger::MERGE_TASK_NAME, isSubSchema,
                                   generateParallelItem, metaMerger->EnableMultiOutputSegmentParallel());
    mergeItemIdentifys.push_back(metaIdentify);

    for (auto iter = sourceSchema->Begin(); iter != sourceSchema->End(); iter++) {
        SourceGroupMergerPtr groupMerger(new SourceGroupMerger(*iter));
        auto groupParallelItem = [this, isSubSchema, groupMerger, instanceCount](size_t planIdx, bool isEntireDataSet) {
            return DoCreateParallelMergeItemByMerger(groupMerger, planIdx, isSubSchema, isEntireDataSet, instanceCount);
        };

        MergeItemIdentify groupIdentify(SOURCE_TASK_NAME, SourceGroupMerger::GetMergeTaskName((*iter)->GetGroupId()),
                                        isSubSchema, groupParallelItem,
                                        groupMerger->EnableMultiOutputSegmentParallel());
        mergeItemIdentifys.push_back(groupIdentify);
    }
}

void MergeTaskItemCreator::InitAttributeMergeTaskIdentify(const IndexPartitionSchemaPtr& schema, bool isSubSchema,
                                                          uint32_t instanceCount, bool isOptimize,
                                                          vector<MergeItemIdentify>& mergeItemIdentifys)
{
    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    if (!attrSchema) {
        return;
    }

    auto attrConfigs = attrSchema->CreateIterator();
    auto iter = attrConfigs->Begin();
    for (; iter != attrConfigs->End(); iter++) {
        const AttributeConfigPtr& attrConfig = *iter;
        if (attrConfig->GetPackAttributeConfig() != NULL) {
            continue;
        }

        const string& attrName = attrConfig->GetAttrName();
        AttributeMergerPtr attrMerger = CreateAttributeMerger(attrSchema, attrName, false, isOptimize);

        bool enableInPlanMultiSegmentParallel = attrMerger->EnableMultiOutputSegmentParallel();
        if (!mMergeConfig.EnableInPlanMultiSegmentParallel()) {
            enableInPlanMultiSegmentParallel = false;
        }
        auto generateParallelItem = [this, isSubSchema, attrMerger, instanceCount](size_t planIdx,
                                                                                   bool isEntireDataSet) {
            return DoCreateParallelMergeItemByMerger(attrMerger, planIdx, isSubSchema, instanceCount, isEntireDataSet);
        };

        MergeItemIdentify identify(ATTRIBUTE_TASK_NAME, attrName, isSubSchema, generateParallelItem,
                                   enableInPlanMultiSegmentParallel);
        mergeItemIdentifys.push_back(identify);
    }

    auto packAttrConfigs = attrSchema->CreatePackAttrIterator();
    auto packIter = packAttrConfigs->Begin();
    for (; packIter != packAttrConfigs->End(); packIter++) {
        const PackAttributeConfigPtr& packConfig = *packIter;
        const string& attrName = packConfig->GetAttrName();
        assert(packConfig);
        AttributeMergerPtr packAttrMerger = CreateAttributeMerger(attrSchema, attrName, true, isOptimize);
        bool enableInPlanMultiSegmentParallel = packAttrMerger->EnableMultiOutputSegmentParallel();
        if (!mMergeConfig.EnableInPlanMultiSegmentParallel()) {
            enableInPlanMultiSegmentParallel = false;
        }
        auto generateParallelItem = [this, isSubSchema, packAttrMerger, instanceCount](size_t planIdx,
                                                                                       bool isEntireDataSet) {
            return DoCreateParallelMergeItemByMerger(packAttrMerger, planIdx, isSubSchema, isEntireDataSet,
                                                     instanceCount);
        };
        MergeItemIdentify identify(PACK_ATTRIBUTE_TASK_NAME, attrName, isSubSchema, generateParallelItem,
                                   enableInPlanMultiSegmentParallel);
        mergeItemIdentifys.push_back(identify);
    }
}

AttributeMergerPtr MergeTaskItemCreator::CreateAttributeMerger(const AttributeSchemaPtr& attrSchema,
                                                               const string& attrName, bool isPackAttribute,
                                                               bool isOptimize) const
{
    bool needMergePatch = !isOptimize;
    if (isPackAttribute) {
        const PackAttributeConfigPtr& packConfig = attrSchema->GetPackAttributeConfig(attrName);
        assert(packConfig);
        AttributeMergerPtr packAttrMerger(AttributeMergerFactory::GetInstance()->CreatePackAttributeMerger(
            packConfig, needMergePatch, 1024 * 1024 * mMergeConfig.uniqPackAttrMergeBufferSize));
        return packAttrMerger;
    }

    AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(attrName);
    assert(attrConfig);
    AttributeMergerPtr attrMerger(
        AttributeMergerFactory::GetInstance()->CreateAttributeMerger(attrConfig, needMergePatch));
    return attrMerger;
}

void MergeTaskItemCreator::InitIndexMergeTaskIdentify(const IndexPartitionSchemaPtr& schema, bool isSubSchema,
                                                      uint32_t instanceCount,
                                                      vector<MergeItemIdentify>& mergeItemIdentifys)
{
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    assert(indexSchema);
    auto indexConfigs = indexSchema->CreateIterator(true);
    auto iter = indexConfigs->Begin();
    for (; iter != indexConfigs->End(); iter++) {
        const IndexConfigPtr& indexConfig = *iter;
        if (indexConfig->GetNonTruncateIndexName() != "") {
            // Ignore: truncate index config
            continue;
        }
        const string& indexName = indexConfig->GetIndexName();
        IndexMergerPtr indexMerger = CreateIndexMerger(indexConfig);

        bool enableInPlanMultiSegmentParallel = indexMerger->EnableMultiOutputSegmentParallel();
        if (!mMergeConfig.EnableInPlanMultiSegmentParallel()) {
            enableInPlanMultiSegmentParallel = false;
        }

        auto generateParallelItem = [this, isSubSchema, indexMerger, instanceCount](size_t planIdx,
                                                                                    bool isEntireDataSet) {
            return DoCreateParallelMergeItemByMerger(indexMerger, planIdx, isSubSchema, isEntireDataSet, instanceCount);
        };

        MergeItemIdentify identify(INDEX_TASK_NAME, indexName, isSubSchema, std::move(generateParallelItem),
                                   enableInPlanMultiSegmentParallel);
        mergeItemIdentifys.push_back(identify);
    }
}

IndexMergerPtr MergeTaskItemCreator::CreateIndexMerger(const IndexConfigPtr& indexConfig) const
{
    InvertedIndexType indexType = indexConfig->GetInvertedIndexType();
    IndexMergerPtr indexMerger(IndexMergerFactory::GetInstance()->CreateIndexMerger(indexType, mPluginManager));
    index_base::MergeItemHint hint;
    index_base::MergeTaskResourceVector taskResources;
    config::MergeIOConfig ioConfig;

    indexMerger->Init(indexConfig, hint, taskResources, ioConfig, NULL, NULL);
    return indexMerger;
}

void MergeTaskItemCreator::InitMergeTaskIdentify(const IndexPartitionSchemaPtr& schema, bool isSubSchema,
                                                 uint32_t instanceCount, bool isOptimize)
{
    if (schema->GetTableType() == tt_kv || schema->GetTableType() == tt_kkv) {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        assert(indexSchema);
        string indexName = indexSchema->GetPrimaryKeyIndexFieldName();
        mMergeItemIdentifys.push_back(MergeItemIdentify(KEY_VALUE_TASK_NAME, indexName, isSubSchema));
        return;
    }

    mMergeItemIdentifys.push_back(MergeItemIdentify(DELETION_MAP_TASK_NAME, "", isSubSchema));

    InitSummaryMergeTaskIdentify(schema, isSubSchema, instanceCount, mMergeItemIdentifys);

    InitSourceMergeTaskIdentify(schema, isSubSchema, instanceCount, mMergeItemIdentifys);

    InitIndexMergeTaskIdentify(schema, isSubSchema, instanceCount, mMergeItemIdentifys);

    InitAttributeMergeTaskIdentify(schema, isSubSchema, instanceCount, isOptimize, mMergeItemIdentifys);
}

vector<MergeTaskItem> MergeTaskItemCreator::CreateMergeTaskItems(uint32_t instanceCount, bool isOptimize)
{
    InitMergeTaskIdentify(mSchema, false, instanceCount, isOptimize);
    if (mSubSegDir) {
        const IndexPartitionSchemaPtr& subSchema = mSchema->GetSubIndexPartitionSchema();
        assert(subSchema);
        InitMergeTaskIdentify(subSchema, true, instanceCount, isOptimize);
    }

    MergeTaskItems allItems;
    CreateMergeTaskItemsByMergeItemIdentifiers(mMergeItemIdentifys, instanceCount, allItems);
    return allItems;
}

void MergeTaskItemCreator::CreateMergeTaskItemsByMergeItemIdentifiers(
    const vector<MergeItemIdentify>& mergeItemIdentifys, size_t instanceCount,
    vector<MergeTaskItem>& mergeTaskItems) const
{
    const auto& mergePlans = mMergeMeta->GetMergePlans();

    size_t taskGroupId = 0;
    for (size_t planIdx = 0; planIdx < mergePlans.size(); ++planIdx) {
        bool isEntireDataSet = mergePlans[planIdx].IsEntireDataSet(mMergeMeta->GetTargetVersion());
        for (const auto& identify : mergeItemIdentifys) {
            CreateParallelMergeTaskItems(identify, planIdx, isEntireDataSet, instanceCount, taskGroupId,
                                         mergeTaskItems);
        }
    }
}

void MergeTaskItemCreator::CreateParallelMergeTaskItems(const MergeItemIdentify& identify, size_t planIdx,
                                                        bool isEntireDataSet, size_t instanceCount, size_t& taskGroupId,
                                                        vector<MergeTaskItem>& mergeTaskItems) const
{
    const auto& plan = mMergeMeta->GetMergePlan(planIdx);
    auto targetSegCount = plan.GetTargetSegmentCount();
    auto isSub = identify.isSub;

    vector<ParallelMergeItem> items;
    if (!identify.enableInPlanMultiSegmentParallel) {
        if (identify.generateParallelMergeItem) {
            items = identify.generateParallelMergeItem(planIdx, isEntireDataSet);
        }
        if (items.empty()) {
            ParallelMergeItem parallelItem;
            parallelItem.SetId(0);
            parallelItem.SetTaskGroupId(taskGroupId);
            parallelItem.SetTotalParallelCount(1);
            items.push_back(parallelItem);
        }
        for (auto& item : items) {
            item.SetTaskGroupId(taskGroupId);
            item.SetTotalParallelCount(items.size());
            MergeTaskItem mergeTaskItem(planIdx, identify.type, identify.name, isSub, -1);
            mergeTaskItem.SetParallelMergeItem(item);
            mergeTaskItems.push_back(std::move(mergeTaskItem));
        }
        ++taskGroupId;
    } else {
        for (size_t i = 0; i < targetSegCount; ++i) {
            MergeTaskItem mergeTaskItem(planIdx, identify.type, identify.name, identify.isSub, i);
            mergeTaskItem.mParallelMergeItem.SetId(0);
            mergeTaskItem.mParallelMergeItem.SetTotalParallelCount(1);
            mergeTaskItem.mParallelMergeItem.SetTaskGroupId(taskGroupId);
            mergeTaskItems.push_back(mergeTaskItem);
            ++taskGroupId;
        }
    }
}
}} // namespace indexlib::merger
