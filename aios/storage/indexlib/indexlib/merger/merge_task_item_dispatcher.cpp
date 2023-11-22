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
#include "indexlib/merger/merge_task_item_dispatcher.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <queue>
#include <type_traits>

#include "alog/Logger.h"
#include "fslib/common/common_type.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/config/schema_modify_operation.h"
#include "indexlib/config/section_attribute_config.h"
#include "indexlib/config/truncate_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/framework/VersionMeta.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/inverted_index/config/AdaptiveDictionaryConfig.h"
#include "indexlib/index/summary/Constant.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergeTaskItemDispatcher);

struct MergeTaskItemPairComp {
    bool operator()(const pair<MergeTaskItems, double>& lst, const pair<MergeTaskItems, double>& rst)
    {
        if (lst.second == rst.second) {
            return lst.first.size() > rst.first.size();
        }

        return lst.second > rst.second;
    }
};

MergeTaskItemDispatcher::MergeTaskItemDispatcher(const merger::SegmentDirectoryPtr& segDir,
                                                 const merger::SegmentDirectoryPtr& subSegDir,
                                                 const IndexPartitionSchemaPtr& schema,
                                                 const TruncateOptionConfigPtr& truncateOptionConfig)
    : mSegDir(segDir)
    , mSubSegDir(subSegDir)
    , mSchema(schema)
    , mTruncateOptionConfig(truncateOptionConfig)
{
}

MergeTaskItemDispatcher::MergeTaskItemDispatcher(const merger::SegmentDirectoryPtr& segDir,
                                                 const IndexPartitionSchemaPtr& schema,
                                                 const TruncateOptionConfigPtr& truncateOptionConfig)
    : mSegDir(segDir)
    , mSchema(schema)
    , mTruncateOptionConfig(truncateOptionConfig)
{
}

MergeTaskItemDispatcher::~MergeTaskItemDispatcher() {}

vector<MergeTaskItems> MergeTaskItemDispatcher::DispatchMergeTaskItem(const IndexMergeMeta& meta,
                                                                      MergeTaskItems& allItems, uint32_t instanceCount)
{
    const auto& mergePlans = meta.GetMergePlans();
    pair<int32_t, int32_t> segIdx = GetSampleSegmentIdx(mergePlans);
    if (segIdx.first == -1) {
        return vector<MergeTaskItems>();
    }
    uint32_t mainPartDocCount = mergePlans[segIdx.first].GetSegmentMergeInfos()[segIdx.second].segmentInfo.docCount;
    uint32_t subPartDocCount = 0;
    if (mSubSegDir) {
        subPartDocCount = mergePlans[segIdx.first].GetSubSegmentMergeInfos()[segIdx.second].segmentInfo.docCount;
    }
    segmentid_t segmentId = mergePlans[segIdx.first].GetSegmentMergeInfos()[segIdx.second].segmentId;
    InitMergeTaskItemCost(segmentId, mainPartDocCount, subPartDocCount, meta, allItems);

    for (auto it = allItems.begin(); it != allItems.end(); it++) {
        IE_LOG(INFO, "MergeItem[%s], cost[%.6lf]", it->ToString().c_str(), it->mCost);
    }

    return DispatchWorkItems(allItems, instanceCount);
}

pair<int32_t, int32_t> MergeTaskItemDispatcher::GetSampleSegmentIdx(const vector<MergePlan>& mergePlans)
{
    pair<int32_t, int32_t> ret(-1, -1);
    uint32_t maxDocCount = 0;
    for (uint32_t i = 0; i < mergePlans.size(); ++i) {
        const SegmentMergeInfos& segMergeInfos = mergePlans[i].GetSegmentMergeInfos();
        for (size_t j = 0; j < segMergeInfos.size(); ++j) {
            const SegmentInfo& segInfo = segMergeInfos[j].segmentInfo;
            if (segInfo.docCount >= maxDocCount) {
                ret = std::make_pair(i, j);
                maxDocCount = segInfo.docCount;
            }
        }
    }
    return ret;
}

void MergeTaskItemDispatcher::InitMergeTaskItemCost(segmentid_t sampleSegId, uint32_t mainPartDocCount,
                                                    uint32_t subPartDocCount, const IndexMergeMeta& meta,
                                                    MergeTaskItems& allItems)
{
    const auto& mergePlans = meta.GetMergePlans();
    map<MergeItemIdentify, double> costMap;
    const IndexPartitionSchemaPtr& mainSchema = mSchema;
    const IndexPartitionSchemaPtr& subSchema = mSchema->GetSubIndexPartitionSchema();

    for (size_t i = 0; i < allItems.size(); ++i) {
        IndexPartitionSchemaPtr schema = mainSchema;
        MergeTaskItem& taskItem = allItems[i];
        const auto& plan = mergePlans[taskItem.mMergePlanIdx];
        const auto& reclaimMap = taskItem.mIsSubItem ? meta.GetSubReclaimMap(taskItem.mMergePlanIdx)
                                                     : meta.GetReclaimMap(taskItem.mMergePlanIdx);

        uint32_t targetDocCount =
            taskItem.mIsSubItem ? plan.GetSubTargetSegmentInfo(0).docCount : plan.GetTargetSegmentInfo(0).docCount;

        uint32_t sampleDocCount = mainPartDocCount;
        float outputSegmentRatio = 1.0;
        if (taskItem.mTargetSegmentIdx != -1 && reclaimMap && reclaimMap->GetNewDocCount() > 0) {
            outputSegmentRatio =
                reclaimMap->GetTargetSegmentDocCount(taskItem.mTargetSegmentIdx) * 1.0 / reclaimMap->GetNewDocCount();
        }
        float dataRatio = taskItem.GetParallelMergeItem().GetDataRatio();

        dataRatio = outputSegmentRatio * dataRatio;

        const string& mergeType = taskItem.mMergeType;
        index_base::SegmentData segData;
        if (taskItem.mIsSubItem) {
            schema = subSchema;
            sampleDocCount = subPartDocCount;
            segData = mSubSegDir->GetPartitionData()->GetSegmentData(sampleSegId);
        } else {
            segData = mSegDir->GetPartitionData()->GetSegmentData(sampleSegId);
        }

        if (!segData.GetDirectory())

        {
            INDEXLIB_FATAL_ERROR(FileIO, "segment [id:%d] not exist!", sampleSegId);
        }

        if (sampleDocCount == 0) {
            taskItem.mCost = targetDocCount * dataRatio;
            continue;
        }

        if (mergeType == ATTRIBUTE_TASK_NAME) {
            const string& name = taskItem.mName;
            const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
            const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfig(name);

            assert(attrConfig);
            double cost = 1.0;
            if (!attrConfig->IsMultiValue() && attrConfig->GetFieldType() != ft_string) {
                cost = SizeOfFieldType(attrConfig->GetFieldType());
            } else {
                cost = GetDirSize(segData, taskItem.mMergeType, taskItem.mName) * 1.0 / sampleDocCount;
                if (attrConfig->IsUniqEncode()) {
                    cost *= UNIQ_ENCODE_ATTR_COST_BOOST;
                }
            }
            taskItem.mCost = cost * targetDocCount * dataRatio;
            continue;
        }

        if (mergeType == PACK_ATTRIBUTE_TASK_NAME) {
            const string& packAttrName = taskItem.mName;
            double cost = GetDirSize(segData, ATTRIBUTE_DIR_NAME, packAttrName) * 1.0 / sampleDocCount;

            const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
            const PackAttributeConfigPtr& packConfig = attrSchema->GetPackAttributeConfig(packAttrName);
            assert(packConfig);
            CompressTypeOption compressOption = packConfig->GetCompressType();
            if (compressOption.HasUniqEncodeCompress()) {
                cost *= UNIQ_ENCODE_ATTR_COST_BOOST;
            }
            taskItem.mCost = cost * targetDocCount * dataRatio;
            continue;
        }

        if (mergeType == INDEX_TASK_NAME) {
            const string& name = taskItem.mName;
            const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
            const IndexConfigPtr& indexConfig = indexSchema->GetIndexConfig(name);
            double cost = GetDirSize(segData, taskItem.mMergeType, taskItem.mName) * 1.0 / sampleDocCount;
            if (indexConfig->GetInvertedIndexType() == it_primarykey64 ||
                indexConfig->GetInvertedIndexType() == it_primarykey128) {
                taskItem.mCost = cost * targetDocCount * dataRatio;
                continue;
            }
            if (mTruncateOptionConfig && mTruncateOptionConfig->IsTruncateIndex(name)) {
                const TruncateIndexConfig& truncateIndexConfig = mTruncateOptionConfig->GetTruncateIndexConfig(name);
                cost *= (truncateIndexConfig.GetTruncateIndexPropertySize() + 1);
            }
            if (indexConfig->GetInvertedIndexType() == it_pack || indexConfig->GetInvertedIndexType() == it_expack) {
                PackageIndexConfigPtr packageIndexConfig = dynamic_pointer_cast<PackageIndexConfig>(indexConfig);
                assert(packageIndexConfig);
                if (packageIndexConfig->HasSectionAttribute()) {
                    cost += GetDirSize(segData, taskItem.mMergeType,
                                       SectionAttributeConfig::IndexNameToSectionAttributeName(taskItem.mName)) *
                            1.0 / sampleDocCount;
                }
            }
            taskItem.mCost = cost * INDEX_COST_BOOST * targetDocCount * dataRatio;
            continue;
        }

        if (mergeType == DELETION_MAP_TASK_NAME) {
            taskItem.mCost = DELETION_MAP_COST * targetDocCount * dataRatio;
            continue;
        }

        if (mergeType == SUMMARY_TASK_NAME) {
            string name = (taskItem.mName == index::DEFAULT_SUMMARYGROUPNAME) ? "" : taskItem.mName;
            double cost = GetDirSize(segData, taskItem.mMergeType, name) * 1.0 / sampleDocCount;
            taskItem.mCost = cost * targetDocCount * dataRatio;
            continue;
        }

        // otherwise
        double cost = GetDirSize(segData, taskItem.mMergeType, taskItem.mName) * 1.0 / sampleDocCount;
        taskItem.mCost = cost * targetDocCount * dataRatio;
    }
}

uint64_t MergeTaskItemDispatcher::GetDirSize(const DirectoryPtr& segmentDirectory, const string& dirName,
                                             const string& name)
{
    assert(segmentDirectory);
    string subDirName = dirName + "/" + name;
    DirectoryPtr targetDirectory = segmentDirectory->GetDirectory(subDirName, false);
    return GetDirSize(targetDirectory);
}

uint64_t MergeTaskItemDispatcher::GetDirSize(const index_base::SegmentData& segData, const string& dirName,
                                             const string& name)
{
    uint64_t size = GetDirSize(segData.GetDirectory(), dirName, name);
    if (size > 0) {
        return size;
    }

    if (dirName == INDEX_DIR_NAME) {
        return GetDirSize(segData.GetIndexDirectory(name, false));
    }
    if (dirName == ATTRIBUTE_DIR_NAME) {
        return GetDirSize(segData.GetAttributeDirectory(name, false));
    }
    return 0;
}

uint64_t MergeTaskItemDispatcher::GetDirSize(const DirectoryPtr& directory)
{
    if (!directory) {
        return 0;
    }

    fslib::FileList fileList;
    directory->ListDir("", fileList, false);

    uint64_t fileLen = 0;
    for (fslib::FileList::const_iterator it = fileList.begin(); it != fileList.end(); ++it) {
        if (directory->IsDir(*it)) {
            DirectoryPtr subDirectory = directory->GetDirectory(*it, false);
            fileLen += GetDirSize(subDirectory);
        } else {
            fileLen += directory->GetFileLength(*it);
        }
    }
    return fileLen;
}

vector<MergeTaskItems> MergeTaskItemDispatcher::DispatchWorkItems(MergeTaskItems& allItems, uint32_t instanceCount)
{
    vector<pair<MergeTaskItems, double>> taskItemsWeight;
    for (uint32_t i = 0; i < instanceCount; ++i) {
        MergeTaskItems tmp;
        taskItemsWeight.push_back(std::make_pair(tmp, 0.0));
    }
    sort(allItems.begin(), allItems.end(), MergeTaskItemComp());
    for (uint32_t i = 0; i < allItems.size(); ++i) {
        make_heap(taskItemsWeight.begin(), taskItemsWeight.end(), MergeTaskItemPairComp());
        taskItemsWeight[0].first.push_back(allItems[i]);
        taskItemsWeight[0].second += allItems[i].mCost;
    }

    uint32_t idleInstanceCount = 0;
    vector<MergeTaskItems> vec;
    for (uint32_t i = 0; i < instanceCount; ++i) {
        vec.push_back(taskItemsWeight[i].first);
        if (taskItemsWeight[i].first.empty()) {
            ++idleInstanceCount;
        }
    }

    IE_LOG(INFO, "merge instanceCount [%u], idle instanceCount [%u]", instanceCount, idleInstanceCount);
    if (idleInstanceCount > 0) {
        IE_LOG(WARN, "Idle merge instance exist! idle count in total instances [%u / %u]", idleInstanceCount,
               instanceCount);
    }
    return vec;
}
}} // namespace indexlib::merger
