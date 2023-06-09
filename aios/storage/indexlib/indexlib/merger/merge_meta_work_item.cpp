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
#include "indexlib/merger/merge_meta_work_item.h"

#include "autil/TimeUtility.h"
#include "indexlib/index/merger_util/reclaim_map/sub_reclaim_map_creator.h"
#include "indexlib/index/merger_util/truncate/bucket_map_creator.h"
#include "indexlib/index/merger_util/truncate/truncate_attribute_reader_creator.h"
#include "indexlib/index/merger_util/truncate/truncate_index_writer_creator.h"
#include "indexlib/index_base/index_meta/progress_synchronizer.h"
#include "indexlib/merger/split_strategy/split_segment_strategy.h"

using namespace std;
using namespace indexlib::merger;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergeMetaWorkItem);

MergeMetaWorkItem::MergeMetaWorkItem(
    const config::IndexPartitionSchemaPtr& schema, const config::MergeConfig& mergeConfig,
    const SegmentDirectoryPtr& segDir, const MergePlan& mergePlan, const SegmentMergeInfos& segMergeInfos,
    const DeletionMapReaderPtr& delMapReader, const ReclaimMapCreatorPtr& reclaimMapCreator,
    const SplitSegmentStrategyPtr& splitStrategy, const SegmentMergeInfos& subSegMergeInfos,
    const DeletionMapReaderPtr& subDelMapReader, uint32_t planId, IndexMergeMeta* mergeMeta)
    : mSchema(schema)
    , mMergeConfig(mergeConfig)
    , mSegmentDirectory(segDir)
    , mMergePlan(mergePlan)
    , mSegMergeInfos(segMergeInfos)
    , mDelMapReader(delMapReader)
    , mReclaimMapCreator(reclaimMapCreator)
    , mSplitStrategy(splitStrategy)
    , mSubSegMergeInfos(subSegMergeInfos)
    , mSubDelMapReader(subDelMapReader)
    , mPlanId(planId)
    , mMergeMeta(mergeMeta)
{
}

MergeMetaWorkItem::~MergeMetaWorkItem() {}

void MergeMetaWorkItem::Process()
{
    int64_t time = 0;
    {
        autil::ScopedTime t(time);
        CreateMergePlanMetaAndResource(mMergePlan, mPlanId, mSegMergeInfos, mDelMapReader, mSubSegMergeInfos,
                                       mSubDelMapReader, mMergeMeta);
    }
    IE_LOG(INFO, "Finish creating merge plan meta and resource . used [%ld s], PlanId [%u]", time / 1000000, mPlanId);
}

void MergeMetaWorkItem::Destroy()
{
    mDelMapReader.reset();
    mSubDelMapReader.reset();
}

void MergeMetaWorkItem::CreateMergePlanMetaAndResource(const MergePlan& plan, uint32_t planId,
                                                       const SegmentMergeInfos& segMergeInfos,
                                                       const DeletionMapReaderPtr& deletionMapReader,
                                                       const SegmentMergeInfos& subSegMergeInfos,
                                                       const DeletionMapReaderPtr& subDeletionMapReader,
                                                       IndexMergeMeta* meta)
{
    const auto& inPlanSegMergeInfos = plan.GetSegmentMergeInfos();

    ReclaimMapPtr reclaimMap;
    if (mSplitStrategy) {
        reclaimMap = mReclaimMapCreator->Create(inPlanSegMergeInfos, deletionMapReader,
                                                [this](segmentid_t oldSegId, docid_t oldLocalId) -> segmentindex_t {
                                                    return mSplitStrategy->Process(oldSegId, oldLocalId);
                                                });
    } else {
        reclaimMap = mReclaimMapCreator->Create(inPlanSegMergeInfos, deletionMapReader, nullptr);
    }

    ReclaimMapPtr subReclaimMap;
    SegmentMergeInfos subInPlanSegMergeInfos;

    auto subPartSegmentDirectory = mSegmentDirectory->GetSubSegmentDirectory();
    if (subPartSegmentDirectory) {
        CreateInPlanMergeInfos(subInPlanSegMergeInfos, subSegMergeInfos, plan);
        subReclaimMap =
            CreateSubPartReclaimMap(reclaimMap, inPlanSegMergeInfos, subDeletionMapReader, subInPlanSegMergeInfos);
    }
    index::legacy::BucketMaps bucketMaps = CreateBucketMaps(inPlanSegMergeInfos, reclaimMap);
    SegmentInfo targetSegInfo =
        CreateTargetSegmentInfo(inPlanSegMergeInfos, segMergeInfos, reclaimMap->GetDeletedDocCount());
    SegmentInfo subTargetSegmentInfo;
    if (subPartSegmentDirectory) {
        subTargetSegmentInfo =
            CreateTargetSegmentInfo(subInPlanSegMergeInfos, subSegMergeInfos, subReclaimMap->GetDeletedDocCount());
    }

    MergePlanResource& planResource = meta->GetMergePlanResouce(planId);
    MergePlan& targetPlan = meta->GetMergePlan(planId);
    targetPlan = plan;
    targetPlan.SetSubSegmentMergeInfos(subInPlanSegMergeInfos);

    SplitTargetSegment(targetPlan, targetSegInfo, subTargetSegmentInfo, mSplitStrategy, reclaimMap, subReclaimMap);
    planResource.reclaimMap = reclaimMap;
    planResource.subReclaimMap = subReclaimMap;
    planResource.bucketMaps = bucketMaps;
}

void MergeMetaWorkItem::CreateInPlanMergeInfos(SegmentMergeInfos& inPlanSegMergeInfos,
                                               const SegmentMergeInfos& segMergeInfos, const MergePlan& plan) const
{
    exdocid_t baseDocId = 0;
    for (size_t i = 0; i < segMergeInfos.size(); ++i) {
        const SegmentMergeInfo& segMergeInfo = segMergeInfos[i];
        if (plan.HasSegment(segMergeInfo.segmentId)) {
            SegmentMergeInfo inPlanSegMergeInfo = segMergeInfo;
            inPlanSegMergeInfo.baseDocId = baseDocId;
            inPlanSegMergeInfos.push_back(segMergeInfo);
            baseDocId += segMergeInfo.segmentInfo.docCount;
        }
    }
}

void MergeMetaWorkItem::SplitTargetSegment(MergePlan& plan, const SegmentInfo& targetSegInfo,
                                           const SegmentInfo& subTargetSegInfo,
                                           const SplitSegmentStrategyPtr& splitStrategy,
                                           const ReclaimMapPtr& reclaimMap, const ReclaimMapPtr& subReclaimMap)
{
    plan.ClearTargetSegments();
    if (!splitStrategy) {
        plan.AddTargetSegment();
        plan.SetTargetSegmentInfo(0, targetSegInfo);
        plan.SetSubTargetSegmentInfo(0, subTargetSegInfo);
        return;
    }
    auto customizeMetrics = splitStrategy->GetSegmentCustomizeMetrics();
    size_t targetSegmentCount = customizeMetrics.size();
    for (size_t i = 0; i < targetSegmentCount; ++i) {
        plan.AddTargetSegment();
    }

    for (size_t i = 0; i < targetSegmentCount; ++i) {
        plan.SetTargetSegmentCustomizeMetrics(i, customizeMetrics[i]);
        SegmentInfo segInfo = targetSegInfo;
        segInfo.docCount = reclaimMap->GetTargetSegmentDocCount(i);
        segInfo.schemaId = mSchema->GetSchemaVersionId();
        plan.SetTargetSegmentInfo(i, segInfo);
        if (mSchema->EnableTemperatureLayer()) {
            index_base::SegmentTemperatureMeta meta;
            if (!splitStrategy->GetTargetSegmentTemperatureMeta(i, meta)) {
                INDEXLIB_FATAL_ERROR(Runtime, "cannot get target segment [%lu] temperature meta", i);
            }
            plan.SetTargetSegmentTemperatureMeta(i, meta);
        }
        if (mSchema->GetFileCompressSchema()) {
            uint64_t fingerPrint = mSchema->GetFileCompressSchema()->GetFingerPrint();
            plan.SetTargetSegmentCompressFingerPrint(i, fingerPrint);
        }
        if (subReclaimMap) {
            auto subSegInfo = subTargetSegInfo;
            subSegInfo.docCount = subReclaimMap->GetTargetSegmentDocCount(i);
            plan.SetSubTargetSegmentInfo(i, subSegInfo);
        }
    }
}

ReclaimMapPtr MergeMetaWorkItem::CreateSubPartReclaimMap(const ReclaimMapPtr& mainReclaimMap,
                                                         const SegmentMergeInfos& mainSegMergeInfos,
                                                         const DeletionMapReaderPtr& delMapReader,
                                                         const SegmentMergeInfos& segMergeInfos)
{
    AttributeConfigPtr mainJoinAttrConfig =
        mSchema->GetAttributeSchema()->GetAttributeConfig(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    assert(mainJoinAttrConfig);
    // subDoc not support truncate index
    SubReclaimMapCreator creator(false, mainJoinAttrConfig);
    return creator.CreateSubReclaimMap(mainReclaimMap.get(), mainSegMergeInfos, segMergeInfos, mSegmentDirectory,
                                       delMapReader);
}

index::legacy::BucketMaps MergeMetaWorkItem::CreateBucketMaps(const SegmentMergeInfos& mergeInfos,
                                                              const ReclaimMapPtr& reclaimMap) const
{
    index::legacy::BucketMaps bucketMaps;
    const config::TruncateOptionConfigPtr& truncOptionConfig = mMergeConfig.truncateOptionConfig;
    if (!truncOptionConfig || truncOptionConfig->GetTruncateProfiles().empty()) {
        return bucketMaps;
    }
    index::legacy::TruncateAttributeReaderCreator attrReaderCreator(mSegmentDirectory, mSchema->GetAttributeSchema(),
                                                                    mergeInfos, reclaimMap);
    return index::legacy::BucketMapCreator::CreateBucketMaps(truncOptionConfig, &attrReaderCreator,
                                                             mMergeConfig.maxMemUseForMerge * 1024 * 1024);
}

SegmentInfo MergeMetaWorkItem::CreateTargetSegmentInfo(const SegmentMergeInfos& inPlanSegMergeInfos,
                                                       const SegmentMergeInfos& segMergeInfos,
                                                       uint32_t deletedDocCount) const
{
    SegmentInfo mergedSegInfo;
    for (size_t i = 0; i < inPlanSegMergeInfos.size(); ++i) {
        const SegmentInfo& segInfo = inPlanSegMergeInfos[i].segmentInfo;
        mergedSegInfo.docCount = mergedSegInfo.docCount + segInfo.docCount;
        mergedSegInfo.maxTTL = max(mergedSegInfo.maxTTL, segInfo.maxTTL);
    }
    mergedSegInfo.docCount = mergedSegInfo.docCount - deletedDocCount;
    mergedSegInfo.schemaId = mSchema->GetSchemaVersionId();

    MultiPartSegmentDirectoryPtr multiPartSegDir = DYNAMIC_POINTER_CAST(MultiPartSegmentDirectory, mSegmentDirectory);
    if (multiPartSegDir) {
        vector<SegmentInfo> segInfos;
        GetLastSegmentInfosForMultiPartitionMerge(multiPartSegDir, segMergeInfos, segInfos);
        // use maxTs & min locator
        ProgressSynchronizer ps;
        ps.Init(segInfos);
        indexlibv2::framework::Locator locator;
        locator.Deserialize(ps.GetLocator().ToString());
        mergedSegInfo.SetLocator(locator);
        mergedSegInfo.timestamp = ps.GetTimestamp();
    } else {
        mergedSegInfo.SetLocator(segMergeInfos.back().segmentInfo.GetLocator());
        mergedSegInfo.timestamp = segMergeInfos.back().segmentInfo.timestamp;
    }
    mergedSegInfo.mergedSegment = true;
    IE_LOG(DEBUG, "Merged doc count: [%lu], deleted doc count: [%d]", mergedSegInfo.docCount, deletedDocCount);
    return mergedSegInfo;
}

void MergeMetaWorkItem::GetLastSegmentInfosForMultiPartitionMerge(const MultiPartSegmentDirectoryPtr& multiPartSegDir,
                                                                  const SegmentMergeInfos& segMergeInfos,
                                                                  vector<SegmentInfo>& segInfos)
{
    vector<segmentid_t> lastSegIds;
    uint32_t partCount = multiPartSegDir->GetPartitionCount();
    for (uint32_t partId = 0; partId < partCount; ++partId) {
        const std::vector<segmentid_t>& partSegIds = multiPartSegDir->GetSegIdsByPartId(partId);
        if (partSegIds.empty()) {
            continue;
        }
        lastSegIds.push_back(partSegIds.back());
    }

    assert(segInfos.empty());
    for (auto info : segMergeInfos) {
        if (find(lastSegIds.begin(), lastSegIds.end(), info.segmentId) == lastSegIds.end()) {
            continue;
        }
        segInfos.push_back(info.segmentInfo);
    }
    assert(segInfos.size() == lastSegIds.size());
}
}} // namespace indexlib::merger
