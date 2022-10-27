#include <autil/TimeUtility.h>
#include "indexlib/merger/merge_meta_work_item.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_index_writer_creator.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map_creator.h"
#include "indexlib/index_base/index_meta/progress_synchronizer.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_attribute_reader_creator.h"
#include "indexlib/index/normal/inverted_index/truncate/bucket_map_creator.h"
#include "indexlib/merger/split_strategy/split_segment_strategy.h"

using namespace std;
IE_NAMESPACE_USE(merger);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, MergeMetaWorkItem);

MergeMetaWorkItem::MergeMetaWorkItem(const config::IndexPartitionSchemaPtr& schema,
                                     const config::MergeConfig& mergeConfig,
                                     const SegmentDirectoryPtr& segDir,
                                     const MergePlan &mergePlan,
                                     const SegmentMergeInfos &segMergeInfos,
                                     const DeletionMapReaderPtr& delMapReader,
                                     const ReclaimMapCreatorPtr& reclaimMapCreator,
                                     const SplitSegmentStrategyPtr& splitStrategy,
                                     const SegmentMergeInfos &subSegMergeInfos,
                                     const DeletionMapReaderPtr &subDelMapReader,
                                     uint32_t planId,
                                     IndexMergeMeta *mergeMeta)
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

MergeMetaWorkItem::~MergeMetaWorkItem()
{
}

void MergeMetaWorkItem::Process()
{
    int64_t time = 0;
    {
        autil::ScopedTime t(time);
        CreateMergePlanMetaAndResource(mMergePlan, mPlanId, mSegMergeInfos, mDelMapReader,
                mSubSegMergeInfos, mSubDelMapReader, mMergeMeta);
    }
    IE_LOG(INFO, "Finish creating merge plan meta and resource . used [%ld s], PlanId [%u]",
           time / 1000000, mPlanId);
}

void MergeMetaWorkItem:: Destroy()
{
    mDelMapReader.reset();
    mSubDelMapReader.reset();
}

void MergeMetaWorkItem::CreateMergePlanMetaAndResource(
        const MergePlan &plan,
        uint32_t planId,
        const SegmentMergeInfos &segMergeInfos,
        const DeletionMapReaderPtr &deletionMapReader,
        const SegmentMergeInfos &subSegMergeInfos,
        const DeletionMapReaderPtr &subDeletionMapReader,
        IndexMergeMeta *meta)
{
    const auto& inPlanSegMergeInfos = plan.GetSegmentMergeInfos();

    ReclaimMapPtr reclaimMap;
    if (mSplitStrategy)
    {
        reclaimMap = mReclaimMapCreator->Create(deletionMapReader, inPlanSegMergeInfos,
            [this](segmentid_t oldSegId, docid_t oldLocalId) -> segmentindex_t {
                return mSplitStrategy->Process(oldSegId, oldLocalId);
            });
    }
    else
    {
        reclaimMap = mReclaimMapCreator->Create(deletionMapReader, inPlanSegMergeInfos, nullptr);
    }

    ReclaimMapPtr subReclaimMap;
    SegmentMergeInfos subInPlanSegMergeInfos;

    auto subPartSegmentDirectory = mSegmentDirectory->GetSubSegmentDirectory();
    if (subPartSegmentDirectory)
    {
        CreateInPlanMergeInfos(subInPlanSegMergeInfos, subSegMergeInfos, plan);
        subReclaimMap = CreateSubPartReclaimMap(reclaimMap, inPlanSegMergeInfos,
                subDeletionMapReader, subInPlanSegMergeInfos);
    }
    BucketMaps bucketMaps = CreateBucketMaps(inPlanSegMergeInfos, reclaimMap);
    SegmentInfo targetSegInfo = CreateTargetSegmentInfo(
            inPlanSegMergeInfos, segMergeInfos,
            reclaimMap->GetDeletedDocCount());
    SegmentInfo subTargetSegmentInfo;
    if (subPartSegmentDirectory)
    {
        subTargetSegmentInfo = CreateTargetSegmentInfo(
                subInPlanSegMergeInfos, subSegMergeInfos,
                subReclaimMap->GetDeletedDocCount());
    }

    MergePlanResource& planResource = meta->GetMergePlanResouce(planId);
    MergePlan& targetPlan = meta->GetMergePlan(planId);
    targetPlan = plan;
    targetPlan.SetSubSegmentMergeInfos(subInPlanSegMergeInfos);

    SplitTargetSegment(targetPlan, targetSegInfo, subTargetSegmentInfo,
                       mSplitStrategy, reclaimMap, subReclaimMap);
    planResource.reclaimMap = reclaimMap;
    planResource.subReclaimMap = subReclaimMap;
    planResource.bucketMaps = bucketMaps;
}

void MergeMetaWorkItem::CreateInPlanMergeInfos(
        SegmentMergeInfos& inPlanSegMergeInfos,
        const SegmentMergeInfos& segMergeInfos,
        const MergePlan& plan) const
{
    exdocid_t baseDocId = 0;
    for (size_t i = 0; i < segMergeInfos.size(); ++i)
    {
        const SegmentMergeInfo& segMergeInfo = segMergeInfos[i];
        if (plan.HasSegment(segMergeInfo.segmentId))
        {
            SegmentMergeInfo inPlanSegMergeInfo = segMergeInfo;
            inPlanSegMergeInfo.baseDocId = baseDocId;
            inPlanSegMergeInfos.push_back(segMergeInfo);

            baseDocId += segMergeInfo.segmentInfo.docCount;
        }
    }
}

void MergeMetaWorkItem::SplitTargetSegment(MergePlan& plan, const SegmentInfo& targetSegInfo,
    const SegmentInfo& subTargetSegInfo, const SplitSegmentStrategyPtr& splitStrategy,
    const ReclaimMapPtr& reclaimMap, const ReclaimMapPtr& subReclaimMap)
{
    plan.ClearTargetSegments();
    if (!splitStrategy)
    {
        plan.AddTargetSegment();
        plan.SetTargetSegmentInfo(0, targetSegInfo);
        plan.SetSubTargetSegmentInfo(0, subTargetSegInfo);
        return;
    }
    auto customizeMetrics = splitStrategy->GetSegmentCustomizeMetrics();
    size_t targetSegmentCount = customizeMetrics.size();
    for (size_t i = 0; i < targetSegmentCount; ++i)
    {
        plan.AddTargetSegment();
    }

    for (size_t i = 0; i < targetSegmentCount; ++i)
    {
        plan.SetTargetSegmentCustomizeMetrics(i, customizeMetrics[i]);
        SegmentInfo segInfo = targetSegInfo;
        segInfo.docCount = reclaimMap->GetTargetSegmentDocCount(i);
        segInfo.schemaId = mSchema->GetSchemaVersionId();
        plan.SetTargetSegmentInfo(i, segInfo);
        if (subReclaimMap)
        {
            auto subSegInfo = subTargetSegInfo;
            subSegInfo.docCount = subReclaimMap->GetTargetSegmentDocCount(i);
            plan.SetSubTargetSegmentInfo(i, subSegInfo);
        }
    }
}

ReclaimMapPtr MergeMetaWorkItem::CreateSubPartReclaimMap(
            const ReclaimMapPtr& mainReclaimMap,
            const SegmentMergeInfos& mainSegMergeInfos,
            const DeletionMapReaderPtr& delMapReader,
            const SegmentMergeInfos& segMergeInfos)
{
    ReclaimMapPtr reclaimMap(new ReclaimMap);
    if (!reclaimMap)
    {
        INDEXLIB_FATAL_ERROR(OutOfMemory, "Allocate memory FAILED.");
    }

    AttributeConfigPtr mainJoinAttrConfig =
        mSchema->GetAttributeSchema()->GetAttributeConfig(
                MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    assert(mainJoinAttrConfig);
    // subDoc not support truncate index
    reclaimMap->Init(mainReclaimMap.get(), mSegmentDirectory,
                     mainJoinAttrConfig, mainSegMergeInfos,
                     segMergeInfos, delMapReader, false);
    return reclaimMap;
}

BucketMaps MergeMetaWorkItem::CreateBucketMaps(const SegmentMergeInfos& mergeInfos,
        const ReclaimMapPtr& reclaimMap) const
{
    BucketMaps bucketMaps;
    const config::TruncateOptionConfigPtr& truncOptionConfig = mMergeConfig.truncateOptionConfig;
    if (!truncOptionConfig || truncOptionConfig->GetTruncateProfiles().empty())
    {
        return bucketMaps;
    }
    TruncateAttributeReaderCreator attrReaderCreator(
            mSegmentDirectory, mSchema->GetAttributeSchema(),
            mergeInfos, reclaimMap);
    return BucketMapCreator::CreateBucketMaps(truncOptionConfig,
            &attrReaderCreator, mMergeConfig.maxMemUseForMerge * 1024 * 1024);
}

SegmentInfo MergeMetaWorkItem::CreateTargetSegmentInfo(
        const SegmentMergeInfos &inPlanSegMergeInfos,
        const SegmentMergeInfos &segMergeInfos,
        uint32_t deletedDocCount) const
{
    SegmentInfo mergedSegInfo;
    for (size_t i = 0; i < inPlanSegMergeInfos.size(); ++i)
    {
        const SegmentInfo& segInfo = inPlanSegMergeInfos[i].segmentInfo;
        mergedSegInfo.docCount += segInfo.docCount;
        mergedSegInfo.maxTTL = max(mergedSegInfo.maxTTL, segInfo.maxTTL);
    }
    mergedSegInfo.docCount -= deletedDocCount;
    mergedSegInfo.schemaId = mSchema->GetSchemaVersionId();

    MultiPartSegmentDirectoryPtr multiPartSegDir =
        DYNAMIC_POINTER_CAST(MultiPartSegmentDirectory, mSegmentDirectory);
    if (multiPartSegDir)
    {
        vector<SegmentInfo> segInfos;
        GetLastSegmentInfosForMultiPartitionMerge(
                multiPartSegDir, segMergeInfos, segInfos);
        // use maxTs & min locator
        ProgressSynchronizer ps;
        ps.Init(segInfos);
        mergedSegInfo.locator = ps.GetLocator();
        mergedSegInfo.timestamp = ps.GetTimestamp();
    }
    else
    {
        mergedSegInfo.locator = segMergeInfos.back().segmentInfo.locator;
        mergedSegInfo.timestamp = segMergeInfos.back().segmentInfo.timestamp;
    }
    mergedSegInfo.mergedSegment = true;
    IE_LOG(DEBUG, "Merged doc count: [%lu], deleted doc count: [%d]",
           mergedSegInfo.docCount, deletedDocCount);
    return mergedSegInfo;
}

void MergeMetaWorkItem::GetLastSegmentInfosForMultiPartitionMerge(
            const MultiPartSegmentDirectoryPtr& multiPartSegDir,
            const SegmentMergeInfos &segMergeInfos,
            vector<SegmentInfo> &segInfos)
{
    vector<segmentid_t> lastSegIds;
    uint32_t partCount = multiPartSegDir->GetPartitionCount();
    for (uint32_t partId = 0; partId < partCount; ++partId)
    {
        const std::vector<segmentid_t>&  partSegIds =
            multiPartSegDir->GetSegIdsByPartId(partId);
        if (partSegIds.empty())
        {
            continue;
        }
        lastSegIds.push_back(partSegIds.back());
    }

    assert(segInfos.empty());
    for (auto info : segMergeInfos)
    {
        if (find(lastSegIds.begin(), lastSegIds.end(), info.segmentId) == lastSegIds.end())
        {
            continue;
        }
        segInfos.push_back(info.segmentInfo);
    }
    assert(segInfos.size() == lastSegIds.size());
}


IE_NAMESPACE_END(merger);

