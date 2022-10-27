#ifndef __INDEXLIB_MERGE_META_WORK_ITEM_H
#define __INDEXLIB_MERGE_META_WORK_ITEM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/resource_control_work_item.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/merger/merge_plan.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/merger/multi_part_segment_directory.h"
#include "indexlib/merger/index_merge_meta.h"

DECLARE_REFERENCE_CLASS(index, ReclaimMapCreator);
DECLARE_REFERENCE_CLASS(merger, SplitSegmentStrategy);

IE_NAMESPACE_BEGIN(merger);

class MergeMetaWorkItem : public util::ResourceControlWorkItem
{
public:
    // TODO
    // too many arguments
    MergeMetaWorkItem(const config::IndexPartitionSchemaPtr& schema,
                      const config::MergeConfig& mergeConfig,
                      const merger::SegmentDirectoryPtr& segDir,
                      const MergePlan &mergePlan,
                      const index_base::SegmentMergeInfos &segMergeInfos,
                      const index::DeletionMapReaderPtr& delMapReader,
                      const index::ReclaimMapCreatorPtr& reclaimMapCreator,
                      const SplitSegmentStrategyPtr& splitStrategy,
                      const index_base::SegmentMergeInfos &subSegMergeInfos,
                      const index::DeletionMapReaderPtr &subDelMapReader,
                      uint32_t planId,
                      merger::IndexMergeMeta *mergeMeta);


    ~MergeMetaWorkItem();
public:
    void Process() override;

    void Destroy() override;

    //TODO: Control merge mem use
    int64_t GetRequiredResource() const override { return 0; }

public:
    static void GetLastSegmentInfosForMultiPartitionMerge(
            const MultiPartSegmentDirectoryPtr& multiPartSegDir,
            const index_base::SegmentMergeInfos &segMergeInfos,
            std::vector<index_base::SegmentInfo> &segInfos);

private:
    void CreateMergePlanMetaAndResource(
        const MergePlan &plan,
        uint32_t planId,
        const index_base::SegmentMergeInfos &segMergeInfos,
        const index::DeletionMapReaderPtr &deletionMapReader,
        const index_base::SegmentMergeInfos &subSegMergeInfos,
        const index::DeletionMapReaderPtr &subDeletionMapReader,
        merger::IndexMergeMeta *meta);
    void CreateInPlanMergeInfos(index_base::SegmentMergeInfos& inPlanSegMergeInfos,
                                const index_base::SegmentMergeInfos& segMergeInfos,
                                const MergePlan& plan) const;

    index::ReclaimMapPtr CreateSubPartReclaimMap(
        const index::ReclaimMapPtr& mainReclaimMap,
        const index_base::SegmentMergeInfos& mainSegMergeInfos,
        const index::DeletionMapReaderPtr& delMapReader,
        const index_base::SegmentMergeInfos& segMergeInfos);
    index::BucketMaps CreateBucketMaps(const index_base::SegmentMergeInfos& mergeInfos,
                                const index::ReclaimMapPtr& reclaimMap) const;
    index_base::SegmentInfo CreateTargetSegmentInfo(
        const index_base::SegmentMergeInfos &inPlanSegMergeInfos,
        const index_base::SegmentMergeInfos &segMergeInfos,
        uint32_t deletedDocCount) const;
    void SplitTargetSegment(MergePlan& plan, const index_base::SegmentInfo& targetSegInfo,
        const index_base::SegmentInfo& subTargetSegInfo,
        const SplitSegmentStrategyPtr& splitStrategy, const index::ReclaimMapPtr& reclaimMap,
        const index::ReclaimMapPtr& subReclaimMap);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::MergeConfig mMergeConfig;
    merger::SegmentDirectoryPtr mSegmentDirectory;
    MergePlan mMergePlan;
    index_base::SegmentMergeInfos mSegMergeInfos;
    index::DeletionMapReaderPtr mDelMapReader;
    const index::ReclaimMapCreatorPtr mReclaimMapCreator;
    const SplitSegmentStrategyPtr mSplitStrategy;
    index_base::SegmentMergeInfos mSubSegMergeInfos;
    index::DeletionMapReaderPtr mSubDelMapReader;
    uint32_t mPlanId;
    merger::IndexMergeMeta *mMergeMeta;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeMetaWorkItem);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_META_WORK_ITEM_H
