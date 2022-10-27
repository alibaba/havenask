#ifndef __INDEXLIB_SUMMARY_MERGER_H
#define __INDEXLIB_SUMMARY_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"
#include "indexlib/index/merger_resource.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"

DECLARE_REFERENCE_CLASS(index, ReclaimMap);
DECLARE_REFERENCE_CLASS(index, SegmentDirectoryBase);
DECLARE_REFERENCE_CLASS(index_base, MergeTaskResourceManager);
DECLARE_REFERENCE_CLASS(index_base, ParallelMergeItem);

IE_NAMESPACE_BEGIN(index);

class SummaryMerger
{
public:
    SummaryMerger() {}
    virtual ~SummaryMerger() {}

public:
    virtual void BeginMerge(const SegmentDirectoryBasePtr& segDir) = 0;

    virtual void Merge(const MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
        = 0;

    virtual void SortByWeightMerge(const MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
        = 0;

    virtual int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
        const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        bool isSortedMerge) const = 0;

    virtual bool EnableMultiOutputSegmentParallel() const { return true; }

    virtual std::vector<index_base::ParallelMergeItem> CreateParallelMergeItems(
        const SegmentDirectoryBasePtr& segDir,
        const index_base::SegmentMergeInfos& inPlanSegMergeInfos, uint32_t instanceCount,
        bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr) const = 0;

    virtual void EndParallelMerge(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        int32_t totalParallelCount,
        const std::vector<index_base::MergeTaskResourceVector>& instResourceVec) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SummaryMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SUMMARY_MERGER_H
