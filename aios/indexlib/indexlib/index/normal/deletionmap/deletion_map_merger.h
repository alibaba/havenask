#ifndef __INDEXLIB_DELETION_MAP_MERGER_H
#define __INDEXLIB_DELETION_MAP_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index/merger_resource.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"

DECLARE_REFERENCE_CLASS(index, SegmentDirectoryBase);
DECLARE_REFERENCE_CLASS(index, ReclaimMap);
DECLARE_REFERENCE_CLASS(index_base, PatchFileInfo);

IE_NAMESPACE_BEGIN(index);

class DeletionMapMerger
{
public:
    DeletionMapMerger() 
    {}
    virtual ~DeletionMapMerger() {}

public:
    void BeginMerge(const SegmentDirectoryBasePtr& segDir);
    
    void Merge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    void SortByWeightMerge(const MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
        const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, bool isSortedMerge) const;

private:
    void InnerMerge(const file_system::DirectoryPtr& directory,
                    const index_base::SegmentMergeInfos& segMergeInfos, 
                    const ReclaimMapPtr& reclaimMap);
    bool InMergePlan(segmentid_t segId, 
                     const index_base::SegmentMergeInfos& segMergeInfos);
    static void CopyOnePatchFile(const index_base::PatchFileInfo& patchFileInfo,
                                 const file_system::DirectoryPtr& directory);

private:
    SegmentDirectoryBasePtr mSegmentDirectory;

private:
    friend class DeletionMapMergeTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DeletionMapMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DELETION_MAP_MERGER_H
