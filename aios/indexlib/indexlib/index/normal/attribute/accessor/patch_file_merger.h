#ifndef __INDEXLIB_PATCH_FILE_MERGER_H
#define __INDEXLIB_PATCH_FILE_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(file_system, Directory);
IE_NAMESPACE_BEGIN(index);

class PatchFileMerger
{
public:
    PatchFileMerger() {}
    virtual ~PatchFileMerger() {}

public:
    virtual void Merge(const index_base::PartitionDataPtr& partitionData,
                       const index_base::SegmentMergeInfos& segMergeInfos,
                       const file_system::DirectoryPtr& targetAttrDir) = 0;
};

DEFINE_SHARED_PTR(PatchFileMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PATCH_FILE_MERGER_H
