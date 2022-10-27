#ifndef __INDEXLIB_PATCH_FILE_FILTER_H
#define __INDEXLIB_PATCH_FILE_FILTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/partition_data.h"

IE_NAMESPACE_BEGIN(index);

class PatchFileFilter
{
public:
    PatchFileFilter(const index_base::PartitionDataPtr& partitionData,
                    bool isIncConsistentWithRt,
                    segmentid_t startLoadSegment);
    ~PatchFileFilter();

public:
    index_base::AttrPatchInfos Filter(
            const index_base::AttrPatchInfos& attrPatchInfos);
    
private:
    index_base::AttrPatchFileInfoVec FilterLoadedPatchFileInfos(
            const index_base::AttrPatchFileInfoVec& patchInfosVec, 
            segmentid_t startLoadSegment);

private:
    index_base::PartitionDataPtr mPartitionData;
    segmentid_t mStartLoadSegment;
    bool mIsIncConsistentWithRt;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PatchFileFilter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PATCH_FILE_FILTER_H
