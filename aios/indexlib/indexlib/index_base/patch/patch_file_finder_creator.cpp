#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/index_base/patch/single_part_patch_finder.h"
#include "indexlib/index_base/patch/multi_part_patch_finder.h"
#include "indexlib/index_base/segment/multi_part_segment_directory.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/partition_data.h"

using namespace std;

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, PatchFileFinderCreator);

PatchFileFinderCreator::PatchFileFinderCreator() 
{
}

PatchFileFinderCreator::~PatchFileFinderCreator() 
{
}

PatchFileFinderPtr PatchFileFinderCreator::Create(
        PartitionData* partitionData)
{
    assert(partitionData);
    const SegmentDirectoryPtr& segDir = partitionData->GetSegmentDirectory();
    assert(segDir);

    MultiPartSegmentDirectoryPtr multiSegDir = 
        DYNAMIC_POINTER_CAST(MultiPartSegmentDirectory, segDir);
    if (multiSegDir)
    {
        MultiPartPatchFinderPtr patchFinder(new MultiPartPatchFinder);
        patchFinder->Init(multiSegDir);
        return patchFinder;
    }

    SinglePartPatchFinderPtr singlePatchFinder(new SinglePartPatchFinder);
    singlePatchFinder->Init(segDir);
    return singlePatchFinder;
}

IE_NAMESPACE_END(index_base);

