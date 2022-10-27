#include "indexlib/index/normal/attribute/accessor/patch_file_filter.h"
#include "indexlib/index_base/segment/segment_data.h"

using namespace std;
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PatchFileFilter);

PatchFileFilter::PatchFileFilter(
        const PartitionDataPtr& partitionData,
        bool isIncConsistentWithRt,
        segmentid_t startLoadSegment)
    : mPartitionData(partitionData)
    , mStartLoadSegment(startLoadSegment)
    , mIsIncConsistentWithRt(isIncConsistentWithRt)
{
}

PatchFileFilter::~PatchFileFilter() 
{
}

AttrPatchInfos PatchFileFilter::Filter(const AttrPatchInfos& attrPatchInfos)
{
    AttrPatchInfos filtPatchInfos;
    PartitionData::Iterator iter = mPartitionData->Begin();
    for (; iter != mPartitionData->End(); iter++)
    {
        const SegmentData& segmentData = *iter;
        segmentid_t destSegmentId = segmentData.GetSegmentId();

        if (mIsIncConsistentWithRt && destSegmentId < mStartLoadSegment)
        {
            continue;
        }
        AttrPatchInfos::const_iterator iter = 
            attrPatchInfos.find(destSegmentId);
        if (iter == attrPatchInfos.end())
        {
            continue;
        }

        const AttrPatchFileInfoVec& patchVec = iter->second;
        AttrPatchFileInfoVec validPatchInfos = 
            FilterLoadedPatchFileInfos(patchVec, mStartLoadSegment);
        if (validPatchInfos.empty())
        {
            continue;
        }
        filtPatchInfos[destSegmentId] = validPatchInfos;
    }
    return filtPatchInfos;
}

AttrPatchFileInfoVec PatchFileFilter::FilterLoadedPatchFileInfos(
        const AttrPatchFileInfoVec& patchInfosVec, segmentid_t startLoadSegment)
{
    AttrPatchFileInfoVec validPatchInfos;
    for (size_t i = 0; i < patchInfosVec.size(); i++)
    {
        if (patchInfosVec[i].srcSegment >= startLoadSegment)
        {
            validPatchInfos.push_back(patchInfosVec[i]);
        }
    }
    return validPatchInfos;
}

IE_NAMESPACE_END(index);

