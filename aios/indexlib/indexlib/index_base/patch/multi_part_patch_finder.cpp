#include "indexlib/index_base/patch/multi_part_patch_finder.h"
#include "indexlib/index_base/patch/single_part_patch_finder.h"
#include "indexlib/index_base/segment/multi_part_segment_directory.h"

using namespace std;

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, MultiPartPatchFinder);

MultiPartPatchFinder::MultiPartPatchFinder() 
{
}

MultiPartPatchFinder::~MultiPartPatchFinder() 
{
}

void MultiPartPatchFinder::Init(
        const MultiPartSegmentDirectoryPtr& segmentDirectory)
{
    mSegmentDirectory = segmentDirectory;
}

void MultiPartPatchFinder::FindDeletionMapFiles(
        DeletePatchInfos& patchInfos)
{
    assert(patchInfos.empty());
    const std::vector<SegmentDirectoryPtr>& segmentDirectoryVec = 
        mSegmentDirectory->GetSegmentDirectories();
    for (size_t i = 0; i < segmentDirectoryVec.size(); i++)
    {
        DeletePatchInfos tmpPatchInfos;
        SinglePartPatchFinder patchFinder;
        patchFinder.Init(segmentDirectoryVec[i]);
        patchFinder.FindDeletionMapFiles(tmpPatchInfos);
        AddDeletionMapFiles(i, tmpPatchInfos, patchInfos);
    }
}

void MultiPartPatchFinder::AddAttributePatchFiles(
        size_t partitionIdx,
        const AttrPatchInfos& singlePartPatchInfos,
        AttrPatchInfos& multiPartPatchInfos)
{
    AttrPatchInfos::const_iterator iter = singlePartPatchInfos.begin();
    for (; iter != singlePartPatchInfos.end(); iter++)
    {
        segmentid_t virtualSegmentId = 
            mSegmentDirectory->EncodeToVirtualSegmentId(
                    partitionIdx, iter->first);
        if (virtualSegmentId == INVALID_SEGMENTID)
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                    "attr patch for segmentid %d in partition %lu not exist",
                    iter->first, partitionIdx);
        }

        AttrPatchFileInfoVec patchInfoVector = iter->second;
        ConvertSrcSegmentIdToVirtual(partitionIdx, patchInfoVector);
        multiPartPatchInfos[virtualSegmentId] = patchInfoVector;
    }
}

void MultiPartPatchFinder::ConvertSrcSegmentIdToVirtual(
        size_t partitionIdx,
        AttrPatchFileInfoVec& patchInfoVector)
{
    for (size_t i = 0; i < patchInfoVector.size(); i++)
    {
        ConvertSrcSegmentIdToVirtual(partitionIdx, patchInfoVector[i]);
    }
}

void MultiPartPatchFinder::ConvertSrcSegmentIdToVirtual(
        size_t partitionIdx, 
        PatchFileInfo& patchInfo)
{
    segmentid_t virtualSrcSegmentId = 
        mSegmentDirectory->EncodeToVirtualSegmentId(
                partitionIdx, patchInfo.srcSegment);
    if (virtualSrcSegmentId == INVALID_SEGMENTID)
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                             "patch src segmentid %d in partition %lu not exist",
                             patchInfo.srcSegment, partitionIdx);
    }
    patchInfo.srcSegment = virtualSrcSegmentId;
}

void MultiPartPatchFinder::AddDeletionMapFiles(
        size_t partitionIdx,
        const DeletePatchInfos& singlePartPatchInfos,
        DeletePatchInfos& multiPartPatchInfos)
{
    DeletePatchInfos::const_iterator iter = singlePartPatchInfos.begin();
    for (; iter != singlePartPatchInfos.end(); iter++)
    {
        segmentid_t virtualSegmentId = 
            mSegmentDirectory->EncodeToVirtualSegmentId(
                    partitionIdx, iter->first);
        if (virtualSegmentId == INVALID_SEGMENTID)
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                    "deletionmap for segmentid %d in partition %lu not exist",
                    iter->first, partitionIdx);
        }
        PatchFileInfo patchInfo = iter->second;
        ConvertSrcSegmentIdToVirtual(partitionIdx, patchInfo);
        multiPartPatchInfos[virtualSegmentId] = patchInfo;
    }
}

void MultiPartPatchFinder::FindAttrPatchFiles(
        const string& attrName, schema_opid_t opId, AttrPatchInfos& patchInfos)
{
    assert(patchInfos.empty());
    const std::vector<SegmentDirectoryPtr>& segmentDirectoryVec = 
        mSegmentDirectory->GetSegmentDirectories();
    for (size_t i = 0; i < segmentDirectoryVec.size(); i++)
    {
        AttrPatchInfos tmpPatchInfos;
        SinglePartPatchFinder patchFinder;
        patchFinder.Init(segmentDirectoryVec[i]);
        patchFinder.FindAttrPatchFiles(attrName, opId, tmpPatchInfos);
        AddAttributePatchFiles(i, tmpPatchInfos, patchInfos);
    }
}

void MultiPartPatchFinder::FindAttrPatchFilesForSegment(
        const string& attrName, segmentid_t segmentId,
        schema_opid_t opId, AttrPatchFileInfoVec& patchInfo)
{
    segmentid_t physicalSegId = INVALID_SEGMENTID;
    size_t partitionIdx = 0;
    if (!mSegmentDirectory->DecodeVirtualSegmentId(
                    segmentId, physicalSegId, partitionIdx))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                    "virtual segmentid %d not exist", segmentId);
    }

    const std::vector<SegmentDirectoryPtr>& segmentDirectoryVec = 
        mSegmentDirectory->GetSegmentDirectories();
    assert(partitionIdx < segmentDirectoryVec.size());

    SinglePartPatchFinder patchFinder;
    patchFinder.Init(segmentDirectoryVec[partitionIdx]);
    patchFinder.FindAttrPatchFilesForSegment(
            attrName, physicalSegId, opId, patchInfo);

    ConvertSrcSegmentIdToVirtual(partitionIdx, patchInfo);
}

void MultiPartPatchFinder::FindAttrPatchFilesInSegment(
        const string& attrName, segmentid_t segmentId,
        schema_opid_t opId, AttrPatchInfos& patchInfos)
{
    segmentid_t physicalSegId = INVALID_SEGMENTID;
    size_t partitionIdx = 0;
    if (!mSegmentDirectory->DecodeVirtualSegmentId(
                    segmentId, physicalSegId, partitionIdx))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                    "virtual segmentid %d not exist", segmentId);
    }

    const std::vector<SegmentDirectoryPtr>& segmentDirectoryVec = 
        mSegmentDirectory->GetSegmentDirectories();
    assert(partitionIdx < segmentDirectoryVec.size());
    
    AttrPatchInfos tmpPatchInfos;
    SinglePartPatchFinder patchFinder;
    patchFinder.Init(segmentDirectoryVec[partitionIdx]);
    patchFinder.FindAttrPatchFilesInSegment(
            attrName, physicalSegId, opId, tmpPatchInfos);
    AddAttributePatchFiles(partitionIdx, tmpPatchInfos, patchInfos);
}

IE_NAMESPACE_END(index_base);

