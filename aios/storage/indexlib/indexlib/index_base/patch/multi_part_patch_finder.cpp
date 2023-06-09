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
#include "indexlib/index_base/patch/multi_part_patch_finder.h"

#include "indexlib/index_base/patch/single_part_patch_finder.h"
#include "indexlib/index_base/segment/multi_part_segment_directory.h"

using namespace std;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, MultiPartPatchFinder);

MultiPartPatchFinder::MultiPartPatchFinder() {}

MultiPartPatchFinder::~MultiPartPatchFinder() {}

void MultiPartPatchFinder::Init(const MultiPartSegmentDirectoryPtr& segmentDirectory)
{
    mSegmentDirectory = segmentDirectory;
}

void MultiPartPatchFinder::FindDeletionMapFiles(DeletePatchInfos& patchInfos)
{
    assert(patchInfos.empty());
    const std::vector<SegmentDirectoryPtr>& segmentDirectoryVec = mSegmentDirectory->GetSegmentDirectories();
    for (size_t i = 0; i < segmentDirectoryVec.size(); i++) {
        DeletePatchInfos tmpPatchInfos;
        SinglePartPatchFinder patchFinder;
        patchFinder.Init(segmentDirectoryVec[i]);
        patchFinder.FindDeletionMapFiles(tmpPatchInfos);
        AddDeletionMapFiles(i, tmpPatchInfos, patchInfos);
    }
}

void MultiPartPatchFinder::AddPatchFiles(size_t partitionIdx, const PatchInfos& singlePartPatchInfos,
                                         PatchInfos* multiPartPatchInfos)
{
    PatchInfos::const_iterator iter = singlePartPatchInfos.begin();
    for (; iter != singlePartPatchInfos.end(); iter++) {
        segmentid_t virtualSegmentId = mSegmentDirectory->EncodeToVirtualSegmentId(partitionIdx, iter->first);
        if (virtualSegmentId == INVALID_SEGMENTID) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "patch for segmentid %d in partition %lu not exist", iter->first,
                                 partitionIdx);
        }

        PatchFileInfoVec patchInfoVector = iter->second;
        ConvertSegmentIdToVirtual(partitionIdx, &patchInfoVector);
        (*multiPartPatchInfos)[virtualSegmentId] = patchInfoVector;
    }
}

void MultiPartPatchFinder::ConvertSegmentIdToVirtual(size_t partitionIdx, PatchFileInfoVec* patchInfoVector)
{
    for (size_t i = 0; i < patchInfoVector->size(); i++) {
        ConvertSegmentIdToVirtual(partitionIdx, &(patchInfoVector->at(i)));
    }
}

void MultiPartPatchFinder::ConvertSegmentIdToVirtual(size_t partitionIdx, PatchFileInfo* patchInfo)
{
    segmentid_t virtualSrcSegmentId = mSegmentDirectory->EncodeToVirtualSegmentId(partitionIdx, patchInfo->srcSegment);
    if (virtualSrcSegmentId == INVALID_SEGMENTID) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "patch src segmentid %d dest segmentid %d in partition %lu not exist",
                             patchInfo->srcSegment, patchInfo->destSegment, partitionIdx);
    }
    patchInfo->srcSegment = virtualSrcSegmentId;
    if (patchInfo->destSegment != INVALID_SEGMENTID) {
        segmentid_t virtualDestSegmentId =
            mSegmentDirectory->EncodeToVirtualSegmentId(partitionIdx, patchInfo->destSegment);
        if (virtualSrcSegmentId == INVALID_SEGMENTID) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "patch src segmentid %d dest segmentid %d in partition %lu not exist",
                                 patchInfo->srcSegment, patchInfo->destSegment, partitionIdx);
        }
        patchInfo->destSegment = virtualDestSegmentId;
    }
}

void MultiPartPatchFinder::AddDeletionMapFiles(size_t partitionIdx, const DeletePatchInfos& singlePartPatchInfos,
                                               DeletePatchInfos& multiPartPatchInfos)
{
    DeletePatchInfos::const_iterator iter = singlePartPatchInfos.begin();
    for (; iter != singlePartPatchInfos.end(); iter++) {
        segmentid_t virtualSegmentId = mSegmentDirectory->EncodeToVirtualSegmentId(partitionIdx, iter->first);
        if (virtualSegmentId == INVALID_SEGMENTID) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "deletionmap for segmentid %d in partition %lu not exist", iter->first,
                                 partitionIdx);
        }
        PatchFileInfo patchInfo = iter->second;
        ConvertSegmentIdToVirtual(partitionIdx, &patchInfo);
        multiPartPatchInfos[virtualSegmentId] = patchInfo;
    }
}

void MultiPartPatchFinder::FindPatchFiles(PatchType patchType, const std::string& dirName, schema_opid_t opId,
                                          PatchInfos* patchInfos)
{
    assert(patchInfos->empty());
    const std::vector<SegmentDirectoryPtr>& segmentDirectoryVec = mSegmentDirectory->GetSegmentDirectories();
    for (size_t i = 0; i < segmentDirectoryVec.size(); i++) {
        PatchInfos tmpPatchInfos;
        SinglePartPatchFinder patchFinder;
        patchFinder.Init(segmentDirectoryVec[i]);
        patchFinder.FindPatchFiles(patchType, dirName, opId, &tmpPatchInfos);
        AddPatchFiles(i, tmpPatchInfos, patchInfos);
    }
}

void MultiPartPatchFinder::FindPatchFilesForTargetSegment(PatchType patchType, const std::string& dirName,
                                                          segmentid_t targetSegmentId, schema_opid_t opId,
                                                          PatchFileInfoVec* patchInfo)
{
    segmentid_t physicalSegId = INVALID_SEGMENTID;
    size_t partitionIdx = 0;
    if (!mSegmentDirectory->DecodeVirtualSegmentId(targetSegmentId, physicalSegId, partitionIdx)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "virtual segmentid %d not exist", targetSegmentId);
    }

    const std::vector<SegmentDirectoryPtr>& segmentDirectoryVec = mSegmentDirectory->GetSegmentDirectories();
    assert(partitionIdx < segmentDirectoryVec.size());

    SinglePartPatchFinder patchFinder;
    patchFinder.Init(segmentDirectoryVec[partitionIdx]);
    patchFinder.FindPatchFilesForTargetSegment(patchType, dirName, physicalSegId, opId, patchInfo);

    ConvertSegmentIdToVirtual(partitionIdx, patchInfo);
}

void MultiPartPatchFinder::FindPatchFilesFromSegment(PatchType patchType, const std::string& dirName,
                                                     segmentid_t fromSegmentId, schema_opid_t opId,
                                                     PatchInfos* patchInfos)
{
    segmentid_t physicalSegId = INVALID_SEGMENTID;
    size_t partitionIdx = 0;
    if (!mSegmentDirectory->DecodeVirtualSegmentId(fromSegmentId, physicalSegId, partitionIdx)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "virtual segmentid %d not exist", fromSegmentId);
    }

    const std::vector<SegmentDirectoryPtr>& segmentDirectoryVec = mSegmentDirectory->GetSegmentDirectories();
    assert(partitionIdx < segmentDirectoryVec.size());

    PatchInfos tmpPatchInfos;
    SinglePartPatchFinder patchFinder;
    patchFinder.Init(segmentDirectoryVec[partitionIdx]);
    patchFinder.FindPatchFilesFromSegment(patchType, dirName, physicalSegId, opId, &tmpPatchInfos);
    AddPatchFiles(partitionIdx, tmpPatchInfos, patchInfos);
}
}} // namespace indexlib::index_base
