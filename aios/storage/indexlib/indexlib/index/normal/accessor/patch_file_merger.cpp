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
#include "indexlib/index/normal/accessor/patch_file_merger.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/normal/accessor/patch_merger.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"

namespace indexlib { namespace index {
IE_LOG_SETUP(index, PatchFileMerger);

void PatchFileMerger::Merge(const index_base::PartitionDataPtr& partitionData,
                            const index_base::SegmentMergeInfos& segMergeInfos,
                            const file_system::DirectoryPtr& targetAttrDir)
{
    index_base::Version version = partitionData->GetVersion();
    if (version.GetSegmentCount() == segMergeInfos.size()) {
        // optimize merge no need merge patch file
        return;
    }
    if (segMergeInfos.size() == 0) {
        IE_LOG(INFO, "segMergeInfos is empty");
        return;
    }

    index_base::PatchInfos patchInfos;
    CollectPatchFiles(partitionData, segMergeInfos, &patchInfos);
    DoMergePatchFiles(patchInfos, targetAttrDir);
}

void PatchFileMerger::CollectPatchFiles(const index_base::PartitionDataPtr& partitionData,
                                        const index_base::SegmentMergeInfos& segMergeInfos,
                                        index_base::PatchInfos* patchInfos)
{
    index_base::PatchInfos patchesInMergeSegment;
    index_base::PatchInfos allPatches;
    FindPatchFiles(partitionData, segMergeInfos, &patchesInMergeSegment, &allPatches);
    index_base::Version version = partitionData->GetVersion();
    index_base::PatchInfos::iterator iter = patchesInMergeSegment.begin();
    for (; iter != patchesInMergeSegment.end(); iter++) {
        if (NeedMergePatch(version, segMergeInfos, iter->first)) {
            GetNeedMergePatchInfos(iter->first, iter->second, allPatches, patchInfos);
        }
    }
}

bool PatchFileMerger::NeedMergePatch(const index_base::Version& version,
                                     const index_base::SegmentMergeInfos& segMergeInfos, segmentid_t destSegId)
{
    if (!version.HasSegment(destSegId)) {
        return false;
    }

    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        if (segMergeInfos[i].segmentId == destSegId) {
            return false;
        }
    }
    return true;
}

void PatchFileMerger::GetNeedMergePatchInfos(segmentid_t destSegment,
                                             const index_base::PatchFileInfoVec& inMergeSegmentPatches,
                                             const index_base::PatchInfos& allPatchInfos,
                                             index_base::PatchInfos* mergePatchInfos)
{
    segmentid_t minSrcSegment = INVALID_SEGMENTID;
    segmentid_t maxSrcSegment = INVALID_SEGMENTID;
    FindSrcSegmentRange(inMergeSegmentPatches, &minSrcSegment, &maxSrcSegment);

    index_base::PatchInfos::const_iterator iter = allPatchInfos.find(destSegment);
    if (iter == allPatchInfos.end()) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "patch file for dest segment[%d] not exist!", destSegment);
    }

    const index_base::PatchFileInfoVec& destSegmentPathInfos = iter->second;
    for (size_t i = 0; i < destSegmentPathInfos.size(); i++) {
        segmentid_t srcSegment = destSegmentPathInfos[i].srcSegment;
        if (srcSegment >= minSrcSegment && srcSegment <= maxSrcSegment) {
            (*mergePatchInfos)[destSegment].push_back(destSegmentPathInfos[i]);
        }
    }
}

void PatchFileMerger::FindSrcSegmentRange(const index_base::PatchFileInfoVec& patchFiles, segmentid_t* minSrcSegId,
                                          segmentid_t* maxSrcSegId)
{
    for (size_t i = 0; i < patchFiles.size(); i++) {
        if (i == 0) {
            *minSrcSegId = patchFiles[i].srcSegment;
            *maxSrcSegId = patchFiles[i].srcSegment;
            continue;
        }

        if (patchFiles[i].srcSegment < *minSrcSegId) {
            *minSrcSegId = patchFiles[i].srcSegment;
            continue;
        }

        if (patchFiles[i].srcSegment > *maxSrcSegId) {
            *maxSrcSegId = patchFiles[i].srcSegment;
        }
    }
}

void PatchFileMerger::DoMergePatchFiles(const index_base::PatchInfos& patchInfos,
                                        const file_system::DirectoryPtr& targetDir)
{
    for (index_base::PatchInfos::const_iterator iter = patchInfos.begin(); iter != patchInfos.end(); ++iter) {
        segmentid_t srcSegId = INVALID_SEGMENTID;
        const index_base::PatchFileInfoVec& patchInfoVec = iter->second;
        for (size_t i = 0; i < patchInfoVec.size(); ++i) {
            if (patchInfoVec[i].srcSegment > srcSegId) {
                srcSegId = patchInfoVec[i].srcSegment;
            }
        }
        assert(srcSegId != INVALID_SEGMENTID);
        std::string patchFileName = autil::StringUtil::toString(srcSegId) + "_" +
                                    autil::StringUtil::toString(iter->first) + "." + PATCH_FILE_NAME;
        file_system::FileWriterPtr destPatchFileWriter = targetDir->CreateFileWriter(patchFileName);
        PatchMergerPtr patchMerger = CreatePatchMerger(iter->first);
        patchMerger->Merge(iter->second, destPatchFileWriter);
        destPatchFileWriter->Close().GetOrThrow();
    }
}
}} // namespace indexlib::index
