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
#include "indexlib/index/common/patch/PatchFileMerger.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/common/patch/PatchMerger.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PatchFileMerger);

Status PatchFileMerger::Merge(const IIndexMerger::SegmentMergeInfos& segMergeInfos)
{
    auto& targetSegments = segMergeInfos.targetSegments;
    if (targetSegments.empty()) {
        AUTIL_LOG(ERROR, "invalid target segments");
        return Status::InternalError("invalid target segments");
    }
    auto it = targetSegments.rbegin();
    auto targetSegmentDir = (*it)->segmentDir;
    if (targetSegmentDir == nullptr) {
        AUTIL_LOG(ERROR, "invalid target segment directory");
        return Status::InternalError("invalid target segments directory");
    }

    if (segMergeInfos.srcSegments.size() == 0) {
        AUTIL_LOG(INFO, "segment source merge infos is empty");
        return Status::OK();
    }

    PatchInfos patchInfos;
    auto status = CollectPatchFiles(segMergeInfos, &patchInfos);
    RETURN_IF_STATUS_ERROR(status, "collect patch files failed");
    RETURN_IF_STATUS_ERROR(DoMergePatchFiles(patchInfos, targetSegmentDir->GetIDirectory()),
                           "merge patch operation failed.");
    return Status::OK();
}

Status PatchFileMerger::CollectPatchFiles(const IIndexMerger::SegmentMergeInfos& segMergeInfos, PatchInfos* patchInfos)
{
    PatchInfos patchesInMergeSegment;
    // 1) first, find from segment dir
    //    a) the srcSegment in PatchFileInfo in merged segment maybe not equal to it's own segment
    //       .e.g in segment 4 maybe has 3_1.patch
    auto status = FindPatchFiles(segMergeInfos, &patchesInMergeSegment);
    RETURN_IF_STATUS_ERROR(status, "find patch info fail.");

    // 2) second, find from op log dir(_allPatchInfos include the infos)
    //    a) for the scene of {2, 536870920, 536870921, 536870922} -> {2, 3},
    //    b) if build segment has op log for segment2, the patch only exist in op log dir, not segment dir
    //    c) op log dir patch file list example: 3_2.patch(build segment for merged segment patch),
    //    536870921_536870920.patch, 536870922_536870921.patch d) op log patch we only collect 3_2.patch, because
    //    536870921_536870920.patch, 536870922_536870921.patch will be merged
    std::set<segmentid_t> srcSegmentIds;
    for (auto targetSegment : segMergeInfos.targetSegments) {
        srcSegmentIds.insert(targetSegment->segmentId);
    }

    for (const auto& [_, patchFiles] : _allPatchInfos) {
        for (size_t i = 0; i < patchFiles.Size(); ++i) {
            const auto& patchFile = patchFiles[i];
            const auto srcSegId = patchFile.srcSegment;
            const auto dstSegId = patchFile.destSegment;
            if (srcSegmentIds.find(srcSegId) == srcSegmentIds.end()) {
                continue;
            }
            auto it = patchesInMergeSegment.find(dstSegId);
            if (it == patchesInMergeSegment.end()) {
                PatchFileInfos files;
                files.PushBack(patchFile);
                patchesInMergeSegment.insert(it, std::make_pair(dstSegId, std::move(files)));
            } else {
                it->second.PushBack(patchFile);
            }
        }
    }
    // 3) filter src segment patch, collect target segment patch range from [minSrcSegment, maxSrcSegment]
    PatchInfos::iterator iter = patchesInMergeSegment.begin();
    for (; iter != patchesInMergeSegment.end(); iter++) {
        if (NeedMergePatch(segMergeInfos, iter->first)) {
            // find all patches in range [minSrcSegment, maxSrcSegment]
            auto st = GetNeedMergePatchInfos(iter->first, iter->second, patchInfos);
            if (!st.IsOK()) {
                AUTIL_LOG(ERROR, "get need merge patch info failed, destSegId[%d]", iter->first);
                return st;
            }
        }
    }
    return Status::OK();
}

bool PatchFileMerger::NeedMergePatch(const IIndexMerger::SegmentMergeInfos& segMergeInfos, segmentid_t destSegId)
{
    PatchInfos::const_iterator iter = _allPatchInfos.find(destSegId);
    if (iter == _allPatchInfos.end()) {
        // patch is useless in current version
        return false;
    }
    for (const auto& srcSegment : segMergeInfos.srcSegments) {
        if (srcSegment.segment->GetSegmentId() == destSegId) {
            return false;
        }
    }
    return true;
}

Status PatchFileMerger::GetNeedMergePatchInfos(segmentid_t destSegId, const PatchFileInfos& inMergeSegmentPatches,
                                               PatchInfos* mergePatchInfos)
{
    segmentid_t minSrcSegment = INVALID_SEGMENTID;
    segmentid_t maxSrcSegment = INVALID_SEGMENTID;
    FindSrcSegmentRange(inMergeSegmentPatches, &minSrcSegment, &maxSrcSegment);

    PatchInfos::const_iterator iter = _allPatchInfos.find(destSegId);
    if (iter == _allPatchInfos.end()) {
        AUTIL_LOG(ERROR, "patch file for dest segment[%d] not exist.", destSegId);
        return Status::InternalError("patch file for dest segment not exist, segId:", destSegId);
    }

    const PatchFileInfos& destSegmentPathInfos = iter->second;
    for (size_t i = 0; i < destSegmentPathInfos.Size(); i++) {
        segmentid_t srcSegment = destSegmentPathInfos[i].srcSegment;
        if (srcSegment >= minSrcSegment && srcSegment <= maxSrcSegment) {
            (*mergePatchInfos)[destSegId].PushBack(destSegmentPathInfos[i]);
        }
    }
    return Status::OK();
}

void PatchFileMerger::FindSrcSegmentRange(const PatchFileInfos& patchFileInfos, segmentid_t* minSrcSegId,
                                          segmentid_t* maxSrcSegId)
{
    for (size_t i = 0; i < patchFileInfos.Size(); i++) {
        if (i == 0) {
            *minSrcSegId = patchFileInfos[i].srcSegment;
            *maxSrcSegId = patchFileInfos[i].srcSegment;
            continue;
        }

        if (patchFileInfos[i].srcSegment < *minSrcSegId) {
            *minSrcSegId = patchFileInfos[i].srcSegment;
            continue;
        }

        if (patchFileInfos[i].srcSegment > *maxSrcSegId) {
            *maxSrcSegId = patchFileInfos[i].srcSegment;
        }
    }
}

Status PatchFileMerger::DoMergePatchFiles(const PatchInfos& patchInfos,
                                          const std::shared_ptr<indexlib::file_system::IDirectory>& targetSegmentDir)
{
    for (PatchInfos::const_iterator iter = patchInfos.begin(); iter != patchInfos.end(); ++iter) {
        segmentid_t srcSegId = INVALID_SEGMENTID;
        const PatchFileInfos& patchFileInfos = iter->second;
        for (size_t i = 0; i < patchFileInfos.Size(); ++i) {
            if (patchFileInfos[i].srcSegment > srcSegId) {
                srcSegId = patchFileInfos[i].srcSegment;
            }
        }
        auto patchMerger = CreatePatchMerger(iter->first);
        assert(srcSegId != INVALID_SEGMENTID);

        auto [status, targetPatchFileWriter] =
            patchMerger->CreateTargetPatchFileWriter(targetSegmentDir, srcSegId, iter->first);
        RETURN_IF_STATUS_ERROR(status, "create patch file writer failed, patch file: %s ",
                               targetPatchFileWriter->GetLogicalPath().c_str());

        status = patchMerger->Merge(iter->second, targetPatchFileWriter);
        RETURN_IF_STATUS_ERROR(status, "merge patch file failed, patch file: %s ",
                               targetPatchFileWriter->GetLogicalPath().c_str());

        status = targetPatchFileWriter->Close().Status();
        RETURN_IF_STATUS_ERROR(status, "close patch file writer failed, patch file: %s",
                               targetPatchFileWriter->GetLogicalPath().c_str());
    }
    return Status::OK();
}

} // namespace indexlibv2::index
