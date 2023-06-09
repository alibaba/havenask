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
#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index, PatchMerger);

namespace indexlib { namespace index {

class PatchFileMerger
{
public:
    PatchFileMerger() {}
    virtual ~PatchFileMerger() {}

public:
    void Merge(const index_base::PartitionDataPtr& partitionData, const index_base::SegmentMergeInfos& segMergeInfos,
               const file_system::DirectoryPtr& targetDir);

public:
    void CollectPatchFiles(const index_base::PartitionDataPtr& partitionData,
                           const index_base::SegmentMergeInfos& segMergeInfos, index_base::PatchInfos* patchInfos);

public:
    static bool NeedMergePatch(const index_base::Version& version, const index_base::SegmentMergeInfos& segMergeInfos,
                               segmentid_t destSegId);

    static void GetNeedMergePatchInfos(segmentid_t destSegment,
                                       const index_base::PatchFileInfoVec& inMergeSegmentPatches,
                                       const index_base::PatchInfos& allPatchInfos,
                                       index_base::PatchInfos* mergePatchInfos);

    static void FindSrcSegmentRange(const index_base::PatchFileInfoVec& patchFiles, segmentid_t* minSrcSegId,
                                    segmentid_t* maxSrcSegId);

private:
    void DoMergePatchFiles(const index_base::PatchInfos& patchInfos, const file_system::DirectoryPtr& targetDir);

private:
    virtual PatchMergerPtr CreatePatchMerger(segmentid_t segId) const = 0;
    virtual void FindPatchFiles(const index_base::PartitionDataPtr& partitionData,
                                const index_base::SegmentMergeInfos& segMergeInfos,
                                index_base::PatchInfos* patchesInMergeSegment, index_base::PatchInfos* allPatches) = 0;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
