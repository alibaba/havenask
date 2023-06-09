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
#include "indexlib/index/normal/inverted_index/accessor/index_dedup_patch_file_merger.h"

#include "indexlib/config/index_config.h"
#include "indexlib/index/normal/inverted_index/accessor/index_patch_merger.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"

namespace indexlib { namespace index {
IE_LOG_SETUP(index, IndexDedupPatchFileMerger);

PatchMergerPtr IndexDedupPatchFileMerger::CreatePatchMerger(segmentid_t segId) const
{
    return IndexPatchMergerPtr(new IndexPatchMerger(_indexConfig));
}

void IndexDedupPatchFileMerger::FindPatchFiles(const index_base::PartitionDataPtr& partitionData,
                                               const index_base::SegmentMergeInfos& segMergeInfos,
                                               index_base::PatchInfos* patchesInMergeSegment,
                                               index_base::PatchInfos* allPatches)
{
    index_base::PatchFileFinderPtr patchFinder = index_base::PatchFileFinderCreator::Create(partitionData.get());
    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        segmentid_t segId = segMergeInfos[i].segmentId;
        patchFinder->FindIndexPatchFilesFromSegment(_indexConfig, segId, patchesInMergeSegment);
    }
    patchFinder->FindIndexPatchFiles(_indexConfig, allPatches);
}
}} // namespace indexlib::index
