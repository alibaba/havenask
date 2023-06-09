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
#include "indexlib/index/normal/accessor/patch_file_merger.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);

namespace indexlib { namespace index {

class IndexDedupPatchFileMerger : public PatchFileMerger
{
public:
    IndexDedupPatchFileMerger(const config::IndexConfigPtr& indexConfig) : _indexConfig(indexConfig) {}

private:
    PatchMergerPtr CreatePatchMerger(segmentid_t segId) const override;
    void FindPatchFiles(const index_base::PartitionDataPtr& partitionData,
                        const index_base::SegmentMergeInfos& segMergeInfos,
                        index_base::PatchInfos* patchesInMergeSegment, index_base::PatchInfos* allPatches) override;

private:
    config::IndexConfigPtr _indexConfig;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
