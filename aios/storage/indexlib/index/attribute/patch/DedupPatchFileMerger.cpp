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
#include "indexlib/index/attribute/patch/DedupPatchFileMerger.h"

#include "indexlib/framework/Segment.h"
#include "indexlib/index/attribute/SegmentUpdateBitmap.h"
#include "indexlib/index/attribute/patch/AttributePatchFileFinder.h"
#include "indexlib/index/attribute/patch/AttributePatchMerger.h"
#include "indexlib/index/attribute/patch/AttributePatchMergerFactory.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, DedupPatchFileMerger);

Status DedupPatchFileMerger::FindPatchFiles(const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                                            PatchInfos* patchInfos)
{
    std::vector<std::shared_ptr<framework::Segment>> segments;
    for (auto srcSegment : segMergeInfos.srcSegments) {
        segments.push_back(srcSegment.segment);
    }

    auto patchFinder = std::make_unique<AttributePatchFileFinder>();
    auto status = patchFinder->FindAllPatchFiles(segments, _attrConfig, patchInfos);
    RETURN_IF_STATUS_ERROR(status, "find patch file fail for attribute [%s].", _attrConfig->GetAttrName().c_str());
    return Status::OK();
}

std::shared_ptr<PatchMerger> DedupPatchFileMerger::CreatePatchMerger(segmentid_t segId) const
{
    std::shared_ptr<SegmentUpdateBitmap> segUpdateBitmap;
    if (_attrUpdateBitmap) {
        segUpdateBitmap = _attrUpdateBitmap->GetSegmentBitmap(segId);
    }
    std::shared_ptr<AttributePatchMerger> patchMerger =
        AttributePatchMergerFactory::Create(_attrConfig, segUpdateBitmap);
    return std::dynamic_pointer_cast<PatchMerger>(patchMerger);
}

} // namespace indexlibv2::index
