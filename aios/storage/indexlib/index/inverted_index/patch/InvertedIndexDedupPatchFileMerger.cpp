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
#include "indexlib/index/inverted_index/patch/InvertedIndexDedupPatchFileMerger.h"

#include "indexlib/framework/Segment.h"
#include "indexlib/index/inverted_index/patch/InvertedIndexPatchFileFinder.h"
#include "indexlib/index/inverted_index/patch/InvertedIndexPatchMerger.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InvertedIndexDedupPatchFileMerger);

Status InvertedIndexDedupPatchFileMerger::FindPatchFiles(
    const indexlibv2::index::IIndexMerger::SegmentMergeInfos& segMergeInfos, indexlibv2::index::PatchInfos* patchInfos)
{
    std::vector<std::shared_ptr<indexlibv2::framework::Segment>> segments;
    for (auto srcSegment : segMergeInfos.srcSegments) {
        segments.push_back(srcSegment.segment);
    }

    auto patchFinder = std::make_unique<InvertedIndexPatchFileFinder>();
    auto status = patchFinder->FindAllPatchFiles(segments, _invertedIndexConfig, patchInfos);
    RETURN_IF_STATUS_ERROR(status, "find patch file fail for inverted index [%s].",
                           _invertedIndexConfig->GetIndexName().c_str());
    return Status::OK();
}

std::shared_ptr<indexlibv2::index::PatchMerger> InvertedIndexDedupPatchFileMerger::CreatePatchMerger(segmentid_t) const
{
    auto patchMerger = std::make_shared<InvertedIndexPatchMerger>(_invertedIndexConfig);
    return std::dynamic_pointer_cast<indexlibv2::index::PatchMerger>(patchMerger);
}

} // namespace indexlib::index
