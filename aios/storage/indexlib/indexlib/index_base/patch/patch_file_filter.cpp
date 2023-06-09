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
#include "indexlib/index_base/patch/patch_file_filter.h"

#include "indexlib/index_base/segment/segment_data.h"

using namespace indexlib::index_base;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, PatchFileFilter);

PatchFileFilter::PatchFileFilter(const PartitionDataPtr& partitionData, bool ignorePatchToOldIncSegment,
                                 segmentid_t startLoadSegment)
    : _partitionData(partitionData)
    , _startLoadSegment(startLoadSegment)
    , _ignorePatchToOldIncSegment(ignorePatchToOldIncSegment)
{
}

PatchFileFilter::~PatchFileFilter() {}

PatchInfos PatchFileFilter::Filter(const PatchInfos& patchInfos)
{
    PatchInfos filtPatchInfos;
    PartitionData::Iterator iter = _partitionData->Begin();
    for (; iter != _partitionData->End(); iter++) {
        const SegmentData& segmentData = *iter;
        segmentid_t destSegmentId = segmentData.GetSegmentId();

        if (_ignorePatchToOldIncSegment && destSegmentId < _startLoadSegment) {
            continue;
        }
        PatchInfos::const_iterator iter = patchInfos.find(destSegmentId);
        if (iter == patchInfos.end()) {
            continue;
        }

        const PatchFileInfoVec& patchVec = iter->second;
        PatchFileInfoVec validPatchInfos = FilterLoadedPatchFileInfos(patchVec, _startLoadSegment);
        if (validPatchInfos.empty()) {
            continue;
        }
        filtPatchInfos[destSegmentId] = validPatchInfos;
    }
    return filtPatchInfos;
}

PatchFileInfoVec PatchFileFilter::FilterLoadedPatchFileInfos(const PatchFileInfoVec& patchInfosVec,
                                                             segmentid_t startLoadSegment)
{
    PatchFileInfoVec validPatchInfos;
    for (size_t i = 0; i < patchInfosVec.size(); i++) {
        if (patchInfosVec[i].srcSegment >= startLoadSegment) {
            validPatchInfos.push_back(patchInfosVec[i]);
        }
    }
    return validPatchInfos;
}
}} // namespace indexlib::index_base
