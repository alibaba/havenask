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
#include "indexlib/index/normal/attribute/accessor/dedup_patch_file_merger.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_merger.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_merger_factory.h"
#include "indexlib/index/normal/attribute/attribute_update_bitmap.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DedupPatchFileMerger);

DedupPatchFileMerger::DedupPatchFileMerger(const AttributeConfigPtr& attrConfig,
                                           const AttributeUpdateBitmapPtr& attrUpdateBitmap)
    : _attributeConfig(attrConfig)
    , _attrUpdateBitmap(attrUpdateBitmap)
{
}

void DedupPatchFileMerger::FindPatchFiles(const index_base::PartitionDataPtr& partitionData,
                                          const index_base::SegmentMergeInfos& segMergeInfos,
                                          index_base::PatchInfos* patchesInMergeSegment,
                                          index_base::PatchInfos* allPatches)
{
    PatchFileFinderPtr patchFinder = PatchFileFinderCreator::Create(partitionData.get());
    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        segmentid_t segId = segMergeInfos[i].segmentId;
        patchFinder->FindAttrPatchFilesFromSegment(_attributeConfig, segId, patchesInMergeSegment);
    }
    patchFinder->FindAttrPatchFiles(_attributeConfig, allPatches);
}

PatchMergerPtr DedupPatchFileMerger::CreatePatchMerger(segmentid_t segId) const
{
    SegmentUpdateBitmapPtr segUpdateBitmap;
    if (_attrUpdateBitmap) {
        segUpdateBitmap = _attrUpdateBitmap->GetSegmentBitmap(segId);
    }
    PatchMergerPtr patchMerger(AttributePatchMergerFactory::Create(_attributeConfig, segUpdateBitmap));
    return patchMerger;
}
}} // namespace indexlib::index
