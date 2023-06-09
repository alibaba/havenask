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
#include "indexlib/index/normal/attribute/accessor/dedup_pack_attribute_patch_file_merger.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/attribute/accessor/dedup_patch_file_merger.h"
#include "indexlib/index/normal/attribute/attribute_update_bitmap.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/util/PathUtil.h"

using namespace std;

using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DedupPackAttributePatchFileMerger);

void DedupPackAttributePatchFileMerger::Merge(const PartitionDataPtr& partitionData,
                                              const SegmentMergeInfos& segMergeInfos, const DirectoryPtr& targetAttrDir)
{
    AttributeUpdateBitmapPtr packAttrUpdateBitmap(new AttributeUpdateBitmap());
    packAttrUpdateBitmap->Init(partitionData);

    const vector<AttributeConfigPtr>& attrConfVec = mPackAttrConfig->GetAttributeConfigVec();

    for (size_t i = 0; i < attrConfVec.size(); ++i) {
        RemoveOption removeOption = RemoveOption::MayNonExist();
        targetAttrDir->RemoveDirectory(attrConfVec[i]->GetAttrName(), removeOption);
        DirectoryPtr subAttrDir = targetAttrDir->MakeDirectory(attrConfVec[i]->GetAttrName());
        DedupPatchFileMerger subAttrPatchMerger(attrConfVec[i], packAttrUpdateBitmap);
        subAttrPatchMerger.Merge(partitionData, segMergeInfos, subAttrDir);
    }
    packAttrUpdateBitmap->Dump(targetAttrDir);
}

uint32_t DedupPackAttributePatchFileMerger::EstimateMemoryUse(const PartitionDataPtr& partitionData,
                                                              const SegmentMergeInfos& segMergeInfos)
{
    const vector<AttributeConfigPtr>& attrConfVec = mPackAttrConfig->GetAttributeConfigVec();

    PatchInfos patchInfos;
    for (size_t i = 0; i < attrConfVec.size(); ++i) {
        if (!attrConfVec[i]->IsAttributeUpdatable()) {
            continue;
        }
        DedupPatchFileMerger subAttrPatchMerger(attrConfVec[i]);
        subAttrPatchMerger.CollectPatchFiles(partitionData, segMergeInfos, &patchInfos);
    }

    int64_t totalDocCount = 0;
    PatchInfos::iterator iter = patchInfos.begin();
    for (; iter != patchInfos.end(); ++iter) {
        segmentid_t segId = iter->first;
        totalDocCount += partitionData->GetSegmentData(segId).GetSegmentInfo()->docCount;
    }
    return Bitmap::GetDumpSize(totalDocCount);
}
}} // namespace indexlib::index
