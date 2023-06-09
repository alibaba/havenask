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
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"

#include "indexlib/config/attribute_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"

using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace index {

void AttributePatchReader::Init(const PartitionDataPtr& partitionData, segmentid_t segmentId)
{
    PatchInfos attrPatchInfos;
    PatchFileFinderPtr patchFinder = PatchFileFinderCreator::Create(partitionData.get());

    patchFinder->FindAttrPatchFiles(mAttrConfig, &attrPatchInfos);
    PatchFileInfoVec patchInfoVec = attrPatchInfos[segmentId];
    for (size_t i = 0; i < patchInfoVec.size(); i++) {
        AddPatchFile(patchInfoVec[i].patchDirectory, patchInfoVec[i].patchFileName, patchInfoVec[i].srcSegment);
    }
}
}} // namespace indexlib::index
