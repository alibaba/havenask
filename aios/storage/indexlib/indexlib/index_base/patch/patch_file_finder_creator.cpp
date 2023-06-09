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
#include "indexlib/index_base/patch/patch_file_finder_creator.h"

#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/patch/multi_part_patch_finder.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/patch/single_part_patch_finder.h"
#include "indexlib/index_base/segment/multi_part_segment_directory.h"

using namespace std;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, PatchFileFinderCreator);

PatchFileFinderCreator::PatchFileFinderCreator() {}

PatchFileFinderCreator::~PatchFileFinderCreator() {}

PatchFileFinderPtr PatchFileFinderCreator::Create(PartitionData* partitionData)
{
    assert(partitionData);
    const SegmentDirectoryPtr& segDir = partitionData->GetSegmentDirectory();
    assert(segDir);

    MultiPartSegmentDirectoryPtr multiSegDir = DYNAMIC_POINTER_CAST(MultiPartSegmentDirectory, segDir);
    if (multiSegDir) {
        MultiPartPatchFinderPtr patchFinder(new MultiPartPatchFinder);
        patchFinder->Init(multiSegDir);
        return patchFinder;
    }

    SinglePartPatchFinderPtr singlePatchFinder(new SinglePartPatchFinder);
    singlePatchFinder->Init(segDir);
    return singlePatchFinder;
}
}} // namespace indexlib::index_base
