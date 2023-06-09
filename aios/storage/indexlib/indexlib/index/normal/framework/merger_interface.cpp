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
#include "indexlib/index/normal/framework/merger_interface.h"
namespace indexlib { namespace index {
IE_LOG_SETUP(index, MergerInterface);

std::vector<index_base::ParallelMergeItem> MergerInterface::CreateParallelMergeItems(
    const SegmentDirectoryBasePtr& segDir, const index_base::SegmentMergeInfos& inPlanSegMergeInfos,
    uint32_t instanceCount, bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr) const
{
    return std::vector<index_base::ParallelMergeItem>();
}

void MergerInterface::EndParallelMerge(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                       int32_t totalParallelCount,
                                       const std::vector<index_base::MergeTaskResourceVector>& instResourceVec)
{
    assert(false);
    INDEXLIB_FATAL_ERROR(UnSupported, "built-in index not support parallel task merge!");
}
}} // namespace indexlib::index
