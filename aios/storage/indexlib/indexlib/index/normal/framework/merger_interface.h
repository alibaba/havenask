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
#include "indexlib/index/util/merger_resource.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, ReclaimMap);
DECLARE_REFERENCE_CLASS(index, SegmentDirectoryBase);
DECLARE_REFERENCE_CLASS(index_base, MergeTaskResourceManager);
namespace indexlib { namespace index {

// interface for merger
class MergerInterface
{
public:
    virtual void BeginMerge(const SegmentDirectoryBasePtr& segDir) = 0;

    virtual void Merge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                       const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) = 0;

    virtual void SortByWeightMerge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                                   const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) = 0;

    virtual int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                                      const index_base::SegmentMergeInfos& segMergeInfos,
                                      const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                      bool isSortedMerge) const = 0;

    virtual bool EnableMultiOutputSegmentParallel() const { return false; }

    virtual std::vector<index_base::ParallelMergeItem>
    CreateParallelMergeItems(const SegmentDirectoryBasePtr& segDir,
                             const index_base::SegmentMergeInfos& inPlanSegMergeInfos, uint32_t instanceCount,
                             bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr) const;

    virtual void EndParallelMerge(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                  int32_t totalParallelCount,
                                  const std::vector<index_base::MergeTaskResourceVector>& instResourceVec);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergerInterface);
}} // namespace indexlib::index
