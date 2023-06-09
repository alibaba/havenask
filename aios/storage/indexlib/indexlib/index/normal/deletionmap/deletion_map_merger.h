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
#ifndef __INDEXLIB_DELETION_MAP_MERGER_H
#define __INDEXLIB_DELETION_MAP_MERGER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/util/merger_resource.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, SegmentDirectoryBase);
DECLARE_REFERENCE_CLASS(index, ReclaimMap);
DECLARE_REFERENCE_STRUCT(index_base, PatchFileInfo);

namespace indexlib { namespace index {

class DeletionMapMerger
{
public:
    DeletionMapMerger() {}
    virtual ~DeletionMapMerger() {}

public:
    void BeginMerge(const SegmentDirectoryBasePtr& segDir);

    void Merge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
               const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    void SortByWeightMerge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, bool isSortedMerge) const;

private:
    void InnerMerge(const file_system::DirectoryPtr& directory, const index_base::SegmentMergeInfos& segMergeInfos,
                    const ReclaimMapPtr& reclaimMap);
    bool InMergePlan(segmentid_t segId, const index_base::SegmentMergeInfos& segMergeInfos);
    static void CopyOnePatchFile(const index_base::PatchFileInfo& patchFileInfo,
                                 const file_system::DirectoryPtr& directory);

private:
    SegmentDirectoryBasePtr mSegmentDirectory;

private:
    friend class DeletionMapMergeTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DeletionMapMerger);
}} // namespace indexlib::index

#endif //__INDEXLIB_DELETION_MAP_MERGER_H
