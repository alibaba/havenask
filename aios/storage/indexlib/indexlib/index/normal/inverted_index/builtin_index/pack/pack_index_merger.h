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
#ifndef __INDEXLIB_PACK_INDEX_MERGER_H
#define __INDEXLIB_PACK_INDEX_MERGER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/index/normal/framework/index_merger_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_merger.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

class PackIndexMerger : public NormalIndexMerger
{
public:
    typedef AttributeMergerPtr SectionAttributeMergerPtr;

public:
    PackIndexMerger();
    ~PackIndexMerger();

    DECLARE_INDEX_MERGER_IDENTIFIER(pack);
    DECLARE_INDEX_MERGER_CREATOR(PackIndexMerger, it_pack);

public:
    void Init(const config::IndexConfigPtr& indexConfig) override;

    void BeginMerge(const SegmentDirectoryBasePtr& segDir) override;

    void Merge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
               const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    void SortByWeightMerge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    std::shared_ptr<OnDiskIndexIteratorCreator> CreateOnDiskIndexIteratorCreator() override;

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortedMerge) const override;

private:
    SectionAttributeMergerPtr mSectionMerger;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackIndexMerger);
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_PACK_INDEX_MERGER_H
