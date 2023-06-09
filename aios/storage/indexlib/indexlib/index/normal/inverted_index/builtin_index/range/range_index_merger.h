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
#ifndef __INDEXLIB_RANGE_INDEX_MERGER_H
#define __INDEXLIB_RANGE_INDEX_MERGER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeInfo.h"
#include "indexlib/index/normal/framework/index_merger_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/on_disk_range_index_iterator_creator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

class RangeLevelIndexMerger : public NormalIndexMerger
{
    using NormalIndexMerger::Init;

public:
    explicit RangeLevelIndexMerger() {}
    ~RangeLevelIndexMerger() {}

    DECLARE_INDEX_MERGER_IDENTIFIER(range_level);

public:
    OnDiskIndexIteratorCreatorPtr CreateOnDiskIndexIteratorCreator() override
    {
        return OnDiskIndexIteratorCreatorPtr(
            new OnDiskRangeIndexIteratorCreator(GetPostingFormatOption(), mIOConfig, mIndexConfig, mParentIndexName));
    }

    void Init(const std::string& parentIndexName, const config::IndexConfigPtr& indexConfig,
              const index_base::MergeItemHint& hint, const index_base::MergeTaskResourceVector& taskResources,
              const config::MergeIOConfig& ioConfig, TruncateIndexWriterCreator* truncateCreator,
              AdaptiveBitmapIndexWriterCreator* adaptiveCreator)
    {
        NormalIndexMerger::Init(indexConfig, hint, taskResources, ioConfig, truncateCreator, adaptiveCreator);
        mParentIndexName = parentIndexName;
    }

    void PrepareIndexOutputSegmentResource(const index::MergerResource& resource,
                                           const index_base::SegmentMergeInfos& segMergeInfos,
                                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

private:
    std::string mParentIndexName;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeLevelIndexMerger);

class RangeIndexMerger : public NormalIndexMerger
{
public:
    DECLARE_INDEX_MERGER_IDENTIFIER(range);
    DECLARE_INDEX_MERGER_CREATOR(RangeIndexMerger, it_range);

public:
    RangeIndexMerger()
    {
        mBottomLevelIndexMerger.reset(new RangeLevelIndexMerger());
        mHighLevelIndexMerger.reset(new RangeLevelIndexMerger());
    }
    ~RangeIndexMerger() {}

public:
    void Init(const config::IndexConfigPtr& indexConfig, const index_base::MergeItemHint& hint,
              const index_base::MergeTaskResourceVector& taskResources, const config::MergeIOConfig& ioConfig,
              TruncateIndexWriterCreator* truncateCreator, AdaptiveBitmapIndexWriterCreator* adaptiveCreator) override;
    void BeginMerge(const SegmentDirectoryBasePtr& segDir) override;
    void Merge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
               const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    void SortByWeightMerge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;
    void SetReadBufferSize(uint32_t bufSize) override;

protected:
    void EndMerge() override { assert(false); }

    OnDiskIndexIteratorCreatorPtr CreateOnDiskIndexIteratorCreator() override
    {
        assert(false);
        return OnDiskIndexIteratorCreatorPtr();
    }
    int64_t GetMaxLengthOfPosting(const index_base::SegmentData& segData) const override;

    void MergeRangeInfo(const index_base::SegmentMergeInfos& segMergeInfos,
                        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    void PrepareIndexOutputSegmentResource(const index::MergerResource& resource,
                                           const index_base::SegmentMergeInfos& segMergeInfos,
                                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

private:
    RangeLevelIndexMergerPtr mBottomLevelIndexMerger;
    RangeLevelIndexMergerPtr mHighLevelIndexMerger;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeIndexMerger);
/////////////////////////////////////////////
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_RANGE_INDEX_MERGER_H
