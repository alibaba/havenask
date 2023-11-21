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

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/inverted_index/format/PostingDecoderImpl.h"
#include "indexlib/index/inverted_index/format/PostingFormat.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_segment_posting_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"

namespace indexlib { namespace index {
struct PostingWriterResource;
}} // namespace indexlib::index

namespace indexlib { namespace index { namespace legacy {
class PostingMergerImpl : public index::PostingMerger
{
public:
    PostingMergerImpl(PostingWriterResource* postingWriterResource,
                      const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos);

    ~PostingMergerImpl();

public:
    void Merge(const index::SegmentTermInfos& segTermInfos, const index::ReclaimMapPtr& reclaimMap) override;

    void SortByWeightMerge(const index::SegmentTermInfos& segTermInfos,
                           const index::ReclaimMapPtr& reclaimMap) override;

    void Dump(const index::DictKeyInfo& key, IndexOutputSegmentResources& indexOutputSegmentResources) override;

    uint64_t GetPostingLength() const override;

    ttf_t GetTotalTF() override
    {
        if (mttf == -1) {
            mttf = mPostingWriter->GetTotalTF();
        }
        return mttf;
    }

    df_t GetDocFreq() override
    {
        if (mDf == -1) {
            mDf = mPostingWriter->GetDF();
        }
        return mDf;
    }

    bool IsCompressedPostingHeader() const override { return true; }

private:
    void MergeOneSegment(const index::ReclaimMapPtr& reclaimMap, PostingDecoderImpl* decoder,
                         SingleTermIndexSegmentPatchIterator* patchIter, docid_t baseDocId);

    MultiSegmentPostingWriterPtr
    CreatePostingWriter(PostingWriterResource* postingWriterResource,
                        const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos);

    std::shared_ptr<PostingIterator> CreatePostingIterator() override
    {
        return mPostingWriter->CreatePostingIterator(mTermPayload);
    }

private:
    PostingFormatOption mPostingFormatOption;
    MultiSegmentPostingWriterPtr mPostingWriter;
    df_t mDf;
    ttf_t mttf;

private:
    friend class PostingMergerImplTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PostingMergerImpl);
}}} // namespace indexlib::index::legacy
