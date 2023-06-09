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
#ifndef __INDEXLIB_BITMAP_POSTING_MERGER_H
#define __INDEXLIB_BITMAP_POSTING_MERGER_H

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/SegmentTermInfo.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/multi_segment_bitmap_posting_writer.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

class BitmapPostingMerger : public PostingMerger
{
public:
    BitmapPostingMerger(autil::mem_pool::Pool* pool, const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos,
                        optionflag_t optionFlag);
    ~BitmapPostingMerger();

public:
    void Merge(const SegmentTermInfos& segTermInfos, const ReclaimMapPtr& reclaimMap) override;

    void SortByWeightMerge(const SegmentTermInfos& segTermInfos, const ReclaimMapPtr& reclaimMap) override;

    void Dump(const index::DictKeyInfo& key, IndexOutputSegmentResources& indexOutputSegmentResources) override;

    df_t GetDocFreq() override
    {
        if (mDf == -1) {
            mDf = mWriter.GetDF();
        }
        return mDf;
    }

    ttf_t GetTotalTF() override { return 0; }

    uint64_t GetPostingLength() const override { return mWriter.GetDumpLength(); }

    bool IsCompressedPostingHeader() const override { return false; }

    std::shared_ptr<PostingIterator> CreatePostingIterator() override
    {
        return mWriter.CreatePostingIterator(mOptionFlag, mTermPayload);
    }

private:
    void ApplyPatch(const SegmentTermInfo* segTermInfo, const ReclaimMapPtr& reclaimMap);

private:
    static const uint32_t DEFAULT_BUFFER_SIZE = 1024 * 8;
    autil::mem_pool::Pool* mPool;
    MultiSegmentBitmapPostingWriter mWriter;
    df_t mDf;
    optionflag_t mOptionFlag;

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BitmapPostingMerger);
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_BITMAP_POSTING_MERGER_H
