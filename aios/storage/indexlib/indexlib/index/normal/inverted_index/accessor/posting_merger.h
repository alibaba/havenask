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
#ifndef __INDEXLIB_POSTING_MERGER_H
#define __INDEXLIB_POSTING_MERGER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/IndexOutputSegmentResource.h"
#include "indexlib/index/inverted_index/SegmentTermInfo.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, PostingIterator);
DECLARE_REFERENCE_CLASS(index, SegmentTermInfo);

namespace indexlib { namespace index {

class PostingMerger
{
public:
    PostingMerger() : mTermPayload(0) {};
    virtual ~PostingMerger() {};

public:
    virtual void Merge(const SegmentTermInfos& segTermInfos, const ReclaimMapPtr& reclaimMap) = 0;
    virtual void SortByWeightMerge(const SegmentTermInfos& segTermInfos, const ReclaimMapPtr& reclaimMap) = 0;
    virtual void Dump(const index::DictKeyInfo& key, IndexOutputSegmentResources& indexOutputSegmentResources) = 0;

    virtual df_t GetDocFreq() = 0;
    virtual ttf_t GetTotalTF() = 0;

    //    virtual uint64_t GetDumpLength() const { return 0; }
    virtual uint64_t GetPostingLength() const = 0;
    virtual bool GetDictInlinePostingValue(uint64_t& inlinePostingValue) { return false; }

    virtual bool IsCompressedPostingHeader() const = 0;
    termpayload_t GetTermPayload() const { return mTermPayload; }

    virtual std::shared_ptr<PostingIterator> CreatePostingIterator() = 0;

protected:
    termpayload_t mTermPayload;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PostingMerger);
}} // namespace indexlib::index

#endif //__INDEXLIB_POSTING_MERGER_H
