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

#include "autil/mem_pool/RecyclePool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/dynamic_index_posting_writer.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/HashMap.h"

namespace indexlib { namespace index {

class InMemDynamicIndexSegmentReader : public index::IndexSegmentReader
{
public:
    typedef util::HashMap<dictkey_t, PostingWriter*, autil::mem_pool::RecyclePool> PostingTable;

public:
    InMemDynamicIndexSegmentReader(const PostingTable* postingTable, bool isNumberIndex,
                                   DynamicIndexPostingWriter** nullPostingWriterPtr);

    ~InMemDynamicIndexSegmentReader();

public:
    bool GetSegmentPosting(const index::DictKeyInfo& key, docid_t baseDocId, SegmentPosting& segPosting,
                           autil::mem_pool::Pool* sessionPool,
                           InvertedIndexSearchTracer* tracer = nullptr) const override;

    void Update(docid_t docId, const index::DictKeyInfo& key, bool isDelete) override;

private:
    PostingWriter* GetPostingWriter(const index::DictKeyInfo& key) const;

private:
    const PostingTable* _dynamicPostingTable;
    DynamicIndexPostingWriter** _nullTermPostingPtr;
    bool _isNumberIndex;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemDynamicIndexSegmentReader);
}} // namespace indexlib::index
