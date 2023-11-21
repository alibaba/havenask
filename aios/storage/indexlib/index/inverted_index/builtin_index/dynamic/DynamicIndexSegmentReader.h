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
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicPostingWriter.h"
#include "indexlib/util/HashMap.h"

namespace indexlib::index {

class DynamicIndexSegmentReader : public IndexSegmentReader
{
    typedef util::HashMap<dictkey_t, PostingWriter*, autil::mem_pool::RecyclePool> PostingTable;

public:
    DynamicIndexSegmentReader(const PostingTable* postingTable, bool isNumberIndex,
                              DynamicPostingWriter** nullPostingWriter);

    ~DynamicIndexSegmentReader();

public:
    bool GetSegmentPosting(const index::DictKeyInfo& key, docid64_t baseDocId, SegmentPosting& segPosting,
                           autil::mem_pool::Pool* sessionPool,
                           indexlib::file_system::ReadOption option = indexlib::file_system::ReadOption(),
                           InvertedIndexSearchTracer* tracer = nullptr) const override;

    void Update(docid_t docId, const index::DictKeyInfo& key, bool isDelete) override;

private:
    PostingWriter* GetPostingWriter(const index::DictKeyInfo& key) const;

private:
    const PostingTable* _dynamicPostingTable;
    DynamicPostingWriter** _nullTermPosting;
    bool _isNumberIndex;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
