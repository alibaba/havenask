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

#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/util/HashMap.h"

namespace indexlib::index {
class BitmapPostingWriter;
class PostingWriter;

class InMemBitmapIndexSegmentReader : public index::IndexSegmentReader
{
public:
    using BitmapPostingTable = util::HashMap<uint64_t, BitmapPostingWriter*>;

    InMemBitmapIndexSegmentReader(const BitmapPostingTable* postingTable, bool isNumberIndex,
                                  BitmapPostingWriter** nullPostingWriterPtr = nullptr);
    ~InMemBitmapIndexSegmentReader();

    bool GetSegmentPosting(const index::DictKeyInfo& key, docid64_t baseDocId, SegmentPosting& segPosting,
                           autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption option,
                           InvertedIndexSearchTracer* tracer) const override;

    void Update(docid_t docId, const index::DictKeyInfo& key, bool isDelete) override;

private:
    PostingWriter* GetPostingWriter(const index::DictKeyInfo& key) const;

    const BitmapPostingTable* _bitmapPostingTable;
    BitmapPostingWriter** _nullTermPostingPtr;
    bool _isNumberIndex;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
