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
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/in_mem_dynamic_index_segment_reader.h"

#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/normal/framework/index_writer.h"

namespace indexlib { namespace index {
IE_LOG_SETUP(index, InMemDynamicIndexSegmentReader);

InMemDynamicIndexSegmentReader::InMemDynamicIndexSegmentReader(
    const InMemDynamicIndexSegmentReader::PostingTable* postingTable, bool isNumberIndex,
    DynamicIndexPostingWriter** nullPostingWriterPtr)
    : _dynamicPostingTable(postingTable)
    , _nullTermPostingPtr(nullPostingWriterPtr)
    , _isNumberIndex(isNumberIndex)
{
}

InMemDynamicIndexSegmentReader::~InMemDynamicIndexSegmentReader() {}

PostingWriter* InMemDynamicIndexSegmentReader::GetPostingWriter(const index::DictKeyInfo& key) const
{
    if (key.IsNull()) {
        return _nullTermPostingPtr ? *_nullTermPostingPtr : nullptr;
    } else if (_dynamicPostingTable) {
        return _dynamicPostingTable->Find(InvertedIndexUtil::GetRetrievalHashKey(_isNumberIndex, key.GetKey()),
                                          nullptr);
    }
    return nullptr;
}

bool InMemDynamicIndexSegmentReader::GetSegmentPosting(const index::DictKeyInfo& key, docid_t baseDocId,
                                                       SegmentPosting& segPosting, autil::mem_pool::Pool* sessionPool,
                                                       indexlib::index::InvertedIndexSearchTracer* tracer) const
{
    assert(_dynamicPostingTable);
    PostingWriter* postingWriter = GetPostingWriter(key);
    if (postingWriter == nullptr) {
        return false;
    }
    segPosting.Init(baseDocId, 0, postingWriter);
    return true;
}

void InMemDynamicIndexSegmentReader::Update(docid_t docId, const index::DictKeyInfo& key, bool isDelete)
{
    PostingWriter* postingWriter = GetPostingWriter(key);
    if (postingWriter == nullptr) {
        return;
    }
    auto dynamicWriter = dynamic_cast<DynamicIndexPostingWriter*>(postingWriter);
    if (!dynamicWriter) {
        IE_LOG(ERROR, "update doc[%d] failed, posting[%s] is not dynamic", docId, typeid(*postingWriter).name());
        return;
    }
    dynamicWriter->Update(docId, isDelete ? document::ModifiedTokens::Operation::REMOVE
                                          : document::ModifiedTokens::Operation::ADD);
}
}} // namespace indexlib::index
