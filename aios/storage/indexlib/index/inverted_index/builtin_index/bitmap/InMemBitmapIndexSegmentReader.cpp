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
#include "indexlib/index/inverted_index/builtin_index/bitmap/InMemBitmapIndexSegmentReader.h"

#include "indexlib/index/inverted_index/InvertedIndexSearchTracer.h"
#include "indexlib/index/inverted_index/InvertedIndexUtil.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"
#include "indexlib/util/Exception.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InMemBitmapIndexSegmentReader);

InMemBitmapIndexSegmentReader::InMemBitmapIndexSegmentReader(const BitmapPostingTable* postingTable, bool isNumberIndex,
                                                             BitmapPostingWriter** nullPostingWriterPtr)
    : _bitmapPostingTable(postingTable)
    , _nullTermPostingPtr(nullPostingWriterPtr)
    , _isNumberIndex(isNumberIndex)
{
}

InMemBitmapIndexSegmentReader::~InMemBitmapIndexSegmentReader() {}

PostingWriter* InMemBitmapIndexSegmentReader::GetPostingWriter(const index::DictKeyInfo& key) const
{
    if (key.IsNull()) {
        return _nullTermPostingPtr ? *_nullTermPostingPtr : nullptr;
    } else if (_bitmapPostingTable) {
        return _bitmapPostingTable->Find(InvertedIndexUtil::GetRetrievalHashKey(_isNumberIndex, key.GetKey()), nullptr);
    }
    return nullptr;
}

bool InMemBitmapIndexSegmentReader::GetSegmentPosting(const index::DictKeyInfo& key, docid_t baseDocId,
                                                      SegmentPosting& segPosting, autil::mem_pool::Pool* sessionPool,
                                                      indexlib::file_system::ReadOption option,
                                                      InvertedIndexSearchTracer* tracer) const
{
    assert(_bitmapPostingTable);
    if (tracer) {
        tracer->IncDictionaryLookupCount();
    }
    PostingWriter* postingWriter = GetPostingWriter(key);
    if (postingWriter == nullptr) {
        return false;
    }
    if (tracer) {
        tracer->IncDictionaryHitCount();
    }
    segPosting.Init(baseDocId, 0, postingWriter);
    return true;
}

void InMemBitmapIndexSegmentReader::Update(docid_t docId, const index::DictKeyInfo& key, bool isDelete)
{
    PostingWriter* postingWriter = GetPostingWriter(key);
    if (postingWriter == nullptr) {
        return;
    }
    auto bitmapWriter = dynamic_cast<BitmapPostingWriter*>(postingWriter);
    if (!bitmapWriter) {
        AUTIL_LOG(ERROR, "update doc[%d] failed, posting[%s] is not bimap", docId, typeid(*postingWriter).name());
        return;
    }
    bitmapWriter->Update(docId, isDelete);
}

} // namespace indexlib::index
