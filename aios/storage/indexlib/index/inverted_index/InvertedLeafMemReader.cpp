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
#include "indexlib/index/inverted_index/InvertedLeafMemReader.h"

#include "indexlib/index/attribute/AttributeMemReader.h"
#include "indexlib/index/inverted_index/InvertedIndexSearchTracer.h"
#include "indexlib/index/inverted_index/InvertedIndexUtil.h"
#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/InMemBitmapIndexSegmentReader.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicIndexSegmentReader.h"
#include "indexlib/index/inverted_index/format/dictionary/InMemDictionaryReader.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InvertedLeafMemReader);

InvertedLeafMemReader::InvertedLeafMemReader(
    const PostingTable* postingTable,
    const std::shared_ptr<indexlibv2::index::AttributeMemReader>& sectionSegmentReader,
    const std::shared_ptr<InMemBitmapIndexSegmentReader>& bitmapSegmentReader,
    const std::shared_ptr<DynamicIndexSegmentReader>& dynamicSegmentReader, const IndexFormatOption& indexFormatOption,
    PostingWriter** nullPostingWriterPtr, HashKeyVector* hashKeyVectorPtr)
    : _postingTable(postingTable)
    , _nullTermPostingPtr(nullPostingWriterPtr)
    , _sectionSegmentReader(sectionSegmentReader)
    , _bitmapSegmentReader(bitmapSegmentReader)
    , _dynamicSegmentReader(dynamicSegmentReader)
    , _indexFormatOption(indexFormatOption)
    , _hashKeyVectorPtr(hashKeyVectorPtr)
{
}

InvertedLeafMemReader::~InvertedLeafMemReader() {}

PostingWriter* InvertedLeafMemReader::GetPostingWriter(const index::DictKeyInfo& key) const
{
    if (key.IsNull()) {
        return _nullTermPostingPtr ? *_nullTermPostingPtr : nullptr;
    } else if (_postingTable) {
        return _postingTable->Find(
            InvertedIndexUtil::GetRetrievalHashKey(_indexFormatOption.IsNumberIndex(), key.GetKey()), nullptr);
    }
    return nullptr;
}

bool InvertedLeafMemReader::GetSegmentPosting(const index::DictKeyInfo& key, docid64_t baseDocId,
                                              SegmentPosting& segPosting, autil::mem_pool::Pool* sessionPool,
                                              indexlib::file_system::ReadOption option,
                                              InvertedIndexSearchTracer* tracer) const
{
    assert(_postingTable);
    SegmentPosting inMemSegPosting(_indexFormatOption.GetPostingFormatOption());
    PostingWriter* postingWriter = GetPostingWriter(key);
    if (tracer) {
        tracer->IncDictionaryLookupCount();
    }
    if (postingWriter == nullptr) {
        return false;
    }
    if (tracer) {
        tracer->IncDictionaryHitCount();
    }
    inMemSegPosting.Init(baseDocId, 0, postingWriter);
    segPosting = inMemSegPosting;
    return true;
}

void InvertedLeafMemReader::Update(docid_t docId, const index::DictKeyInfo& key, bool isDelete)
{
    assert(false);
    INDEXLIB_FATAL_ERROR(InconsistentState, "InMemNormalIndexSegmentReader update called");
}

std::shared_ptr<DictionaryReader> InvertedLeafMemReader::GetDictionaryReader() const
{
    return std::make_shared<InMemDictionaryReader>(_hashKeyVectorPtr);
}

} // namespace indexlib::index
