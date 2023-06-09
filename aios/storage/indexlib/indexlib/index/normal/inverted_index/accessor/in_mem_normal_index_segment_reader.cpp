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
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"

#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/InMemBitmapIndexSegmentReader.h"
#include "indexlib/index/inverted_index/format/InMemPostingDecoder.h"
#include "indexlib/index/normal/framework/index_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/dynamic_index_posting_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/in_mem_dynamic_index_segment_reader.h"

using namespace std;
using namespace autil::mem_pool;

using namespace indexlib::index;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, InMemNormalIndexSegmentReader);

InMemNormalIndexSegmentReader::InMemNormalIndexSegmentReader(
    const PostingTable* postingTable, const std::shared_ptr<AttributeSegmentReader>& sectionSegmentReader,
    const std::shared_ptr<InMemBitmapIndexSegmentReader>& bitmapSegmentReader,
    const std::shared_ptr<InMemDynamicIndexSegmentReader>& dynamicSegmentReader,
    const LegacyIndexFormatOption& indexFormatOption, PostingWriter** nullPostingWriterPtr,
    HashKeyVector* hashKeyVectorPtr)
    : mPostingTable(postingTable)
    , mNullTermPostingPtr(nullPostingWriterPtr)
    , mSectionSegmentReader(sectionSegmentReader)
    , mBitmapSegmentReader(bitmapSegmentReader)
    , mDynamicSegmentReader(dynamicSegmentReader)
    , mIndexFormatOption(indexFormatOption)
    , mHashKeyVectorPtr(hashKeyVectorPtr)
{
}

InMemNormalIndexSegmentReader::~InMemNormalIndexSegmentReader() {}

PostingWriter* InMemNormalIndexSegmentReader::GetPostingWriter(const index::DictKeyInfo& key) const
{
    if (key.IsNull()) {
        return mNullTermPostingPtr ? *mNullTermPostingPtr : nullptr;
    } else if (mPostingTable) {
        return mPostingTable->Find(
            InvertedIndexUtil::GetRetrievalHashKey(mIndexFormatOption.IsNumberIndex(), key.GetKey()), nullptr);
    }
    return nullptr;
}

bool InMemNormalIndexSegmentReader::GetSegmentPosting(const index::DictKeyInfo& key, docid_t baseDocId,
                                                      SegmentPosting& segPosting, Pool* sessionPool,
                                                      InvertedIndexSearchTracer* tracer) const
{
    assert(mPostingTable);
    SegmentPosting inMemSegPosting(mIndexFormatOption.GetPostingFormatOption());
    PostingWriter* postingWriter = GetPostingWriter(key);
    if (postingWriter == nullptr) {
        return false;
    }
    inMemSegPosting.Init(baseDocId, 0, postingWriter);
    segPosting = inMemSegPosting;
    return true;
}

void InMemNormalIndexSegmentReader::Update(docid_t docId, const index::DictKeyInfo& key, bool isDelete)
{
    assert(false);
    INDEXLIB_FATAL_ERROR(InconsistentState, "InMemNormalIndexSegmentReader update called");
}
}} // namespace indexlib::index
