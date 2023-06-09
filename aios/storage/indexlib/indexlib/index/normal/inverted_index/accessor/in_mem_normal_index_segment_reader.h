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
#ifndef __INDEXLIB_IN_MEM_NORMAL_INDEX_SEGMENT_READER_H
#define __INDEXLIB_IN_MEM_NORMAL_INDEX_SEGMENT_READER_H

#include <memory>

#include "autil/SnapshotVector.h"
#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/index/inverted_index/format/dictionary/InMemDictionaryReader.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/util/HashMap.h"

namespace indexlib { namespace index {
class PostingWriter;
class InMemDynamicIndexSegmentReader;
class InMemNormalIndexSegmentReader : public IndexSegmentReader
{
public:
    typedef util::HashMap<dictkey_t, PostingWriter*> PostingTable;
    typedef autil::SnapshotVector<dictkey_t, autil::mem_pool::pool_allocator<dictkey_t>> HashKeyVector;

    InMemNormalIndexSegmentReader(const PostingTable* postingTable,
                                  const std::shared_ptr<index::AttributeSegmentReader>& sectionSegmentReader,
                                  const std::shared_ptr<index::InMemBitmapIndexSegmentReader>& bitmapSegmentReader,
                                  const std::shared_ptr<index::InMemDynamicIndexSegmentReader>& dynamicSegmentReader,
                                  const LegacyIndexFormatOption& indexFormatOption,
                                  PostingWriter** nullPostingWriterPtr, HashKeyVector* hashKeyVectorPtr);

    ~InMemNormalIndexSegmentReader();

    std::shared_ptr<index::AttributeSegmentReader> GetSectionAttributeSegmentReader() const override
    {
        return mSectionSegmentReader;
    }

    std::shared_ptr<index::InMemBitmapIndexSegmentReader> GetBitmapSegmentReader() const override
    {
        return mBitmapSegmentReader;
    }

    std::shared_ptr<index::InMemDynamicIndexSegmentReader> GetInMemDynamicSegmentReader() const override
    {
        return mDynamicSegmentReader;
    }

    bool GetSegmentPosting(const index::DictKeyInfo& key, docid_t baseDocId, SegmentPosting& segPosting,
                           autil::mem_pool::Pool* sessionPool,
                           InvertedIndexSearchTracer* tracer = nullptr) const override;

    std::shared_ptr<DictionaryReader> GetDictionaryReader() const override
    {
        std::shared_ptr<DictionaryReader> dictReader(new InMemDictionaryReader(mHashKeyVectorPtr));
        return dictReader;
    }

    void Update(docid_t docId, const index::DictKeyInfo& key, bool isDelete) override;

private:
    PostingWriter* GetPostingWriter(const index::DictKeyInfo& key) const;

    const PostingTable* mPostingTable;
    PostingWriter** mNullTermPostingPtr;
    std::shared_ptr<index::AttributeSegmentReader> mSectionSegmentReader;
    std::shared_ptr<index::InMemBitmapIndexSegmentReader> mBitmapSegmentReader;
    std::shared_ptr<index::InMemDynamicIndexSegmentReader> mDynamicSegmentReader;
    LegacyIndexFormatOption mIndexFormatOption;
    HashKeyVector* mHashKeyVectorPtr;

    friend class InMemNormalIndexSegmentReaderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemNormalIndexSegmentReader);
}} // namespace indexlib::index

#endif //__INDEXLIB_IN_MEM_NORMAL_INDEX_SEGMENT_READER_H
