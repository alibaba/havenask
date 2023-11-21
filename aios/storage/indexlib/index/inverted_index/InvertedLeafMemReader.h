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

#include "autil/Log.h"
#include "autil/SnapshotVector.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/index/inverted_index/format/IndexFormatOption.h"
#include "indexlib/util/HashMap.h"

namespace indexlibv2::index {
class AttributeMemReader;
}
namespace indexlib::index {
class PostingWriter;
class InMemDictionaryReader;
class InMemBitmapIndexSegmentReader;
class DynamicIndexSegmentReader;

class InvertedLeafMemReader : public IndexSegmentReader
{
public:
    using PostingTable = util::HashMap<dictkey_t, PostingWriter*>;
    using HashKeyVector = autil::SnapshotVector<dictkey_t, autil::mem_pool::pool_allocator<dictkey_t>>;

    InvertedLeafMemReader(const PostingTable* postingTable,
                          const std::shared_ptr<indexlibv2::index::AttributeMemReader>& sectionSegmentReader,
                          const std::shared_ptr<InMemBitmapIndexSegmentReader>& bitmapSegmentReader,
                          const std::shared_ptr<DynamicIndexSegmentReader>& dynamicSegmentReader,
                          const IndexFormatOption& indexFormatOption, PostingWriter** nullPostingWriterPtr,
                          HashKeyVector* hashKeyVectorPtr);

    ~InvertedLeafMemReader();

    std::shared_ptr<indexlibv2::index::AttributeMemReader> GetSectionAttributeSegmentReaderV2() const override
    {
        return _sectionSegmentReader;
    }

    std::shared_ptr<InMemBitmapIndexSegmentReader> GetBitmapSegmentReader() const override
    {
        return _bitmapSegmentReader;
    }

    std::shared_ptr<index::DynamicIndexSegmentReader> GetDynamicSegmentReader() const override
    {
        return _dynamicSegmentReader;
    }

    bool GetSegmentPosting(const index::DictKeyInfo& key, docid64_t baseDocId, SegmentPosting& segPosting,
                           autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption option,
                           InvertedIndexSearchTracer* tracer) const override;

    std::shared_ptr<DictionaryReader> GetDictionaryReader() const override;

    // not support yet
    void Update(docid_t docId, const index::DictKeyInfo& key, bool isDelete) override;

private:
    PostingWriter* GetPostingWriter(const index::DictKeyInfo& key) const;

    const PostingTable* _postingTable;
    PostingWriter** _nullTermPostingPtr;
    std::shared_ptr<indexlibv2::index::AttributeMemReader> _sectionSegmentReader;
    std::shared_ptr<InMemBitmapIndexSegmentReader> _bitmapSegmentReader;
    std::shared_ptr<DynamicIndexSegmentReader> _dynamicSegmentReader;
    IndexFormatOption _indexFormatOption;
    HashKeyVector* _hashKeyVectorPtr;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
