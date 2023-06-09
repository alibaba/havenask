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

#include "future_lite/coro/Lazy.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/format/IndexFormatOption.h"

namespace autil::mem_pool {
class Pool;
}
namespace indexlib::util {
class BuildResourceMetrics;
}
namespace indexlib::config {
class IndexConfig;
}

namespace indexlib { namespace index {
class IndexAccessoryReader;
class LegacyIndexAccessoryReader;
class IndexUpdateTermIterator;
class KeyIterator;
class SegmentPosting;
class IndexSegmentReader;
class PostingIterator;
class SectionAttributeReader;
class InvertedIndexSearchTracer;

class InvertedIndexReader : public indexlibv2::index::IIndexReader
{
public:
    InvertedIndexReader() : _indexFormatOption(std::make_shared<IndexFormatOption>()) {};
    virtual ~InvertedIndexReader() = default;

public:
    constexpr static uint32_t DEFAULT_STATE_POOL_SIZE = 1000;

public:
    virtual index::Result<PostingIterator*> Lookup(const index::Term& term,
                                                   uint32_t inDocPositionStatePoolSize = DEFAULT_STATE_POOL_SIZE,
                                                   PostingType type = pt_default,
                                                   autil::mem_pool::Pool* sessionPool = nullptr) = 0;
    // pool in LookupAsync and BatchLookup should be thread-safe
    virtual future_lite::coro::Lazy<index::Result<PostingIterator*>>
    LookupAsync(const index::Term* term, uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool* pool,
                file_system::ReadOption option) noexcept = 0;

    // ranges must be sorted and do not intersect
    virtual index::Result<PostingIterator*> PartialLookup(const index::Term& term, const DocIdRangeVector& ranges,
                                                          uint32_t statePoolSize, PostingType type,
                                                          autil::mem_pool::Pool* pool);

    virtual const SectionAttributeReader* GetSectionReader(const std::string& indexName) const = 0;
    virtual std::shared_ptr<KeyIterator> CreateKeyIterator(const std::string& indexName) = 0;

    virtual void SetAccessoryReader(const std::shared_ptr<IndexAccessoryReader>& accessorReader) = 0;

    virtual bool GetSegmentPosting(const index::DictKeyInfo& key, uint32_t segmentIdx, SegmentPosting& segPosting,
                                   file_system::ReadOption option, InvertedIndexSearchTracer* tracer = nullptr) = 0;
    virtual future_lite::coro::Lazy<index::Result<bool>>
    GetSegmentPostingAsync(const index::DictKeyInfo& key, uint32_t segmentIdx, SegmentPosting& segPosting,
                           file_system::ReadOption option, InvertedIndexSearchTracer* tracer) noexcept = 0;

    // to convert field names to fieldmap_t
    virtual bool GenFieldMapMask(const std::string& indexName, const std::vector<std::string>& termFieldNames,
                                 fieldmap_t& targetFieldMap);

public:
    std::string GetIndexName() const
    {
        assert(_indexConfig);
        return _indexConfig->GetIndexName();
    }
    InvertedIndexType GetInvertedIndexType() const
    {
        assert(_indexConfig);
        return _indexConfig->GetInvertedIndexType();
    }
    bool HasTermPayload() const { return _indexFormatOption->HasTermPayload(); }
    bool HasDocPayload() const { return _indexFormatOption->HasDocPayload(); }
    bool HasPositionPayload() const { return _indexFormatOption->HasPositionPayload(); }

protected:
    std::shared_ptr<IndexFormatOption> _indexFormatOption;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;

private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////

inline index::Result<PostingIterator*> InvertedIndexReader::PartialLookup(const index::Term& term,
                                                                          const DocIdRangeVector& ranges,
                                                                          uint32_t statePoolSize, PostingType type,
                                                                          autil::mem_pool::Pool* pool)
{
    return Lookup(term, statePoolSize, type, pool);
}

}} // namespace indexlib::index
