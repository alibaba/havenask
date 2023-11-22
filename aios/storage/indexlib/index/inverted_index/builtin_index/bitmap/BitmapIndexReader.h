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
#include "autil/mem_pool/Pool.h"
#include "indexlib/index/common/DictHasher.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/format/IndexFormatOption.h"
#include "indexlib/util/PooledUniquePtr.h"

namespace indexlibv2::config {
class InvertedIndexConfig;
}

namespace indexlib::index {
class BuildingIndexReader;
class BitmapLeafReader;
class BitmapPostingIterator;
class IndexSegmentReader;
class PostingIterator;
class InMemBitmapIndexSegmentReader;

class BitmapIndexReader
{
public:
    BitmapIndexReader();
    virtual ~BitmapIndexReader();

    Status
    Open(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
         const std::vector<std::pair<docid64_t, std::shared_ptr<BitmapLeafReader>>>& diskSegmentReaders,
         const std::vector<std::pair<docid64_t, std::shared_ptr<InMemBitmapIndexSegmentReader>>>& memSegmentReaders);

    index::Result<PostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize,
                                           autil::mem_pool::Pool* sessionPool,
                                           file_system::ReadOption option) const noexcept;

    index::Result<PostingIterator*> PartialLookup(const index::Term& term, const DocIdRangeVector& ranges,
                                                  uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool,
                                                  file_system::ReadOption) const noexcept;

    virtual BitmapPostingIterator*
    CreateBitmapPostingIterator(autil::mem_pool::Pool* sessionPool,
                                util::PooledUniquePtr<InvertedIndexSearchTracer> tracer) const;
    virtual index::Result<bool> GetSegmentPosting(const index::DictKeyInfo& key, uint32_t segmentIdx,
                                                  SegmentPosting& segPosting, file_system::ReadOption option,
                                                  InvertedIndexSearchTracer* tracer) const noexcept;

private:
    index::Result<PostingIterator*> Lookup(const index::DictKeyInfo& key, const DocIdRangeVector& ranges,
                                           uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool,
                                           file_system::ReadOption option) const noexcept;

    index::ErrorCode FillSegmentPostingVector(const index::DictKeyInfo& key, const DocIdRangeVector& ranges,
                                              autil::mem_pool::Pool* sessionPool,
                                              PostingFormatOption bitmapPostingFormatOption,
                                              std::shared_ptr<SegmentPostingVector>& segPostings,
                                              file_system::ReadOption option,
                                              InvertedIndexSearchTracer* tracer) const noexcept;

    void AddInMemSegmentReader(docid64_t baseDocId,
                               const std::shared_ptr<InMemBitmapIndexSegmentReader>& bitmapSegReader);

private:
    IndexDictHasher _dictHasher;
    std::vector<std::shared_ptr<BitmapLeafReader>> _segmentReaders;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    IndexFormatOption _indexFormatOption;
    std::vector<uint64_t> _segmentDocCount;
    std::vector<docid64_t> _baseDocIds;
    std::shared_ptr<BuildingIndexReader> _buildingIndexReader;

    AUTIL_LOG_DECLARE();
};

inline index::Result<PostingIterator*> BitmapIndexReader::Lookup(const index::Term& term, uint32_t statePoolSize,
                                                                 autil::mem_pool::Pool* sessionPool,
                                                                 file_system::ReadOption option) const noexcept
{
    index::DictKeyInfo key;
    if (!_dictHasher.GetHashKey(term, key)) {
        AUTIL_LOG(WARN, "invalid term [%s], index name [%s]", term.GetWord().c_str(),
                  _indexConfig->GetIndexName().c_str());
        return nullptr;
    }
    return Lookup(key, {}, statePoolSize, sessionPool, option);
}

inline index::Result<PostingIterator*>
BitmapIndexReader::PartialLookup(const index::Term& term, const DocIdRangeVector& ranges, uint32_t statePoolSize,
                                 autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) const noexcept
{
    index::DictKeyInfo key;
    if (!_dictHasher.GetHashKey(term, key)) {
        AUTIL_LOG(WARN, "invalid term [%s], index name [%s]", term.GetWord().c_str(),
                  _indexConfig->GetIndexName().c_str());
        return nullptr;
    }
    return Lookup(key, ranges, statePoolSize, sessionPool, option);
}

} // namespace indexlib::index
