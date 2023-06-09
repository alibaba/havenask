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
#include "indexlib/index/common/field_format/range/RangeFieldEncoder.h"
#include "indexlib/index/inverted_index/InvertedIndexReaderImpl.h"
#include "indexlib/index/inverted_index/SegmentPostings.h"

namespace indexlibv2::index {
class AttributeReader;
}

namespace indexlib::index {
class DocValueFilter;
class RangeIndexReader : public InvertedIndexReaderImpl
{
public:
    RangeIndexReader(const std::shared_ptr<InvertedIndexMetrics>& metrics);
    ~RangeIndexReader();

    void AddAttributeReader(indexlibv2::index::AttributeReader* attrReader) { _attrReader = attrReader; }
    index::Result<PostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize = 1000,
                                           PostingType type = pt_default,
                                           autil::mem_pool::Pool* sessionPool = nullptr) override;

    future_lite::coro::Lazy<index::Result<PostingIterator*>>
    LookupAsync(const index::Term* term, uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool* pool,
                file_system::ReadOption option) noexcept override;

    index::Result<PostingIterator*> PartialLookup(const index::Term& term, const DocIdRangeVector& ranges,
                                                  uint32_t statePoolSize, PostingType type,
                                                  autil::mem_pool::Pool* sessionPool) override
    {
        return Lookup(term, statePoolSize, type, sessionPool);
    }

protected:
    std::shared_ptr<BuildingIndexReader> CreateBuildingIndexReader() const override;
    DocValueFilter* CreateDocValueFilter(const index::Term& term, autil::mem_pool::Pool* sessionPool);
    Status DoOpen(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                  const std::vector<InvertedIndexReaderImpl::Indexer>& indexers) override;

private:
    future_lite::coro::Lazy<index::Result<SegmentPostingsVec>>
    GetSegmentPostings(uint64_t leftTerm, uint64_t rightTerm, autil::mem_pool::Pool* sessionPool,
                       const DocIdRangeVector& ranges, file_system::ReadOption option,
                       InvertedIndexSearchTracer* tracer) const noexcept;

private:
    RangeFieldEncoder::RangeFieldType _rangeFieldType;
    indexlibv2::index::AttributeReader* _attrReader = nullptr;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
