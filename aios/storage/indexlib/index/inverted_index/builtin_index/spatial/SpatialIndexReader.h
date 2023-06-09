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
#include "autil/NoCopyable.h"
#include "indexlib/index/inverted_index/InvertedIndexReaderImpl.h"

namespace indexlibv2::index {
class AttributeReader;

template <typename T>
class MultiValueAttributeReader;
} // namespace indexlibv2::index

namespace indexlib::index {
class DocValueFilter;
class SpatialPostingIterator;
class QueryStrategy;
class Shape;

class SpatialIndexReader : public InvertedIndexReaderImpl
{
public:
    SpatialIndexReader(const std::shared_ptr<InvertedIndexMetrics>& metrics);
    ~SpatialIndexReader();

public:
    void AddAttributeReader(indexlibv2::index::AttributeReader* attrReader);
    Result<PostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize, PostingType type,
                                    autil::mem_pool::Pool* sessionPool) override;

    future_lite::coro::Lazy<Result<PostingIterator*>> LookupAsync(const index::Term* term, uint32_t statePoolSize,
                                                                  PostingType type, autil::mem_pool::Pool* pool,
                                                                  file_system::ReadOption option) noexcept override;

    Result<PostingIterator*> PartialLookup(const index::Term& term, const DocIdRangeVector& ranges,
                                           uint32_t statePoolSize, PostingType type,
                                           autil::mem_pool::Pool* sessionPool) override
    {
        return Lookup(term, statePoolSize, type, sessionPool);
    }

protected:
    DocValueFilter* CreateDocValueFilter(const std::shared_ptr<Shape>& shape, autil::mem_pool::Pool* sessionPool);
    Status DoOpen(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                  const std::vector<InvertedIndexReaderImpl::Indexer>& indexers) override;

    Result<PostingIterator*> CreateSpatialPostingIterator(const std::vector<BufferedPostingIterator*>& postingIterators,
                                                          autil::mem_pool::Pool* sessionPool);
    std::shared_ptr<Shape> ParseShape(const std::string& shapeStr) const;

private:
    int8_t _topLevel;
    int8_t _detailLevel;
    using LocationAttributeReader = indexlibv2::index::MultiValueAttributeReader<double>;
    LocationAttributeReader* _attrReader = nullptr;
    std::shared_ptr<QueryStrategy> _queryStrategy = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
