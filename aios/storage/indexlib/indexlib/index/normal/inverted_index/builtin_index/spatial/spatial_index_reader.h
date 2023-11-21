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

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, Shape);
DECLARE_REFERENCE_CLASS(index, QueryStrategy);
DECLARE_REFERENCE_CLASS(index, AttributeReader);

namespace indexlib { namespace index {
class SpatialPostingIterator;
class DocValueFilter;
template <typename T>
class VarNumAttributeReader;
typedef VarNumAttributeReader<double> LocationAttributeReader;
DEFINE_SHARED_PTR(LocationAttributeReader);
}} // namespace indexlib::index

namespace indexlib { namespace index { namespace legacy {

// TODO: inherit from InvertedIndexReader
class SpatialIndexReader : public NormalIndexReader
{
public:
    SpatialIndexReader();
    ~SpatialIndexReader();

public:
    void Open(const config::IndexConfigPtr& indexConfig, const index_base::PartitionDataPtr& partitionData,
              const InvertedIndexReader* hintReader = nullptr) override;
    index::Result<PostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize = 1000,
                                           PostingType type = pt_default,
                                           autil::mem_pool::Pool* sessionPool = NULL) override;

    future_lite::coro::Lazy<index::Result<PostingIterator*>>
    LookupAsync(const index::Term* term, uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool* pool,
                file_system::ReadOption option) noexcept override;

    void AddAttributeReader(const AttributeReaderPtr& attrReader);

    index::Result<PostingIterator*> PartialLookup(const index::Term& term, const DocIdRangeVector& ranges,
                                                  uint32_t statePoolSize, PostingType type,
                                                  autil::mem_pool::Pool* sessionPool) override
    {
        return Lookup(term, statePoolSize, type, sessionPool);
    }

private:
    SpatialPostingIterator* CreateSpatialPostingIterator(std::vector<BufferedPostingIterator*>& postingIterators,
                                                         autil::mem_pool::Pool* sessionPool);
    std::shared_ptr<Shape> ParseShape(const std::string& shapeStr) const;

    DocValueFilter* CreateDocValueFilter(const std::shared_ptr<Shape>& shape, autil::mem_pool::Pool* sessionPool);

private:
    int8_t mTopLevel;
    int8_t mDetailLevel;
    LocationAttributeReaderPtr mAttrReader;
    std::shared_ptr<QueryStrategy> mQueryStrategy;

private:
    friend class SpatialIndexReaderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SpatialIndexReader);
}}} // namespace indexlib::index::legacy
