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
#include "indexlib/index/inverted_index/builtin_index/spatial/SpatialIndexReader.h"

#include "autil/memory.h"
#include "indexlib/index/attribute/AttributeReader.h"
#include "indexlib/index/common/field_format/spatial/LocationIndexQueryStrategy.h"
#include "indexlib/index/common/field_format/spatial/QueryStrategy.h"
#include "indexlib/index/common/field_format/spatial/ShapeIndexQueryStrategy.h"
#include "indexlib/index/common/field_format/spatial/geo_hash/GeoHashUtil.h"
#include "indexlib/index/common/field_format/spatial/shape/Shape.h"
#include "indexlib/index/common/field_format/spatial/shape/ShapeCreator.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/SeekAndFilterIterator.h"
#include "indexlib/index/inverted_index/builtin_index/spatial/SpatialDocValueFilter.h"
#include "indexlib/index/inverted_index/builtin_index/spatial/SpatialPostingIterator.h"
#include "indexlib/index/inverted_index/config/SpatialIndexConfig.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SpatialIndexReader);

SpatialIndexReader::SpatialIndexReader(const std::shared_ptr<InvertedIndexMetrics>& metrics)
    : InvertedIndexReaderImpl(metrics)
{
}

SpatialIndexReader::~SpatialIndexReader() {}

Status SpatialIndexReader::DoOpen(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                  const std::vector<InvertedIndexReaderImpl::Indexer>& indexers)
{
    auto status = InvertedIndexReaderImpl::DoOpen(indexConfig, indexers);
    RETURN_IF_STATUS_ERROR(status, "spatial index reader [%s] open failed", indexConfig->GetIndexName().c_str());
    auto spatialIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::SpatialIndexConfig>(indexConfig);
    _topLevel = GeoHashUtil::DistanceToLevel(spatialIndexConfig->GetMaxSearchDist());
    _detailLevel = GeoHashUtil::DistanceToLevel(spatialIndexConfig->GetMaxDistError());
    FieldType type = spatialIndexConfig->GetFieldConfig()->GetFieldType();
    if (type == ft_location) {
        _queryStrategy.reset(new LocationIndexQueryStrategy(_topLevel, _detailLevel, indexConfig->GetIndexName(),
                                                            spatialIndexConfig->GetDistanceLoss(), 3000));
    } else {
        _queryStrategy.reset(new ShapeIndexQueryStrategy(_topLevel, _detailLevel, indexConfig->GetIndexName(),
                                                         spatialIndexConfig->GetDistanceLoss(), 3000));
    }
    return Status::OK();
}

Result<PostingIterator*>
SpatialIndexReader::CreateSpatialPostingIterator(const std::vector<BufferedPostingIterator*>& postingIterators,
                                                 autil::mem_pool::Pool* sessionPool)
{
    if (postingIterators.empty()) {
        return nullptr;
    }

    auto iter = autil::unique_ptr_pool_deallocated<autil::mem_pool::Pool, SpatialPostingIterator>(
        sessionPool, IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, SpatialPostingIterator, sessionPool));
    auto ec = iter->Init(postingIterators);
    if (ec != ErrorCode::OK) {
        return ec;
    }
    return iter.release();
}

void SpatialIndexReader::AddAttributeReader(indexlibv2::index::AttributeReader* attrReader)
{
    auto spatialIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::SpatialIndexConfig>(_indexConfig);
    assert(spatialIndexConfig);
    FieldType type = spatialIndexConfig->GetFieldConfig()->GetFieldType();
    if (type != ft_location) {
        // TODO: line polygon support filter
        return;
    }

    _attrReader = dynamic_cast<LocationAttributeReader*>(attrReader);
    if (!_attrReader) {
        AUTIL_LOG(WARN, "spatial index reader should use with location attribute reader");
    }
}
Result<PostingIterator*> SpatialIndexReader::Lookup(const Term& term, uint32_t statePoolSize, PostingType type,
                                                    autil::mem_pool::Pool* sessionPool)
{
    return future_lite::coro::syncAwait(LookupAsync(&term, statePoolSize, type, sessionPool, /*option*/ nullptr));
}

future_lite::coro::Lazy<Result<PostingIterator*>>
SpatialIndexReader::LookupAsync(const index::Term* term, uint32_t statePoolSize, PostingType type,
                                autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) noexcept
{
    auto shape = ParseShape(term->GetWord());
    if (!shape) {
        co_return nullptr;
    }
    std::vector<dictkey_t> terms = _queryStrategy->CalculateTerms(shape);

    DocIdRangeVector ranges;
    if (term->HasHintValues()) {
        ranges = MergeDocIdRanges(term->GetHintValues(), ranges);
    }

    ValidatePartitonRange(ranges);
    std::vector<BufferedPostingIterator*> postingIterators;
    Term defaultTerm;
    defaultTerm.SetNull(false);
    std::vector<future_lite::coro::Lazy<Result<PostingIterator*>>> tasks;
    tasks.reserve(terms.size());
    for (const auto& term : terms) {
        tasks.push_back(CreatePostingIteratorByHashKey(&defaultTerm, index::DictKeyInfo(term), ranges, statePoolSize,
                                                       sessionPool, option));
    }

    auto results = co_await future_lite::coro::collectAll(std::move(tasks));
    assert(terms.size() == results.size());
    for (const auto& result : results) {
        assert(!result.hasError());
        if (result.value().Ok()) {
            if (auto postingIterator = result.value().Value()) {
                postingIterators.push_back((BufferedPostingIterator*)postingIterator);
            }
        } else {
            for (auto postingIterator : postingIterators) {
                IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, postingIterator);
            }
            co_return result.value().GetErrorCode();
        }
    }
    // TODO: if postingIterators.size() == 1 return it self
    auto iterWithResult = CreateSpatialPostingIterator(postingIterators, sessionPool);
    if (!iterWithResult.Ok()) {
        co_return iterWithResult.GetErrorCode();
    }
    auto iter = iterWithResult.Value();
    if (!iter) {
        co_return nullptr;
    }

    DocValueFilter* testFilter = CreateDocValueFilter(shape, sessionPool);
    auto* compositeIter =
        IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, SeekAndFilterIterator, iter, testFilter, sessionPool);
    co_return compositeIter;
}

DocValueFilter* SpatialIndexReader::CreateDocValueFilter(const std::shared_ptr<Shape>& shape,
                                                         autil::mem_pool::Pool* sessionPool)
{
    // current not support filter
    if (shape->GetType() == Shape::ShapeType::POLYGON || shape->GetType() == Shape::ShapeType::LINE) {
        return nullptr;
    }

    if (!_attrReader) {
        AUTIL_LOG(DEBUG, "no valid location attribute reader in spatial index [%s]",
                  _indexConfig->GetIndexName().c_str());
        return nullptr;
    }
    return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, SpatialDocValueFilter, shape, _attrReader, sessionPool);
}

std::shared_ptr<Shape> SpatialIndexReader::ParseShape(const std::string& shapeStr) const
{
    if (shapeStr.size() < 2) {
        AUTIL_LOG(WARN, "Invalid Shape [%s]", shapeStr.c_str());
        return nullptr;
    }
    if (shapeStr[shapeStr.size() - 1] != ')') {
        AUTIL_LOG(WARN, "Invalid Shape [%s]", shapeStr.c_str());
        return nullptr;
    }

    std::string str(shapeStr);
    size_t n = str.find('(');
    if (n == std::string::npos) {
        AUTIL_LOG(WARN, "Invalid Shape [%s]", shapeStr.c_str());
        return nullptr;
    }
    const std::string& shapeName = str.substr(0, n);
    const std::string& shapeArgs = str.substr(n + 1, str.size() - n - 2);
    return ShapeCreator::Create(shapeName, shapeArgs);
}

} // namespace indexlib::index
