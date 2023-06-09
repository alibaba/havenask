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
#include "indexlib/index/normal/inverted_index/builtin_index/spatial/spatial_index_reader.h"

#include "autil/StringUtil.h"
#include "indexlib/config/spatial_index_config.h"
#include "indexlib/index/common/field_format/spatial/LocationIndexQueryStrategy.h"
#include "indexlib/index/common/field_format/spatial/QueryStrategy.h"
#include "indexlib/index/common/field_format/spatial/ShapeIndexQueryStrategy.h"
#include "indexlib/index/common/field_format/spatial/geo_hash/GeoHashUtil.h"
#include "indexlib/index/common/field_format/spatial/shape/Shape.h"
#include "indexlib/index/common/field_format/spatial/shape/ShapeCoverer.h"
#include "indexlib/index/common/field_format/spatial/shape/ShapeCreator.h"
#include "indexlib/index/inverted_index/DocValueFilter.h"
#include "indexlib/index/inverted_index/SeekAndFilterIterator.h"
#include "indexlib/index/inverted_index/builtin_index/spatial/SpatialPostingIterator.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/spatial/spatial_doc_value_filter.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::common;
using namespace indexlib::config;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, SpatialIndexReader);

SpatialIndexReader::SpatialIndexReader() {}

SpatialIndexReader::~SpatialIndexReader() {}

void SpatialIndexReader::Open(const config::IndexConfigPtr& indexConfig,
                              const index_base::PartitionDataPtr& partitionData, const InvertedIndexReader* hintReader)
{
    NormalIndexReader::Open(indexConfig, partitionData, hintReader);
    SpatialIndexConfigPtr spatialIndexConfig = DYNAMIC_POINTER_CAST(SpatialIndexConfig, indexConfig);
    mTopLevel = GeoHashUtil::DistanceToLevel(spatialIndexConfig->GetMaxSearchDist());
    mDetailLevel = GeoHashUtil::DistanceToLevel(spatialIndexConfig->GetMaxDistError());
    FieldType type = spatialIndexConfig->GetFieldConfig()->GetFieldType();
    if (type == ft_location) {
        mQueryStrategy.reset(new LocationIndexQueryStrategy(mTopLevel, mDetailLevel, indexConfig->GetIndexName(),
                                                            spatialIndexConfig->GetDistanceLoss(), 3000));
    } else {
        mQueryStrategy.reset(new ShapeIndexQueryStrategy(mTopLevel, mDetailLevel, indexConfig->GetIndexName(),
                                                         spatialIndexConfig->GetDistanceLoss(), 3000));
    }
}

// TODO: refine with parser
std::shared_ptr<Shape> SpatialIndexReader::ParseShape(const string& shapeStr) const
{
    if (shapeStr.size() < 2) {
        IE_LOG(WARN, "Invalid Shape [%s]", shapeStr.c_str());
        return std::shared_ptr<Shape>();
    }
    if (shapeStr[shapeStr.size() - 1] != ')') {
        IE_LOG(WARN, "Invalid Shape [%s]", shapeStr.c_str());
        return std::shared_ptr<Shape>();
    }

    string str(shapeStr);
    size_t n = str.find('(');
    if (n == string::npos) {
        IE_LOG(WARN, "Invalid Shape [%s]", shapeStr.c_str());
        return std::shared_ptr<Shape>();
    }
    const string& shapeName = str.substr(0, n);
    const string& shapeArgs = str.substr(n + 1, str.size() - n - 2);
    return ShapeCreator::Create(shapeName, shapeArgs);
}

index::Result<PostingIterator*> SpatialIndexReader::Lookup(const Term& term, uint32_t statePoolSize, PostingType type,
                                                           Pool* sessionPool)
{
    return future_lite::coro::syncAwait(LookupAsync(&term, statePoolSize, type, sessionPool, /*option*/ nullptr));
}

future_lite::coro::Lazy<index::Result<PostingIterator*>>
SpatialIndexReader::LookupAsync(const Term* term, uint32_t statePoolSize, PostingType type, Pool* sessionPool,
                                file_system::ReadOption option) noexcept
{
    const string& shapeStr = term->GetWord();
    std::shared_ptr<Shape> shape = ParseShape(shapeStr);
    if (!shape) {
        co_return nullptr;
    }

    vector<dictkey_t> terms = mQueryStrategy->CalculateTerms(shape);

    DocIdRangeVector ranges;
    if (term->HasHintValues()) {
        ranges = MergeDocIdRanges(term->GetHintValues(), ranges);
    }

    ValidatePartitonRange(ranges);
    vector<BufferedPostingIterator*> postingIterators;
    Term defaultTerm;
    defaultTerm.SetNull(false);
    std::vector<future_lite::coro::Lazy<index::Result<PostingIterator*>>> tasks;
    tasks.reserve(terms.size());
    for (size_t i = 0; i < terms.size(); i++) {
        tasks.push_back(NormalIndexReader::CreatePostingIteratorByHashKey(&defaultTerm, index::DictKeyInfo(terms[i]),
                                                                          ranges, statePoolSize, sessionPool, option));
    }

    auto results = co_await future_lite::coro::collectAll(std::move(tasks));
    assert(terms.size() == results.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        assert(!results[i].hasError());
        if (results[i].value().Ok()) {
            PostingIterator* postingIterator = results[i].value().Value();
            if (postingIterator) {
                postingIterators.push_back((BufferedPostingIterator*)postingIterator);
            }
        } else {
            for (size_t j = 0; j < postingIterators.size(); ++j) {
                IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, postingIterators[j]);
            }
            co_return results[i].value().GetErrorCode();
        }
    }
    // TODO: if postingIterators.size() == 1 return it self
    auto iter = CreateSpatialPostingIterator(postingIterators, sessionPool);
    if (!iter) {
        co_return nullptr;
    }

    DocValueFilter* testFilter = CreateDocValueFilter(shape, sessionPool);
    auto* compositeIter =
        IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, SeekAndFilterIterator, iter, testFilter, sessionPool);
    co_return compositeIter;
}

SpatialPostingIterator*
SpatialIndexReader::CreateSpatialPostingIterator(vector<BufferedPostingIterator*>& postingIterators, Pool* sessionPool)
{
    if (postingIterators.empty()) {
        return NULL;
    }

    SpatialPostingIterator* iter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, SpatialPostingIterator, sessionPool);
    iter->Init(postingIterators);
    return iter;
}

void SpatialIndexReader::AddAttributeReader(const AttributeReaderPtr& attrReader)
{
    // TODO: line polygon support filter
    SpatialIndexConfigPtr spatialIndexConfig = DYNAMIC_POINTER_CAST(SpatialIndexConfig, _indexConfig);
    FieldType type = spatialIndexConfig->GetFieldConfig()->GetFieldType();
    if (type != ft_location) {
        return;
    }
    mAttrReader = DYNAMIC_POINTER_CAST(LocationAttributeReader, attrReader);
    if (!mAttrReader) {
        IE_LOG(WARN, "spatial index reader should use with location attribute reader");
    }
}

DocValueFilter* SpatialIndexReader::CreateDocValueFilter(const std::shared_ptr<Shape>& shape, Pool* sessionPool)
{
    // current not support filter
    if (shape->GetType() == Shape::ShapeType::POLYGON || shape->GetType() == Shape::ShapeType::LINE) {
        return NULL;
    }

    if (!mAttrReader) {
        IE_LOG(DEBUG, "no valid location attribute reader in spatial index [%s]", _indexConfig->GetIndexName().c_str());
        return NULL;
    }
    return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, SpatialDocValueFilter, shape, mAttrReader, sessionPool);
}
}}} // namespace indexlib::index::legacy
