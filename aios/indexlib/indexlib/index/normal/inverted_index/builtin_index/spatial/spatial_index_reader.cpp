#include <autil/StringUtil.h>
#include "indexlib/index/normal/inverted_index/builtin_index/spatial/spatial_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/spatial/spatial_doc_value_filter.h"
#include "indexlib/index/normal/inverted_index/builtin_index/spatial/spatial_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/seek_and_filter_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/doc_value_filter.h"
#include "indexlib/index/normal/attribute/accessor/location_attribute_reader.h"
#include "indexlib/common/field_format/spatial/shape/shape_creator.h"
#include "indexlib/common/field_format/spatial/geo_hash/geo_hash_util.h"
#include "indexlib/common/field_format/spatial/shape/shape_coverer.h"
#include "indexlib/common/field_format/spatial/shape_index_query_strategy.h"
#include "indexlib/common/field_format/spatial/location_index_query_strategy.h"
#include "indexlib/common/field_format/spatial/query_strategy.h"
#include "indexlib/common/field_format/spatial/shape/shape.h"
#include "indexlib/config/spatial_index_config.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SpatialIndexReader);

SpatialIndexReader::SpatialIndexReader()
{
}

SpatialIndexReader::~SpatialIndexReader() 
{
}

void SpatialIndexReader::Open(const config::IndexConfigPtr& indexConfig,
                              const index_base::PartitionDataPtr& partitionData)
{
    NormalIndexReader::Open(indexConfig, partitionData);
    SpatialIndexConfigPtr spatialIndexConfig = DYNAMIC_POINTER_CAST(
            SpatialIndexConfig, indexConfig);
    mTopLevel = GeoHashUtil::DistanceToLevel(spatialIndexConfig->GetMaxSearchDist());
    mDetailLevel = GeoHashUtil::DistanceToLevel(spatialIndexConfig->GetMaxDistError());
    FieldType type = spatialIndexConfig->GetFieldConfig()->GetFieldType();
    if (type == ft_location)
    {
        mQueryStrategy.reset(
                new LocationIndexQueryStrategy(
                        mTopLevel, mDetailLevel,
                        indexConfig->GetIndexName(),
                        spatialIndexConfig->GetDistanceLoss(), 3000));
    }
    else
    {
        mQueryStrategy.reset(
                new ShapeIndexQueryStrategy(
                        mTopLevel, mDetailLevel,
                        indexConfig->GetIndexName(),
                        spatialIndexConfig->GetDistanceLoss(), 3000));
    }
}

//TODO: refine with parser
ShapePtr SpatialIndexReader::ParseShape(const string& shapeStr) const
{
    if (shapeStr.size() < 2)
    {
        IE_LOG(WARN, "Invalid Shape [%s]", shapeStr.c_str());
        return ShapePtr();
    }
    if (shapeStr[shapeStr.size() - 1] != ')')
    {
        IE_LOG(WARN, "Invalid Shape [%s]", shapeStr.c_str());
        return ShapePtr();
    }

    string str(shapeStr);
    size_t n = str.find('(');
    if (n == string::npos)
    {
        IE_LOG(WARN, "Invalid Shape [%s]", shapeStr.c_str());
        return ShapePtr();
    }
    const string& shapeName = str.substr(0, n);
    const string& shapeArgs = str.substr(n + 1, str.size() - n - 2);
    return ShapeCreator::Create(shapeName, shapeArgs);
}

PostingIterator* SpatialIndexReader::Lookup(
        const Term& term, uint32_t statePoolSize,
        PostingType type, Pool *sessionPool)
{
    const string& shapeStr = term.GetWord();
    ShapePtr shape = ParseShape(shapeStr);
    if (!shape)
    {
        return NULL;
    }

    vector<dictkey_t> terms;
    mQueryStrategy->CalculateTerms(shape, terms);

    vector<BufferedPostingIterator*> postingIterators;
    Term defaultTerm;
    for (size_t i = 0; i < terms.size(); i++)
    {
        PostingIterator* postingIterator = NormalIndexReader::CreatePostingIteratorByHashKey(
            &defaultTerm, terms[i], {}, statePoolSize, sessionPool).get();
        if (postingIterator)
        {
            postingIterators.push_back((BufferedPostingIterator*)postingIterator);
        }
    }

    // TODO: if postingIterators.size() == 1 return it self
    SpatialPostingIterator* iter =
        CreateSpatialPostingIterator(postingIterators, sessionPool);
    if (!iter)
    {
        return NULL;
    }

    DocValueFilter* testFilter = CreateDocValueFilter(shape, sessionPool);
    auto* compositeIter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,
            SeekAndFilterIterator, iter, testFilter, sessionPool);
    return compositeIter;
}

SpatialPostingIterator* SpatialIndexReader::CreateSpatialPostingIterator(
        vector<BufferedPostingIterator*>& postingIterators, Pool* sessionPool)
{
    if (postingIterators.empty())
    {
        return NULL;
    }

    SpatialPostingIterator* iter = IE_POOL_COMPATIBLE_NEW_CLASS(
            sessionPool, SpatialPostingIterator, sessionPool);
    iter->Init(postingIterators);
    return iter;
}

void SpatialIndexReader::AddAttributeReader(const AttributeReaderPtr& attrReader)
{
    //TODO: line polygon support filter
    SpatialIndexConfigPtr spatialIndexConfig = DYNAMIC_POINTER_CAST(
            SpatialIndexConfig, mIndexConfig);
    FieldType type = spatialIndexConfig->GetFieldConfig()->GetFieldType();
    if (type != ft_location)
    {
        return;
    }
    mAttrReader = DYNAMIC_POINTER_CAST(LocationAttributeReader, attrReader);
    if (!mAttrReader)
    {
        IE_LOG(WARN, "spatial index reader should use with location attribute reader");
    }
}

DocValueFilter* SpatialIndexReader::CreateDocValueFilter(
        const ShapePtr& shape, Pool *sessionPool)
{
    //current not support filter
    if (shape->GetType() == Shape::POLYGON ||
        shape->GetType() == Shape::LINE)
    {
        return NULL;
    }

    if (!mAttrReader)
    {
        IE_LOG(DEBUG, "no valid location attribute reader in spatial index [%s]",
               mIndexConfig->GetIndexName().c_str());
        return NULL;
    }
    return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, SpatialDocValueFilter,
            shape, mAttrReader, sessionPool);
}

IE_NAMESPACE_END(index);

