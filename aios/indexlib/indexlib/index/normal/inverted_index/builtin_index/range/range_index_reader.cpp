#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/building_range_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/range_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/seek_and_filter_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/doc_value_filter.h"
#include "indexlib/config/range_index_config.h"
#include "indexlib/config/field_type_traits.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, RangeIndexReader);

RangeIndexReader::RangeIndexReader()
    : mRangeFieldType(RangeFieldEncoder::UNKOWN)
{
}

RangeIndexReader::~RangeIndexReader() 
{
}

DocValueFilter* RangeIndexReader::CreateDocValueFilter(
        const common::Term& term,
        const AttributeReaderPtr& attrReader,
        autil::mem_pool::Pool *sessionPool)
{
    if (!mAttrReader)
    {
        return NULL;
    }
    
    Int64Term intTerm = *(Int64Term*)(&term);
    DocValueFilter* iter = NULL;
    AttrType type = attrReader->GetType();
    switch (type) {
#define CREATE_ITERATOR_TYPED(attrType)                                 \
        case attrType:                                                  \
        {                                                               \
            typedef AttrTypeTraits<attrType>::AttrItemType Type;        \
            iter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,            \
		    NumberDocValueFilterTyped<Type>, attrReader,        \
                    intTerm.GetLeftNumber(),                            \
                    intTerm.GetRightNumber(), sessionPool);             \
            break;                                                      \
        }
        NUMBER_ATTRIBUTE_MACRO_HELPER(CREATE_ITERATOR_TYPED);
#undef CREATE_ITERATOR_TYPED
    default:
        assert(false);
    }
    return iter;
}

index::PostingIterator* RangeIndexReader::Lookup(const Term& term,
        uint32_t statePoolSize, PostingType type,
        autil::mem_pool::Pool *sessionPool) 
{
    uint64_t leftTerm = 0;
    uint64_t rightTerm = 0;
    const common::Int64Term* rangeTerm = dynamic_cast<const common::Int64Term*>(&term);
    if (!rangeTerm)
    {
        return NULL;
    }
    RangeFieldEncoder::ConvertTerm(rangeTerm, mRangeFieldType, leftTerm, rightTerm);
    SegmentPostingsVec rangeSegmentPostings;
    for (size_t i = 0; i < mSegmentReaders.size(); i++)
    {
        ((RangeIndexSegmentReader*)(mSegmentReaders[i].get()))->Lookup(
                leftTerm, rightTerm, rangeSegmentPostings);
    }
    
    if (mBuildingIndexReader)
    {
        ((BuildingRangeIndexReader*)mBuildingIndexReader.get())->Lookup(
                leftTerm, rightTerm, rangeSegmentPostings, sessionPool);
    }

    RangePostingIterator* iter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,
            RangePostingIterator, mIndexFormatOption.GetPostingFormatOption(), sessionPool);
    if (!iter->Init(rangeSegmentPostings))
    {
        IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, iter);
        return NULL;
    }

    DocValueFilter* testIter =
        CreateDocValueFilter(term, mAttrReader, sessionPool);
    auto* compositeIter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,
            SeekAndFilterIterator, iter, testIter, sessionPool);
    return compositeIter;
}

void RangeIndexReader::Open(const IndexConfigPtr& indexConfig,
                            const PartitionDataPtr& partitionData)
{
    NormalIndexReader::Open(indexConfig, partitionData);
    RangeIndexConfigPtr rangeConfig =
        DYNAMIC_POINTER_CAST(RangeIndexConfig, indexConfig);
    FieldType fieldType = rangeConfig->GetFieldConfig()->GetFieldType();
    mRangeFieldType = RangeFieldEncoder::GetRangeFieldType(fieldType);
}

NormalIndexSegmentReaderPtr RangeIndexReader::CreateSegmentReader()
{
    return NormalIndexSegmentReaderPtr(new RangeIndexSegmentReader);
}

BuildingIndexReaderPtr RangeIndexReader::CreateBuildingIndexReader()
{
    return BuildingIndexReaderPtr(new BuildingRangeIndexReader(
                    mIndexFormatOption.GetPostingFormatOption()));
}

IE_NAMESPACE_END(index);

