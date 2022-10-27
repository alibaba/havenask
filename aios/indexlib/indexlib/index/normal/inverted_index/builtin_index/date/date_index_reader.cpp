#include "indexlib/index/normal/inverted_index/builtin_index/date/date_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/building_date_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/range_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/seek_and_filter_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/doc_value_filter.h"
#include "indexlib/common/field_format/date/date_query_parser.h"
#include "indexlib/common/field_format/date/date_term.h"
#include "indexlib/common/field_format/date/date_field_encoder.h"
#include "indexlib/common/number_term.h"
#include "indexlib/config/date_index_config.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DateIndexReader);

DateIndexReader::DateIndexReader()
{
}

DateIndexReader::~DateIndexReader() 
{
}

void DateIndexReader::Open(const IndexConfigPtr& indexConfig,
                           const PartitionDataPtr& partitionData)
{
    NormalIndexReader::Open(indexConfig, partitionData);
    mDateIndexConfig = DYNAMIC_POINTER_CAST(
            DateIndexConfig, indexConfig);
    mDateQueryParser.reset(new DateQueryParser);
    mDateQueryParser->Init(mDateIndexConfig);
}

PostingIterator* DateIndexReader::Lookup(const Term& term,
        uint32_t statePoolSize, PostingType type, Pool *sessionPool)
{
    uint64_t leftTerm = 0;
    uint64_t rightTerm = 0;
    if (!mDateQueryParser || !mDateQueryParser->ParseQuery(term, leftTerm, rightTerm))
    {
        return NULL;
    }
    
    SegmentPostingsVec dateSegmentPostings;
    for (size_t i = 0; i < mSegmentReaders.size(); i++)
    {
        ((DateIndexSegmentReader*)(mSegmentReaders[i].get()))->Lookup(
                leftTerm, rightTerm, dateSegmentPostings);
    }
    
    if (mBuildingIndexReader)
    {
        ((BuildingDateIndexReader*)mBuildingIndexReader.get())->Lookup(
                leftTerm, rightTerm, dateSegmentPostings, sessionPool);
    }

    RangePostingIterator* iter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,
            RangePostingIterator, mIndexFormatOption.GetPostingFormatOption(), sessionPool);
    if (!iter->Init(dateSegmentPostings))
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

DocValueFilter* DateIndexReader::CreateDocValueFilter(
        const Term& term, const AttributeReaderPtr& attrReader, Pool *sessionPool)
{
    if (!mAttrReader)
    {
        return NULL;
    }
    Int64Term intTerm = *(Int64Term*)(&term);
    AttrType type = attrReader->GetType();
    if (type != AT_UINT64)
    {
        IE_LOG(ERROR, "date index should use uint64 typed attribute");
        return NULL;
    }
    return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,
            NumberDocValueFilterTyped<uint64_t>, attrReader,
            intTerm.GetLeftNumber(), intTerm.GetRightNumber(), sessionPool);
}

NormalIndexSegmentReaderPtr DateIndexReader::CreateSegmentReader() 
{
    return NormalIndexSegmentReaderPtr(new DateIndexSegmentReader);
}

BuildingIndexReaderPtr DateIndexReader::CreateBuildingIndexReader()
{
    return BuildingIndexReaderPtr(new BuildingDateIndexReader(
                    mIndexFormatOption.GetPostingFormatOption()));
}

IE_NAMESPACE_END(index);

