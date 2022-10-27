#ifndef __INDEXLIB_RANGE_INDEX_READER_H
#define __INDEXLIB_RANGE_INDEX_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/common/field_format/range/range_field_encoder.h"

DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(index, DocValueFilter);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, RangeIndexSegmentReader);
DECLARE_REFERENCE_CLASS(index, BuildingRangeIndexReader);

IE_NAMESPACE_BEGIN(index);

class RangeIndexReader : public NormalIndexReader
{
public:
    RangeIndexReader();
    ~RangeIndexReader();
public:
    void Open(const config::IndexConfigPtr& indexConfig,
              const index_base::PartitionDataPtr& partitionData) override;    
    void AddAttributeReader(const AttributeReaderPtr& attrReader)
    { mAttrReader = attrReader; }
    index::PostingIterator* Lookup(const common::Term& term,
                                   uint32_t statePoolSize = 1000, 
                                   PostingType type = pt_default,
                                   autil::mem_pool::Pool *sessionPool = NULL) override;

    future_lite::Future<index::PostingIterator*> LookupAsync(const common::Term* term, uint32_t statePoolSize,
        PostingType type, autil::mem_pool::Pool* pool) override
    {
        return future_lite::makeReadyFuture<index::PostingIterator*>(Lookup(*term, statePoolSize, type, pool));
    }

    index::PostingIterator* PartialLookup(const common::Term& term, const DocIdRangeVector& ranges,
        uint32_t statePoolSize, PostingType type,
        autil::mem_pool::Pool* sessionPool) override
    {
        return Lookup(term, statePoolSize, type, sessionPool);
    }

protected:
    index::NormalIndexSegmentReaderPtr CreateSegmentReader() override;
    index::BuildingIndexReaderPtr CreateBuildingIndexReader() override;
    DocValueFilter* CreateDocValueFilter(
        const common::Term& term,
        const AttributeReaderPtr& attrReader,
        autil::mem_pool::Pool *sessionPool);

private:
    common::RangeFieldEncoder::RangeFieldType mRangeFieldType;
    AttributeReaderPtr mAttrReader;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeIndexReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_RANGE_INDEX_READER_H
