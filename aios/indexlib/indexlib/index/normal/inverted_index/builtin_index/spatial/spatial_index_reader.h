#ifndef __INDEXLIB_SPATIAL_INDEX_READER_H
#define __INDEXLIB_SPATIAL_INDEX_READER_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"

DECLARE_REFERENCE_CLASS(common, Shape);
DECLARE_REFERENCE_CLASS(common, QueryStrategy);
DECLARE_REFERENCE_CLASS(index, SpatialPostingIterator);
DECLARE_REFERENCE_CLASS(index, DocValueFilter);
DECLARE_REFERENCE_CLASS(index, LocationAttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeReader);

IE_NAMESPACE_BEGIN(index);

//TODO: inherit from IndexReader
class SpatialIndexReader : public index::NormalIndexReader
{
public:
    SpatialIndexReader();
    ~SpatialIndexReader();
public:
    void Open(const config::IndexConfigPtr& indexConfig,
              const index_base::PartitionDataPtr& partitionData) override;
    index::PostingIterator* Lookup(const common::Term& term,
                                   uint32_t statePoolSize = 1000, 
                                   PostingType type = pt_default,
                                   autil::mem_pool::Pool *sessionPool = NULL) override;

    future_lite::Future<index::PostingIterator*> LookupAsync(const common::Term* term, uint32_t statePoolSize,
        PostingType type, autil::mem_pool::Pool* pool) override
    {
        return future_lite::makeReadyFuture<index::PostingIterator*>(Lookup(*term, statePoolSize, type, pool));
    }    
    
    void AddAttributeReader(const AttributeReaderPtr& attrReader);

    index::PostingIterator* PartialLookup(const common::Term& term, const DocIdRangeVector& ranges,
                                          uint32_t statePoolSize, PostingType type,
                                          autil::mem_pool::Pool* sessionPool) override
    {
        return Lookup(term, statePoolSize, type, sessionPool);
    }    


private:
    SpatialPostingIterator* CreateSpatialPostingIterator(
            std::vector<BufferedPostingIterator*>& postingIterators,
            autil::mem_pool::Pool* sessionPool);
    common::ShapePtr ParseShape(const std::string& shapeStr) const;

    DocValueFilter* CreateDocValueFilter(
            const common::ShapePtr& shape,
            autil::mem_pool::Pool *sessionPool);

private:
    int8_t mTopLevel;
    int8_t mDetailLevel;
    LocationAttributeReaderPtr mAttrReader;
    common::QueryStrategyPtr mQueryStrategy;
    
private:
    friend class SpatialIndexReaderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SpatialIndexReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SPATIAL_INDEX_READER_H
