#ifndef __INDEXLIB_IN_MEMORY_SEGMENT_READER_H
#define __INDEXLIB_IN_MEMORY_SEGMENT_READER_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/RecyclePool.h>
#include <autil/mem_pool/ChunkAllocatorBase.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_info.h"

DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);
DECLARE_REFERENCE_CLASS(index, SummarySegmentReader);
DECLARE_REFERENCE_CLASS(index, InMemDeletionMapReader);
DECLARE_REFERENCE_CLASS(index, IndexSegmentReader);
DECLARE_REFERENCE_CLASS(index, MultiFieldIndexSegmentReader);

IE_NAMESPACE_BEGIN(index);

class InMemorySegmentReader
{
public:
    typedef std::map<std::string, AttributeSegmentReaderPtr> String2AttrReaderMap;
    typedef std::tr1::shared_ptr<autil::mem_pool::ChunkAllocatorBase> AllocatorPtr;
    typedef std::tr1::shared_ptr<autil::mem_pool::Pool> PoolPtr;
    typedef std::tr1::shared_ptr<autil::mem_pool::RecyclePool> RecyclePoolPtr;

public:
    InMemorySegmentReader(segmentid_t segId = INVALID_SEGMENTID);
    virtual ~InMemorySegmentReader();

public:
    void Init(const index::MultiFieldIndexSegmentReaderPtr& indexSegmentReader,
              const String2AttrReaderMap& attrReaders,
              const SummarySegmentReaderPtr& summaryReader,
              const InMemDeletionMapReaderPtr& delMap,
              const index_base::SegmentInfo& segmentInfo);

    const index::MultiFieldIndexSegmentReaderPtr& GetMultiFieldIndexSegmentReader() const
    { return mIndexSegmentReader; }

    virtual index::IndexSegmentReaderPtr GetSingleIndexSegmentReader(
        const std::string& name
        ) const;
    virtual AttributeSegmentReaderPtr GetAttributeSegmentReader(const std::string& name);
    virtual SummarySegmentReaderPtr GetSummaryReader() const;
    InMemDeletionMapReaderPtr GetInMemDeletionMapReader() const;
    const index_base::SegmentInfo& GetSegmentInfo() const { return mSegmentInfo; }
    segmentid_t GetSegmentId() const { return mSegmentId; }

private:
    segmentid_t mSegmentId;
    String2AttrReaderMap mAttrReaders;
    SummarySegmentReaderPtr mSummaryReader;
    InMemDeletionMapReaderPtr mInMemDeletionMapReader;
    index_base::SegmentInfo mSegmentInfo;
    index::MultiFieldIndexSegmentReaderPtr mIndexSegmentReader;

    IE_LOG_DECLARE();

};

DEFINE_SHARED_PTR(InMemorySegmentReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEMORY_SEGMENT_READER_H
