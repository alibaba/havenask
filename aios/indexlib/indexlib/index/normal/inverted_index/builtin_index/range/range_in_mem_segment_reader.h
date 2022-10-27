#ifndef __INDEXLIB_RANGE_IN_MEM_SEGMENT_READER_H
#define __INDEXLIB_RANGE_IN_MEM_SEGMENT_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_segment_reader.h"
#include "indexlib/common/field_format/range/range_field_encoder.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_postings.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"

DECLARE_REFERENCE_CLASS(index, InMemNormalIndexSegmentReader);
DECLARE_REFERENCE_CLASS(index, SegmentPostings);

IE_NAMESPACE_BEGIN(index);

class RangeInfo;
DEFINE_SHARED_PTR(RangeInfo);

class RangeInMemSegmentReader : public IndexSegmentReader
{
public:
    RangeInMemSegmentReader(
            InMemNormalIndexSegmentReaderPtr bottomReader,
            InMemNormalIndexSegmentReaderPtr highReader,
            RangeInfoPtr timeRangeInfo);
    ~RangeInMemSegmentReader();
public:
    AttributeSegmentReaderPtr GetSectionAttributeSegmentReader() const override
    {
        assert(false);
        return AttributeSegmentReaderPtr();
    }
    
    InMemBitmapIndexSegmentReaderPtr GetBitmapSegmentReader() const override
    {
        assert(false);
        return InMemBitmapIndexSegmentReaderPtr();
        //return mIndexSegmentReader->GetBitmapSegmentReader();
    }

    bool Lookup(uint64_t leftTerm, uint64_t rightTerm,
                docid_t baseDocId,
                const SegmentPostingsPtr &segPosting,
                autil::mem_pool::Pool* sessionPool) const;

private:
    void FillSegmentPostings(
            InMemNormalIndexSegmentReaderPtr reader,
            common::RangeFieldEncoder::Ranges& levelRanges,
            docid_t baseDocId,        
            SegmentPostingsPtr segPostings,
            autil::mem_pool::Pool* sessionPool) const;
private:
    InMemNormalIndexSegmentReaderPtr mBottomLevelReader;
    InMemNormalIndexSegmentReaderPtr mHighLevelReader;
    RangeInfoPtr mRangeInfo;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeInMemSegmentReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_RANGE_IN_MEM_SEGMENT_READER_H
