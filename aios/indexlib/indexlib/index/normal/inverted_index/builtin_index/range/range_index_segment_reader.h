#ifndef __INDEXLIB_RANGE_INDEX_SEGMENT_READER_H
#define __INDEXLIB_RANGE_INDEX_SEGMENT_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_level_segment_reader.h"

IE_NAMESPACE_BEGIN(index);

class RangeIndexSegmentReader : public index::NormalIndexSegmentReader
{
public:
    RangeIndexSegmentReader()
    {}
    ~RangeIndexSegmentReader()
    {}
    
public:
    void Open(const config::IndexConfigPtr& indexConfig,
              const index_base::SegmentData& segmentData) override;
    void Lookup(uint64_t leftTerm, uint64_t rightTerm, SegmentPostingsVec& segmentPostings);

private:
    uint64_t mMinNumber;
    uint64_t mMaxNumber;
    RangeLevelSegmentReaderPtr mBottomLevelReader;
    RangeLevelSegmentReaderPtr mHighLevelReader;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeIndexSegmentReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_RANGE_INDEX_SEGMENT_READER_H
