#ifndef __INDEXLIB_DATE_IN_MEM_SEGMENT_READER_H
#define __INDEXLIB_DATE_IN_MEM_SEGMENT_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_postings.h"
#include "indexlib/config/date_level_format.h"

IE_NAMESPACE_BEGIN(index);
class TimeRangeInfo;
DEFINE_SHARED_PTR(TimeRangeInfo);

class DateInMemSegmentReader : public index::IndexSegmentReader
{
public:
    DateInMemSegmentReader(
            index::InMemNormalIndexSegmentReaderPtr indexSegmentReader,
            config::DateLevelFormat format,
            TimeRangeInfoPtr timeRangeInfo);
    ~DateInMemSegmentReader();
public:
    index::AttributeSegmentReaderPtr GetSectionAttributeSegmentReader() const override
    { return mIndexSegmentReader->GetSectionAttributeSegmentReader(); }
    
    index::InMemBitmapIndexSegmentReaderPtr GetBitmapSegmentReader() const override
    { return mIndexSegmentReader->GetBitmapSegmentReader(); }

    bool Lookup(uint64_t leftTerm, uint64_t rightTerm,
                 docid_t baseDocId,
                 const SegmentPostingsPtr &segPosting,
                 autil::mem_pool::Pool* sessionPool) const;
private:
    config::DateLevelFormat mFormat;
    index::InMemNormalIndexSegmentReaderPtr mIndexSegmentReader;
    TimeRangeInfoPtr mTimeRangeInfo;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DateInMemSegmentReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DATE_IN_MEM_SEGMENT_READER_H
