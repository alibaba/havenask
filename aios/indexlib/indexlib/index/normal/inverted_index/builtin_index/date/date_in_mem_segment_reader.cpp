#include "indexlib/index/normal/inverted_index/builtin_index/date/date_in_mem_segment_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/time_range_info.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_iterator.h"
#include "indexlib/common/field_format/date/date_term.h"

using namespace std;
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DateInMemSegmentReader);

DateInMemSegmentReader::DateInMemSegmentReader(
            index::InMemNormalIndexSegmentReaderPtr indexSegmentReader,
            config::DateLevelFormat format,
            TimeRangeInfoPtr timeRangeInfo)
    : mFormat(format)
    , mIndexSegmentReader(indexSegmentReader)
    , mTimeRangeInfo(timeRangeInfo)
{
}

DateInMemSegmentReader::~DateInMemSegmentReader() 
{
}



bool DateInMemSegmentReader::Lookup(uint64_t leftTerm, uint64_t rightTerm,
                                     docid_t baseDocId,
                                     const SegmentPostingsPtr& segPostings,
                                     autil::mem_pool::Pool* sessionPool) const
{
    uint64_t minTime = mTimeRangeInfo->GetMinTime();
    uint64_t maxTime = mTimeRangeInfo->GetMaxTime();
    if (minTime > maxTime || maxTime < leftTerm || minTime > rightTerm)
    {
        return false;
    }
    if (maxTime <= rightTerm)
    {
        rightTerm = maxTime;
    }
    if (minTime >= leftTerm)
    {
        leftTerm = minTime;
    }
    std::vector<uint64_t> terms;
    DateTerm::CalculateTerms(leftTerm, rightTerm, mFormat, terms);
    for(size_t i = 0; i < terms.size(); i ++)
    {
        SegmentPosting segPosting;
        if (mIndexSegmentReader->GetSegmentPosting(terms[i], baseDocId, segPosting, sessionPool))
        {
            segPostings->AddSegmentPosting(segPosting);
        }
    }
    return true;
}


IE_NAMESPACE_END(index);

