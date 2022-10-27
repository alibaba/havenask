#include "indexlib/index/normal/inverted_index/builtin_index/date/building_date_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_in_mem_segment_reader.h"

using namespace std;
using namespace autil;


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BuildingDateIndexReader);

BuildingDateIndexReader::BuildingDateIndexReader(const PostingFormatOption& postingFormatOption)
    : BuildingIndexReader(postingFormatOption)
{
}

BuildingDateIndexReader::~BuildingDateIndexReader() 
{
}

void BuildingDateIndexReader::Lookup(uint64_t leftTerm, uint64_t rightTerm,
                                     SegmentPostingsVec& segmentPostings,
                                     mem_pool::Pool *sessionPool)
{
    for (size_t i = 0; i < mInnerSegReaders.size(); ++i)
    {
        index::SegmentPosting segPosting(mPostingFormatOption);
        SegmentPostingsPtr dateSegmentPosting(new SegmentPostings);
        DateInMemSegmentReader* innerReader =
            (DateInMemSegmentReader*)mInnerSegReaders[i].second.get();
        if (innerReader->Lookup(leftTerm, rightTerm,
                                mInnerSegReaders[i].first,
                                dateSegmentPosting, sessionPool)
	    && !dateSegmentPosting->GetSegmentPostings().empty())
        {
            segmentPostings.push_back(dateSegmentPosting);
        }
    }
}

IE_NAMESPACE_END(index);

