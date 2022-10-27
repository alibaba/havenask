#include "indexlib/index/normal/inverted_index/builtin_index/range/building_range_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_in_mem_segment_reader.h"

using namespace std;
using namespace autil;



IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BuildingRangeIndexReader);

BuildingRangeIndexReader::BuildingRangeIndexReader(const PostingFormatOption& postingFormatOption)
    : BuildingIndexReader(postingFormatOption)
{
}

BuildingRangeIndexReader::~BuildingRangeIndexReader() 
{
}

void BuildingRangeIndexReader::Lookup(uint64_t leftTerm, uint64_t rightTerm,
                                      SegmentPostingsVec& segmentPostings,
                                      mem_pool::Pool *sessionPool)
{
    for (size_t i = 0; i < mInnerSegReaders.size(); ++i)
    {
        SegmentPostingsPtr segPosting(new SegmentPostings);
        RangeInMemSegmentReader* innerReader =
            (RangeInMemSegmentReader*)mInnerSegReaders[i].second.get();
        if (innerReader->Lookup(leftTerm, rightTerm,
                                mInnerSegReaders[i].first,
                                segPosting, sessionPool)
            && !segPosting->GetSegmentPostings().empty())
        {
            segmentPostings.push_back(segPosting);
        }
    }    
}


IE_NAMESPACE_END(index);
