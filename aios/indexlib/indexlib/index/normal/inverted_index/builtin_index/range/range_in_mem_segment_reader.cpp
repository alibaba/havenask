#include "indexlib/index/normal/inverted_index/builtin_index/range/range_in_mem_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_info.h"

using namespace std;
IE_NAMESPACE_USE(common);

IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, RangeInMemSegmentReader);

RangeInMemSegmentReader::RangeInMemSegmentReader(
            InMemNormalIndexSegmentReaderPtr bottomReader,
            InMemNormalIndexSegmentReaderPtr highReader,
            RangeInfoPtr rangeInfo)
    : mBottomLevelReader(bottomReader)
    , mHighLevelReader(highReader)
    , mRangeInfo(rangeInfo)
{
}

RangeInMemSegmentReader::~RangeInMemSegmentReader() 
{
}

void RangeInMemSegmentReader::FillSegmentPostings(
        InMemNormalIndexSegmentReaderPtr reader,
        RangeFieldEncoder::Ranges& levelRanges,
        docid_t baseDocId,        
        SegmentPostingsPtr segPostings,
        autil::mem_pool::Pool* sessionPool) const                                     
{
    for(size_t i = 0; i < levelRanges.size(); i ++)
    {
        uint64_t left = levelRanges[i].first;
        uint64_t right = levelRanges[i].second;
        for (uint64_t j = left; j <= right; j ++)
        {
            SegmentPosting segPosting;
            if (reader->GetSegmentPosting(j, baseDocId, segPosting, sessionPool))
            {
                segPostings->AddSegmentPosting(segPosting);
            }
            if (j == right)     // when right equal numeric_limits<uin64_t>::max(), plus will cause overflow 
            {
                break;
            }
        }
    }    
}

bool RangeInMemSegmentReader::Lookup(uint64_t leftTerm, uint64_t rightTerm,
                                     docid_t baseDocId,
                                     const SegmentPostingsPtr &segPostings,
                                     autil::mem_pool::Pool* sessionPool) const
{
    uint64_t minNumber = mRangeInfo->GetMinNumber();
    uint64_t maxNumber = mRangeInfo->GetMaxNumber();
    if (minNumber > maxNumber || minNumber > rightTerm || maxNumber < leftTerm)
    {
        return false;
    }

    if (maxNumber < rightTerm)
    {
        rightTerm = maxNumber;
    }

    if (minNumber > leftTerm)
    {
        leftTerm = minNumber;
    }
    RangeFieldEncoder::Ranges bottomLevelRange;
    RangeFieldEncoder::Ranges higherLevelRange;
    RangeFieldEncoder::CalculateSearchRange(
            leftTerm, rightTerm, bottomLevelRange, higherLevelRange);

    FillSegmentPostings(mBottomLevelReader, bottomLevelRange, baseDocId, segPostings, sessionPool);
    FillSegmentPostings(mHighLevelReader, higherLevelRange, baseDocId, segPostings, sessionPool);
    return true;
}

IE_NAMESPACE_END(index);

