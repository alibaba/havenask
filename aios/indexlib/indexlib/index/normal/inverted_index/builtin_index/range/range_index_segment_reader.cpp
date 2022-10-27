#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_info.h"
#include "indexlib/common/field_format/range/range_field_encoder.h"
#include "indexlib/config/range_index_config.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, RangeIndexSegmentReader);

void RangeIndexSegmentReader::Open(const IndexConfigPtr& indexConfig,
                                   const SegmentData& segmentData)
{
    mSegmentData = segmentData;
    mBottomLevelReader.reset(
            new RangeLevelSegmentReader(indexConfig->GetIndexName()));
    RangeIndexConfigPtr rangeConfig =
        DYNAMIC_POINTER_CAST(RangeIndexConfig, indexConfig);
    mBottomLevelReader->Open(rangeConfig->GetBottomLevelIndexConfig(), segmentData);
    mHighLevelReader.reset(
            new RangeLevelSegmentReader(indexConfig->GetIndexName()));
    mHighLevelReader->Open(rangeConfig->GetHighLevelIndexConfig(), segmentData);
    file_system::DirectoryPtr indexDirectory = segmentData.GetIndexDirectory(
            indexConfig->GetIndexName(), true);
    RangeInfo rangeInfo;
    rangeInfo.Load(indexDirectory);
    mMinNumber = rangeInfo.GetMinNumber();
    mMaxNumber = rangeInfo.GetMaxNumber();
}

void RangeIndexSegmentReader::Lookup(
        uint64_t leftTerm, uint64_t rightTerm, SegmentPostingsVec& segmentPostingsVec)
{
    if (mMinNumber > mMaxNumber || mMinNumber > rightTerm || mMaxNumber < leftTerm)
    {
        return;
    }

    bool alignRight = false;
    bool alignLeft = false;
    if (mMaxNumber < rightTerm)
    {
        rightTerm = mMaxNumber;
        alignRight = true;
    }

    if (mMinNumber > leftTerm)
    {
        leftTerm = mMinNumber;
        alignLeft = true;
    }

    RangeFieldEncoder::AlignedTerm(alignLeft, alignRight, leftTerm, rightTerm);
    RangeFieldEncoder::Ranges bottomLevelRange;
    RangeFieldEncoder::Ranges higherLevelRange;
    RangeFieldEncoder::CalculateSearchRange(
            leftTerm, rightTerm, bottomLevelRange, higherLevelRange);
    SegmentPostingsPtr segmentPostings(new SegmentPostings);
    mBottomLevelReader->FillSegmentPostings(bottomLevelRange, segmentPostings);
    mHighLevelReader->FillSegmentPostings(higherLevelRange, segmentPostings);
    if (!segmentPostings->GetSegmentPostings().empty())
    {
        segmentPostingsVec.push_back(segmentPostings);
    }
}

IE_NAMESPACE_END(index);

