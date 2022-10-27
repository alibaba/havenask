#ifndef __INDEXLIB_DATE_INDEX_SEGMENT_READER_H
#define __INDEXLIB_DATE_INDEX_SEGMENT_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/date/date_term.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_postings.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_iterator.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_reader.h"

IE_NAMESPACE_BEGIN(index);

class TimeRangeInfo;
DEFINE_SHARED_PTR(TimeRangeInfo);

class DateIndexSegmentReader : public index::NormalIndexSegmentReader
{
public:
    DateIndexSegmentReader();
    ~DateIndexSegmentReader();
public:
    void Open(const config::IndexConfigPtr& indexConfig,
              const index_base::SegmentData& segmentData) override;
    void Lookup(uint64_t leftTerm, uint64_t rightTerm, SegmentPostingsVec& segmentPostings);
private:
    inline void FillDateSegmentPostings(const common::DateTerm::Ranges& ranges,
            const SegmentPostingsPtr& dateSegmentPostings);

    template <typename IteratorType>
    inline void FillDateSegmentPostings(const DictionaryIteratorPtr& iter,
            const common::DateTerm::Ranges& ranges,
            const SegmentPostingsPtr& dateSegmentPostings);

    void NormalizeTerms(
            uint64_t minTime, uint64_t maxTime,
            uint64_t& leftTerm, uint64_t& rightTerm);
private:
    index::DictionaryReader* CreateDictionaryReader(
        const config::IndexConfigPtr& indexConfig) override;

private:
    uint64_t mMinTime;
    uint64_t mMaxTime;
    config::DateLevelFormat mFormat;
    bool mIsHashTypeDict;
    
private:
    friend class DateIndexSegmentReaderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DateIndexSegmentReader);
inline void DateIndexSegmentReader::NormalizeTerms(
        uint64_t minTime, uint64_t maxTime,
        uint64_t& leftTerm, uint64_t& rightTerm)
{
    if (maxTime <= rightTerm)
    {
        rightTerm = common::DateTerm::NormalizeToNextYear(maxTime);
    }
    if (minTime >= leftTerm)
    {
        leftTerm = common::DateTerm::NormalizeToYear(minTime);
    }
}

inline void DateIndexSegmentReader::Lookup(uint64_t leftTerm, uint64_t rightTerm,
        SegmentPostingsVec& segmentPostings)
{
    if (mMinTime > mMaxTime || mMaxTime < leftTerm || mMinTime > rightTerm)
    {
        return;
    }

    NormalizeTerms(mMinTime, mMaxTime, leftTerm, rightTerm);
    common::DateTerm::Ranges ranges;
    common::DateTerm::CalculateRanges(leftTerm, rightTerm, mFormat, ranges);

    SegmentPostingsPtr dateSegmentPostings(new SegmentPostings);
    FillDateSegmentPostings(ranges, dateSegmentPostings);
    const SegmentPostingVector& postingVec = dateSegmentPostings->GetSegmentPostings();
    if (!postingVec.empty())
    {
        segmentPostings.push_back(dateSegmentPostings);
    }
}

inline void DateIndexSegmentReader::FillDateSegmentPostings(
        const common::DateTerm::Ranges& ranges,
        const SegmentPostingsPtr& dateSegmentPostings)
{
    DictionaryIteratorPtr iter = mDictReader->CreateIterator();
    if (mIsHashTypeDict)
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "date index should use tiered dictionary.");
    }
    else
    {
        FillDateSegmentPostings<TieredDictionaryIterator>(iter, ranges, dateSegmentPostings);
    }
}

template <typename IteratorType>
inline void DateIndexSegmentReader::FillDateSegmentPostings(
        const DictionaryIteratorPtr& iter,
        const common::DateTerm::Ranges& ranges,
        const SegmentPostingsPtr& dateSegmentPostings)
{
    IteratorType* iterImpl = static_cast<IteratorType*>(iter.get());
    for (size_t i = 0; i < ranges.size(); i ++)
    {
        if (!iterImpl->Seek((dictkey_t)ranges[i].first))
        {
            break;
        }

        dictkey_t key = ranges[i].first;
        dictvalue_t value = 0;
        while(iterImpl->IsValid())
        {
            iterImpl->GetCurrent(key, value);
            if (key > ranges[i].second)
            {
                break;
            }
            SegmentPosting segPosting;
            GetSegmentPosting(value, segPosting);
            dateSegmentPostings->AddSegmentPosting(segPosting);
            iterImpl->MoveToNext();
        }
    }
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DATE_INDEX_SEGMENT_READER_H
