#ifndef __INDEXLIB_RANGE_LEVEL_SEGMENT_READER_H
#define __INDEXLIB_RANGE_LEVEL_SEGMENT_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_segment_reader.h"
#include "indexlib/common/field_format/range/range_field_encoder.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_postings.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_iterator.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_reader.h"

IE_NAMESPACE_BEGIN(index);

class RangeLevelSegmentReader : public index::NormalIndexSegmentReader
{
public:
    RangeLevelSegmentReader(const std::string& parentIndexName)
        : mParentIndexName(parentIndexName)
        , mIsHashTypeDict(false)
    {}
    ~RangeLevelSegmentReader() {}
public:
    void Open(const config::IndexConfigPtr& indexConfig,
              const index_base::SegmentData& segmentData) override;
    
    inline void FillSegmentPostings(
            const common::RangeFieldEncoder::Ranges& ranges,
            const SegmentPostingsPtr& segmentPostings);

    template <typename IteratorType>
    inline void FillSegmentPostings(const DictionaryIteratorPtr& iter,
            const common::RangeFieldEncoder::Ranges& ranges,            
            const SegmentPostingsPtr& segmentPostings);
    
private:
    index::DictionaryReader* CreateDictionaryReader(
        const config::IndexConfigPtr& indexConfig) override;

private:
    std::string mParentIndexName;
    bool mIsHashTypeDict;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeLevelSegmentReader);
//////////////////////////////////////////
inline void RangeLevelSegmentReader::FillSegmentPostings(
        const common::RangeFieldEncoder::Ranges& ranges,
        const SegmentPostingsPtr& segmentPostings)
{
    DictionaryIteratorPtr iter = mDictReader->CreateIterator();
    if (mIsHashTypeDict)
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "range index should use tiered dictionary.");
    }
    else
    {
        FillSegmentPostings<TieredDictionaryIterator>(iter, ranges, segmentPostings);        
    }
}

template <typename IteratorType>
inline void RangeLevelSegmentReader::FillSegmentPostings(
        const DictionaryIteratorPtr& iter,
        const common::RangeFieldEncoder::Ranges& ranges,            
        const SegmentPostingsPtr& segmentPostings)
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
            segmentPostings->AddSegmentPosting(segPosting);
            iterImpl->MoveToNext();
        }
    }
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_RANGE_LEVEL_SEGMENT_READER_H
