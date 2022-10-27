#ifndef __INDEXLIB_RANGE_POSTING_ITERATOR_H
#define __INDEXLIB_RANGE_POSTING_ITERATOR_H

#include "indexlib/index/normal/inverted_index/accessor/posting_iterator_impl.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_postings.h"
#include "indexlib/index/normal/inverted_index/accessor/range_segment_postings_iterator.h"


IE_NAMESPACE_BEGIN(index);

class RangePostingIterator : public PostingIteratorImpl
{
public:
    RangePostingIterator(const index::PostingFormatOption& postingFormatOption,
                        autil::mem_pool::Pool* sessionPool);
    ~RangePostingIterator();
public:
    PostingIteratorType GetType() const override { return pi_range; }
    bool Init(const SegmentPostingsVec& segPostings);
    docid_t SeekDoc(docid_t docid) override { return InnerSeekDoc(docid); }
    common::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override;

    docpayload_t GetDocPayload() override { return 0;  }
    pospayload_t GetPosPayload() { return 0; }
    bool HasPosition() const override
    { return false; }
    uint32_t GetSeekDocCount() const { return mSeekDocCounter; }
    //TODO: support
    void Unpack(TermMatchData& termMatchData) override;
    PostingIterator* Clone() const override;
    void Reset() override;
    index::TermPostingInfo GetTermPostingInfo() const override;
private:
    uint32_t LocateSegment(uint32_t startSegCursor, docid_t startDocId);
    bool MoveToSegment(docid_t docid);
    docid_t GetSegmentBaseDocId(uint32_t segmentCursor);
public:
    docid_t InnerSeekDoc(docid_t docid);
    common::ErrorCode InnerSeekDoc(docid_t docid, docid_t& result);
private:
    int32_t mSegmentCursor;
    docid_t mCurrentDocId;
    uint32_t mSeekDocCounter;
    index::PostingFormatOption mPostingFormatOption;
    SegmentPostingsVec mSegmentPostings;
    RangeSegmentPostingsIterator mSegmentPostingsIterator;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangePostingIterator);
////////////////////////////////////////////////////////////////

inline common::ErrorCode RangePostingIterator::SeekDocWithErrorCode(docid_t docId, docid_t& result)
{
    return InnerSeekDoc(docId, result);
}

inline docid_t RangePostingIterator::InnerSeekDoc(docid_t docid)
{
    docid_t ret = INVALID_DOCID;
    auto ec = InnerSeekDoc(docid, ret);
    common::ThrowIfError(ec);
    return ret;
}

inline common::ErrorCode RangePostingIterator::InnerSeekDoc(docid_t docid, docid_t& result)
{
    docid_t curDocId = mCurrentDocId;
    docid = std::max(curDocId+1, docid);
    while(true)
    {
        auto seekRet = mSegmentPostingsIterator.Seek(docid);
        if (unlikely(!seekRet.Ok()))
        {
            return seekRet.GetErrorCode();
        }
        if (seekRet.Value())
        {
            mCurrentDocId = mSegmentPostingsIterator.GetCurrentDocid();
            result = mCurrentDocId;
            return common::ErrorCode::OK;
        }
        try {
            if (!MoveToSegment(docid))
            {
                result = INVALID_DOCID;
                return common::ErrorCode::OK;
            }
        }
        catch (const misc::FileIOException& e)
        {
            return common::ErrorCode::FileIO;
        }
    }    
}

inline bool RangePostingIterator::MoveToSegment(docid_t docid)
{
    if (mSegmentCursor >= (int32_t)mSegmentPostings.size())
    {
        return false;
    }
    mSegmentCursor = LocateSegment(mSegmentCursor + 1, docid);
    if (mSegmentCursor >= (int32_t)mSegmentPostings.size())
    {
        return false;
    }
    mSegmentPostingsIterator.Init(mSegmentPostings[mSegmentCursor], docid,
                                  mSegmentCursor + 1 < (int32_t)mSegmentPostings.size() ?
                                  mSegmentPostings[mSegmentCursor + 1]->GetBaseDocId()
                                  : INVALID_DOCID);
    return true;
}

inline uint32_t RangePostingIterator::LocateSegment(
        uint32_t startSegCursor, docid_t startDocId)
{
    uint32_t curSegCursor = startSegCursor;
    docid_t nextSegBaseDocId = GetSegmentBaseDocId(curSegCursor + 1);
    while (nextSegBaseDocId != INVALID_DOCID && startDocId >= nextSegBaseDocId) 
    {
        ++curSegCursor;
        nextSegBaseDocId = GetSegmentBaseDocId(curSegCursor + 1);
    }

    return curSegCursor;
}

inline docid_t RangePostingIterator::GetSegmentBaseDocId(uint32_t segmentCursor)
{
    if (segmentCursor >= mSegmentPostings.size())
    {
        return INVALID_DOCID;
    }
    return mSegmentPostings[segmentCursor]->GetBaseDocId();
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DATE_POSTING_ITERATOR_H
