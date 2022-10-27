#ifndef __INDEXLIB_POSITION_ITERATOR_TYPED_H
#define __INDEXLIB_POSITION_ITERATOR_TYPED_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator_impl.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_reader_impl.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/single_bitmap_posting_iterator.h"
#include "indexlib/common/byte_slice_reader.h"

IE_NAMESPACE_BEGIN(index);

template<class SingleIterator>
class PositionIteratorTyped : public PostingIteratorImpl
{
public:
    typedef SingleIterator SingleIteratorType;
    typedef typename SingleIterator::InDocPositionStateType StateType;

public:
    PositionIteratorTyped(optionflag_t optionFlag = OPTION_FLAG_ALL,
                          autil::mem_pool::Pool *sessionPool = NULL);
    virtual ~PositionIteratorTyped();

public:
    virtual bool Init(const SegmentPostingVectorPtr& segPostings,
                      const uint32_t statePoolSize);

    docid_t SeekDoc(docid_t docId) override;
    common::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override;
    common::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t& nextpos) override;
    void Unpack(TermMatchData& termMatchData) override;

    docpayload_t GetDocPayload() override;
    pospayload_t GetPosPayload() const;
    PostingIteratorType GetType() const override;
    bool HasPosition() const override
    { return mOptionFlag & of_position_list; }

public:
    // for runtime inline.
    docid_t InnerSeekDoc(docid_t docId);
    common::ErrorCode InnerSeekDoc(docid_t docId, docid_t& result);
    void Reset() override; 

protected:
    common::Result<bool> MoveToNextSegment();
    virtual common::ErrorCode InitSingleIterator(SingleIteratorType *singleIterator) = 0;

protected:
    SingleIteratorType* CreateSingleIterator(uint8_t compressMode) const;

protected:
    optionflag_t mOptionFlag;
    std::vector<SingleIteratorType*> mSingleIterators;
    int32_t mSegmentCursor;
    util::ObjectPool<StateType> mStatePool;
private:
    IE_LOG_DECLARE();
};

////////////////////////////////////////////////////////////
template<class SingleIterator>
PositionIteratorTyped<SingleIterator>::PositionIteratorTyped(
        optionflag_t optionFlag,
        autil::mem_pool::Pool *sessionPool)
    : PostingIteratorImpl(sessionPool)
    , mOptionFlag(optionFlag)
    , mSegmentCursor(-1)
{
}

template<class SingleIterator>
PositionIteratorTyped<SingleIterator>::~PositionIteratorTyped() 
{
    for (size_t i = 0; i < mSingleIterators.size(); i++)
    {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mSingleIterators[i]);
    }
    mSingleIterators.clear();
}

template<class SingleIterator>
bool PositionIteratorTyped<SingleIterator>::Init(const SegmentPostingVectorPtr& segPostings,
        const uint32_t statePoolSize)
{
    if (!PostingIteratorImpl::Init(segPostings))
    {
        return false;
    }
    mStatePool.Init(statePoolSize);
    mSegmentCursor = -1;
    return MoveToNextSegment().ValueOrThrow();
}

template<class SingleIterator>
void PositionIteratorTyped<SingleIterator>::Reset()
{
    for (size_t i = 0; i < mSingleIterators.size(); i++)
    {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mSingleIterators[i]);
    }
    mSingleIterators.clear();

    mSegmentCursor = -1;
    MoveToNextSegment().ValueOrThrow();
}

template<class SingleIterator>
docid_t PositionIteratorTyped<SingleIterator>::InnerSeekDoc(docid_t docId)
{
    docid_t docRet = INVALID_DOCID;
    auto ec = InnerSeekDoc(docId, docRet);
    common::ThrowIfError(ec);
    return docRet;
}

template<class SingleIterator>
common::ErrorCode PositionIteratorTyped<SingleIterator>::InnerSeekDoc(docid_t docId, docid_t& result)
{
    docid_t docRet = INVALID_DOCID;
    for (;;)
    {
        SingleIteratorType* singleIterator = mSingleIterators[mSegmentCursor];
        common::ErrorCode ec = singleIterator->SeekDocWithErrorCode(docId, docRet);
        if (unlikely(ec != common::ErrorCode::OK))
        {
            return ec;
        }

        if(docRet != INVALID_DOCID)
        {
            result = docRet;
            return common::ErrorCode::OK;
        }

        auto ret = MoveToNextSegment();
        if (unlikely(!ret.Ok()))
        {
            return ret.GetErrorCode();
        }

        if (!ret.Value())
        {
            break;
        }
    }
    result = INVALID_DOCID;
    return common::ErrorCode::OK;
}

template<class SingleIterator>
docid_t PositionIteratorTyped<SingleIterator>::SeekDoc(docid_t docId)
{
    return InnerSeekDoc(docId);
}

template<class SingleIterator>
common::ErrorCode PositionIteratorTyped<SingleIterator>::SeekDocWithErrorCode(docid_t docId, docid_t& result)
{
    return InnerSeekDoc(docId, result);
}

template<class SingleIterator>
common::ErrorCode PositionIteratorTyped<SingleIterator>::SeekPositionWithErrorCode(pos_t pos, pos_t& result)
{
    return mSingleIterators[mSegmentCursor]->SeekPositionWithErrorCode(pos, result);
}

template<class SingleIterator>
docpayload_t PositionIteratorTyped<SingleIterator>::GetDocPayload()
{
    return mSingleIterators[mSegmentCursor]->GetDocPayload();
}

template<class SingleIterator>
void PositionIteratorTyped<SingleIterator>::Unpack(TermMatchData& termMatchData)
{
    mSingleIterators[mSegmentCursor]->Unpack(termMatchData);
}

template<class SingleIterator>
pospayload_t PositionIteratorTyped<SingleIterator>::GetPosPayload() const
{
    return mSingleIterators[mSegmentCursor]->GetPosPayload();
}

template<class SingleIterator>
PostingIteratorType PositionIteratorTyped<SingleIterator>::GetType() const
{
    return SingleIterator::GetType();
}

template<class SingleIterator>
inline SingleIterator* PositionIteratorTyped<SingleIterator>::CreateSingleIterator(uint8_t compressMode) const
{
    return IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool, SingleIterator,
            compressMode, mOptionFlag, mSessionPool);
}

template<>
inline SingleBitmapPostingIterator* 
PositionIteratorTyped<SingleBitmapPostingIterator>::CreateSingleIterator(
        uint8_t compressMode) const
{
    return IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool,
            SingleBitmapPostingIterator, mOptionFlag);
}

template <typename SingleIterator>
inline common::Result<bool> PositionIteratorTyped<SingleIterator>::MoveToNextSegment()
{
    if ((size_t)mSegmentCursor != mSegmentPostings->size() - 1)
    {
        ++mSegmentCursor;
        docid_t currentBaseDocId = (*mSegmentPostings)[mSegmentCursor].GetBaseDocId();
        SingleIteratorType* singleIterator = CreateSingleIterator(
                (*mSegmentPostings)[mSegmentCursor].GetCompressMode());
        singleIterator->SetBaseDocId(currentBaseDocId);
        common::ErrorCode ec = InitSingleIterator(singleIterator);
        if (unlikely(ec != common::ErrorCode::OK))
        {
            return false;
        }        
        mSingleIterators.push_back(singleIterator);

        return true;
    }

    return false;
}

template <>
inline common::Result<bool> PositionIteratorTyped<SingleBitmapPostingIterator>::MoveToNextSegment()
{
    if ((size_t)mSegmentCursor != mSegmentPostings->size() - 1)
    {
        ++mSegmentCursor;
        docid_t currentBaseDocId = (*mSegmentPostings)[mSegmentCursor].GetBaseDocId();
        SingleIteratorType* singleIterator = CreateSingleIterator(
                (*mSegmentPostings)[mSegmentCursor].GetCompressMode());
        singleIterator->SetBaseDocId(currentBaseDocId);
        common::ErrorCode ec = InitSingleIterator(singleIterator);
        if (unlikely(ec != common::ErrorCode::OK))
        {
            return false;
        }
        mSingleIterators.push_back(singleIterator);

        return true;
    }

    return false;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PACKAGE_TEXT_POSITION_ITERATOR_H
