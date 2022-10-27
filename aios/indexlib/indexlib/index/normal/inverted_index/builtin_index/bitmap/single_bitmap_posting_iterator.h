#ifndef __INDEXLIB_SINGLE_BITMAP_POSTING_ITERATOR_H
#define __INDEXLIB_SINGLE_BITMAP_POSTING_ITERATOR_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"
#include "indexlib/index/normal/inverted_index/accessor/term_match_data.h"
#include "indexlib/util/bitmap.h"
#include "indexlib/util/object_pool.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_in_doc_position_state.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer.h"

IE_NAMESPACE_BEGIN(index);

class SingleBitmapPostingIterator 
{
public:
    typedef BitmapInDocPositionState InDocPositionStateType;

public:
    SingleBitmapPostingIterator(optionflag_t flag = NO_PAYLOAD);
    ~SingleBitmapPostingIterator();

public:
    void Init(const util::ByteSliceListPtr& sliceListPtr,
              util::ObjectPool<InDocPositionStateType>* statePool);

    // for realtime segment init
    void Init(const PostingWriter* postingWriter,
              util::ObjectPool<InDocPositionStateType>* statePool);

    inline docid_t SeekDoc(docid_t docId);
    inline common::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result);
    inline void Unpack(TermMatchData& termMatchData);
    inline bool HasPosition() const { return false;}

    inline pos_t SeekPosition(pos_t pos) { return INVALID_POSITION; }
    inline common::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t& nextPos)
    {
        nextPos = INVALID_POSITION;
        return common::ErrorCode::OK;
    }
    inline void SetBaseDocId(docid_t baseDocId) { mBaseDocId = baseDocId; }
    inline docid_t GetLastDocId() const { return mLastDocId; }
    inline bool Test(docid_t docId) 
    {
        docid_t localDocId = docId - mBaseDocId;
        if (mBitmap.Test(localDocId)) 
        {
            mCurrentLocalId = localDocId;
            return true;
        } 
        return false;
    }

    index::TermMeta *GetTermMeta() const {return const_cast<index::TermMeta*>(&mTermMeta);}
    docpayload_t GetDocPayload() const { return 0; } 

    tf_t GetTf() const { return 0; }

    static PostingIteratorType GetType() { return pi_bitmap; }

    void Reset();
public:
    // for test.
    void Init(const util::Bitmap& mBitmap, 
              index::TermMeta *termMeta,
              util::ObjectPool<InDocPositionStateType>* statePool);
    // for test
    docid_t GetCurrentGlobalDocId() 
    { return mCurrentLocalId + mBaseDocId; }
private:
    void SetStatePool(util::ObjectPool<InDocPositionStateType>* statePool)
    { mStatePool = statePool; }

private:
    uint32_t *mData;
    docid_t mCurrentLocalId;
    docid_t mBaseDocId;
    util::Bitmap mBitmap;
    docid_t mLastDocId;
    util::ObjectPool<InDocPositionStateType>* mStatePool;
    index::TermMeta mTermMeta;
    
private:
    friend class SingleBitmapPostingIteratorTest;
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<SingleBitmapPostingIterator> SingleBitmapPostingIteratorPtr;

inline common::ErrorCode SingleBitmapPostingIterator::SeekDocWithErrorCode(docid_t docId, docid_t& result)
{
    result = SeekDoc(docId);
    return common::ErrorCode::OK;
}

inline docid_t SingleBitmapPostingIterator::SeekDoc(docid_t docId)
{
    docId -= mBaseDocId;
    docId = std::max(mCurrentLocalId+1, docId);
    if (unlikely(docId >= (docid_t)mBitmap.GetItemCount()))
    {
        return INVALID_DOCID;
    }
    uint32_t blockId = docId / 32;
    uint32_t blockOffset = docId % 32;
    uint32_t data = mData[blockId];
    if (blockOffset)
    {
        data &= (0xffffffff >> blockOffset);
    }
    if (data)
    {
        int i = __builtin_clz(data);
        mCurrentLocalId = (blockId << 5) + i;
        return mCurrentLocalId + mBaseDocId;
    }
    uint32_t blockCount = mBitmap.GetSlotCount();
    while (true)
    {
        ++blockId;
        if (blockId >= blockCount)
        {
            break;
        }
        if (mData[blockId])
        {
            int i = __builtin_clz(mData[blockId]);
            mCurrentLocalId = (blockId << 5) + i;
            return mCurrentLocalId + mBaseDocId;
        }
    }
    return INVALID_DOCID;
}

inline  void SingleBitmapPostingIterator::Unpack(TermMatchData& termMatchData)
{
    InDocPositionStateType* state = mStatePool->Alloc();
    state->SetDocId(mCurrentLocalId + mBaseDocId);
    state->SetTermFreq(1);
    termMatchData.SetInDocPositionState(state);
}

inline void SingleBitmapPostingIterator::Reset()
{
    mCurrentLocalId = INVALID_DOCID;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLE_BITMAP_POSTING_ITERATOR_H
