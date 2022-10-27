#ifndef __INDEXLIB_IN_DOC_POSITION_ITERATOR_STATE_H
#define __INDEXLIB_IN_DOC_POSITION_ITERATOR_STATE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/inverted_index/accessor/position_bitmap_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/in_doc_position_state.h"

DECLARE_REFERENCE_CLASS(index, PositionListSegmentDecoder);
DECLARE_REFERENCE_CLASS(index, InMemPositionListDecoder);

IE_NAMESPACE_BEGIN(index);

class InDocPositionIteratorState : public InDocPositionState
{
public:
    InDocPositionIteratorState(
            uint8_t compressMode = PFOR_DELTA_COMPRESS_MODE, 
            const index::PositionListFormatOption& option = 
            index::PositionListFormatOption())
        : InDocPositionState(option)
        , mPosSegDecoder(NULL)
        , mRecordOffset(0)
        , mOffsetInRecord(0)
        , mTotalPosCount(0)
        , mCompressMode(compressMode)
    {}

    InDocPositionIteratorState(const InDocPositionIteratorState& other)
        : InDocPositionState(other)
        , mPosSegDecoder(other.mPosSegDecoder)
        , mRecordOffset(other.mRecordOffset)
        , mOffsetInRecord(other.mOffsetInRecord)
        , mTotalPosCount(other.mTotalPosCount)
        , mCompressMode(other.mCompressMode)
    {}

    virtual ~InDocPositionIteratorState() {}

public:
    tf_t GetTermFreq() override;
    
    void SetPositionListSegmentDecoder(PositionListSegmentDecoder* posSegDecoder)
    {
        mPosSegDecoder = posSegDecoder;
    }
    
    PositionListSegmentDecoder* GetPositionListSegmentDecoder() const
    {
        return mPosSegDecoder;
    }
    
    void SetCompressMode(uint8_t compressMode)
    {
        mCompressMode = compressMode;
    }
    uint8_t GetCompressMode() const
    {
        return mCompressMode;
    }
    void SetRecordOffset(int32_t recordOffset)
    {
        mRecordOffset = recordOffset;
    }
    int32_t GetRecordOffset() const
    {
        return mRecordOffset;
    }
    void SetOffsetInRecord(int32_t offsetInRecord)
    {
        mOffsetInRecord = offsetInRecord;
    }
    int32_t GetOffsetInRecord() const
    {
        return mOffsetInRecord;
    }

    void SetTotalPosCount(uint32_t totalPosCount)
    {
        mTotalPosCount = totalPosCount;
    }
    uint32_t GetTotalPosCount() const
    {
        return mTotalPosCount;
    }
protected:
    void Clone(InDocPositionIteratorState* state) const;
protected:
    PositionListSegmentDecoder* mPosSegDecoder;
    int32_t mRecordOffset;
    int32_t mOffsetInRecord;
    uint32_t mTotalPosCount;
    uint8_t mCompressMode;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InDocPositionIteratorState);
//////////////////////////////////////////////////////////
inline void InDocPositionIteratorState::Clone(
        InDocPositionIteratorState* state) const
{
    InDocPositionState::Clone(state);
    state->mPosSegDecoder = mPosSegDecoder;
    state->mCompressMode = mCompressMode;
    state->mRecordOffset = mRecordOffset;
    state->mOffsetInRecord = mOffsetInRecord;
    state->mTotalPosCount = mTotalPosCount;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_DOC_POSITION_ITERATOR_STATE_H
