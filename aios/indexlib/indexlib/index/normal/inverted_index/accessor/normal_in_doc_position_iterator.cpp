#include "indexlib/index/normal/inverted_index/accessor/normal_in_doc_position_iterator.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index/normal/inverted_index/format/position_list_segment_decoder.h"

using namespace autil::legacy;

IE_NAMESPACE_USE(common);


IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, NormalInDocPositionIterator);

NormalInDocPositionIterator::NormalInDocPositionIterator(
        PositionListFormatOption option)
    : mCurrentPos(-1)
    , mVisitedPosInBuffer(-1)
    , mVisitedPosInDoc(-1)
    , mPosCountInBuffer(0)
    , mOffsetInRecord(0)
    , mTotalPosCount(0)
    , mSectionMeta(NULL)
    , mVisitedSectionInDoc(-1)
    , mVisitedSectionLen(0)
    , mCurrentSectionId(INVALID_SECID)
    , mCurrentFieldId(-1)
    , mOption(option)
{
}

NormalInDocPositionIterator::~NormalInDocPositionIterator() 
{
    DELETE_AND_SET_NULL(mSectionMeta);
}

void NormalInDocPositionIterator::Init(const NormalInDocState& state)
{
    PositionListSegmentDecoder* posSegDecoder = 
        state.GetPositionListSegmentDecoder();
    assert(posSegDecoder);
    
    if (mState.GetPositionListSegmentDecoder() != posSegDecoder)
    {
        posSegDecoder->SetNeedReopen(true);
    }

    bool needUnpack = posSegDecoder->LocateRecord(&state, mTotalPosCount);
    mOffsetInRecord = posSegDecoder->GetOffsetInRecord();

    mState = state;

    if (needUnpack)
    {
        mVisitedPosInBuffer = -1;
        mPosCountInBuffer = 0;
    }
    else 
    {
        mVisitedPosInBuffer = mOffsetInRecord - 1;
        mPosBuffer[mVisitedPosInBuffer + 1] += 1;
    }

    mCurrentPos = -1;
    mVisitedPosInDoc = 0;
    mCurrentFieldId = -1;

    mVisitedSectionInDoc = -1;
    mVisitedSectionLen = 0;
    mCurrentSectionId = INVALID_SECID;
}

ErrorCode NormalInDocPositionIterator::SeekPositionWithErrorCode(pos_t pos, pos_t& result)
{
    pos = pos >= (mCurrentPos + 1) ? pos : (mCurrentPos + 1);
    while (pos > mCurrentPos)
    {
        if (++mVisitedPosInDoc > (int32_t)mTotalPosCount)
        {
            result = INVALID_POSITION;
            return common::ErrorCode::OK;
        }

        if (++mVisitedPosInBuffer >= mPosCountInBuffer)
        {
            PositionListSegmentDecoder* posSegDecoder = 
                mState.GetPositionListSegmentDecoder();
            assert(posSegDecoder);
            try
            {
                mPosCountInBuffer = posSegDecoder->DecodeRecord(mPosBuffer,
                        MAX_POS_PER_RECORD, mPosPayloadBuffer, MAX_POS_PER_RECORD);
            }
            catch (const misc::FileIOException& e)
            {
                return common::ErrorCode::FileIO;
            }


            if (mVisitedPosInBuffer == 0)
            {
                mVisitedPosInBuffer = mOffsetInRecord;
                mPosBuffer[mVisitedPosInBuffer] += 1;
            }
            else 
            {
                mVisitedPosInBuffer = 0;
            }
        }
        mCurrentPos += mPosBuffer[mVisitedPosInBuffer];
    }
    result = mCurrentPos;
    return common::ErrorCode::OK;
}

pospayload_t NormalInDocPositionIterator::GetPosPayload()
{
    if (HasPosPayload())
    {
        return mPosPayloadBuffer[mVisitedPosInBuffer];
    }
    return 0;
}

sectionid_t NormalInDocPositionIterator::GetSectionId() 
{
    if (!mState.GetSectionReader())
    {
        return 0;
    }
    SeekSectionMeta();
    return mCurrentSectionId;
}

section_len_t NormalInDocPositionIterator::GetSectionLength() 
{
    if (!mState.GetSectionReader())
    {
        return 0;
    }
    SeekSectionMeta();
    return (section_len_t)mSectionMeta->GetSectionLen(mVisitedSectionInDoc);
}

fieldid_t NormalInDocPositionIterator::GetFieldId() 
{
    if (!mState.GetSectionReader())
    {
        return INVALID_FIELDID;
    }
    SeekSectionMeta();
    // NOTE: mCurrentFieldId is fieldIdInPack, convert to real fieldid_t
    return mState.GetFieldId(mCurrentFieldId);
}

section_weight_t NormalInDocPositionIterator::GetSectionWeight() 
{
    if (!mState.GetSectionReader())
    {
        return 0;
    }
    SeekSectionMeta();
    return mSectionMeta->GetSectionWeight(mVisitedSectionInDoc);
}

int32_t NormalInDocPositionIterator::GetFieldPosition() 
{
    if (!mState.GetSectionReader())
    {
        return INVALID_FIELDID;
    }
    SeekSectionMeta();
    return mCurrentFieldId;
}

IE_NAMESPACE_END(index);

