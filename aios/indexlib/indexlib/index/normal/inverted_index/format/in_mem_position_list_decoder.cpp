
#include "indexlib/index/normal/inverted_index/format/in_mem_position_list_decoder.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"

using namespace std;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemPositionListDecoder);

InMemPositionListDecoder::InMemPositionListDecoder(
        const PositionListFormatOption &option, Pool* sessionPool)
    : PositionListSegmentDecoder(option, sessionPool)
    , mPosListBuffer(NULL)
{
}

InMemPositionListDecoder::~InMemPositionListDecoder() 
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mPosListBuffer);
}

void InMemPositionListDecoder::Init(
        ttf_t totalTF,
        PairValueSkipListReader* skipListReader,
        BufferedByteSlice* posListBuffer)
{
    assert(posListBuffer);

    mTotalTf = totalTF;
    mPosSkipListReader = skipListReader;
    mPosListBuffer = posListBuffer;
    mPosListReader.Open(posListBuffer);
    mDecodedPosCount = 0;
}

bool InMemPositionListDecoder::SkipTo(ttf_t currentTTF, 
                                      NormalInDocState* state)
{
    if (mTotalTf <= currentTTF)
    {
        return false;
    }

    if (!mPosSkipListReader)
    {
        assert(mTotalTf <= MAX_POS_PER_RECORD);
        state->SetRecordOffset(0);
        state->SetOffsetInRecord(currentTTF);
        return true;
    }

    if ((int32_t)mDecodedPosCount > currentTTF)
    {
        // skipTo only used with mState in in_doc_state_keeper
        // in same record, mState no need reset RecordOffset
        state->SetOffsetInRecord(currentTTF - mPreRecordTTF);
        return true;
    }
        
    uint32_t offset, len;
    if (mPosSkipListReader->SkipTo(currentTTF + 1,
                                   mDecodedPosCount, offset, len))
    {
        mPreRecordTTF = mPosSkipListReader->GetPrevKey();
    }
    else
    {
        offset = mPosSkipListReader->GetLastValueInBuffer();
        mPreRecordTTF = mPosSkipListReader->GetCurrentKey();
        mDecodedPosCount = mTotalTf;
    }

    state->SetRecordOffset(offset);
    state->SetOffsetInRecord(currentTTF - mPreRecordTTF);
    return true;
}

bool InMemPositionListDecoder::LocateRecord(const NormalInDocState* state,
                      uint32_t& termFrequence)
{
    assert(!mOption.HasTfBitmap());
    mRecordOffset = state->GetRecordOffset();
    mOffsetInRecord = state->GetOffsetInRecord();
    termFrequence = state->mTermFreq;

    if (!mNeedReopen && mLastDecodeOffset == mRecordOffset)
    {
        // no need to relocate
        return false;
    }

    if (mNeedReopen || mLastDecodeOffset > mRecordOffset)
    {
        mPosListReader.Open(mPosListBuffer);
        mLastDecodeOffset = 0;
    }
    mPosListReader.Seek(mRecordOffset);
    mNeedReopen = false;
    return true;
}

uint32_t InMemPositionListDecoder::DecodeRecord(
        pos_t* posBuffer, uint32_t posBufferLen,
        pospayload_t* payloadBuffer, uint32_t payloadBufferLen)
{
    mLastDecodeOffset = mPosListReader.Tell();

    size_t posCount = 0;
    bool ret = mPosListReader.Decode(posBuffer, posBufferLen, posCount);

    if (mOption.HasPositionPayload())
    {
        size_t payloadCount = 0;
        ret &= mPosListReader.Decode(payloadBuffer, 
                payloadBufferLen, payloadCount);
        ret &= (payloadCount == posCount);
    }

    if (!ret)
    {
        INDEXLIB_THROW(misc::IndexCollapsedException, "Pos payload list collapsed.");
    }

    return posCount;
}

IE_NAMESPACE_END(index);

