#include "indexlib/index/normal/inverted_index/format/position_list_segment_decoder.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/common/numeric_compress/encoder_provider.h"


using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PositionListSegmentDecoder);

PositionListSegmentDecoder::PositionListSegmentDecoder(
        const PositionListFormatOption &option, Pool* sessionPool)
    : mPosSkipListReader(NULL)
    , mSessionPool(sessionPool)
    , mPosEncoder(NULL)
    , mPosPayloadEncoder(NULL)
    , mBitmapReader(NULL)
    , mPosBitmapBlockBuffer(NULL)
    , mPosBitmapBlockCount(0)
    , mTotalTf(0)
    , mDecodedPosCount(0)
    , mRecordOffset(0)
    , mPreRecordTTF(0)
    , mOffsetInRecord(0)
    , mPosListBegin(0)
    , mLastDecodeOffset(0)
    , mOption(option)
    , mOwnPosBitmapBlockBuffer(false)
    , mNeedReopen(true)
{
}

PositionListSegmentDecoder::~PositionListSegmentDecoder() 
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mPosSkipListReader);
    IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mBitmapReader);
    if (mOwnPosBitmapBlockBuffer)
    {
        IE_POOL_COMPATIBLE_DELETE_VECTOR(mSessionPool, mPosBitmapBlockBuffer,
                mPosBitmapBlockCount);
    }
}

void PositionListSegmentDecoder::InitPositionBitmapReader(
        const ByteSliceList* posList, uint32_t& posListCursor,
        NormalInDocState* state)
{
    uint32_t bitmapListBegin = posListCursor;
    mBitmapReader = IE_POOL_COMPATIBLE_NEW_CLASS(
            mSessionPool, PositionBitmapReader);
    mBitmapReader->Init(posList, bitmapListBegin);
    posListCursor += mBitmapReader->GetBitmapListSize();

}

void PositionListSegmentDecoder::InitPositionBitmapBlockBuffer(
        ByteSliceReader& posListReader,
        tf_t totalTF,
        uint32_t posSkipListStart,
        uint32_t posSkipListLen,
        NormalInDocState* state)
{
    mPosBitmapBlockCount = posSkipListLen / sizeof(uint32_t);
    mPosBitmapBlockBuffer = IE_POOL_COMPATIBLE_NEW_VECTOR(
            mSessionPool, uint32_t, mPosBitmapBlockCount);

    void *tmpPtr = mPosBitmapBlockBuffer;
    posListReader.ReadMayCopy(tmpPtr, posSkipListLen);
    if (tmpPtr == mPosBitmapBlockBuffer)
    {
        mOwnPosBitmapBlockBuffer = true;
    }
    else
    {
        mOwnPosBitmapBlockBuffer = false;
        IE_POOL_COMPATIBLE_DELETE_VECTOR(mSessionPool, mPosBitmapBlockBuffer,
                mPosBitmapBlockCount);
        mPosBitmapBlockBuffer = (uint32_t*)tmpPtr;
    }

}

void PositionListSegmentDecoder::InitPositionSkipList(
        const ByteSliceList* posList,
        tf_t totalTF,
        uint32_t posSkipListStart,
        uint32_t posSkipListLen,
        NormalInDocState* state)
{
    uint32_t posSkipListEnd = posSkipListStart + posSkipListLen;
    if (ShortListOptimizeUtil::IsShortPosList(totalTF))
    {
        mDecodedPosCount = totalTF;
        state->SetRecordOffset(posSkipListEnd);
    }
    else
    {
        mPosSkipListReader = IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool, 
                PairValueSkipListReader);
        mPosSkipListReader->Load(posList, posSkipListStart, posSkipListEnd,
                (totalTF - 1) / MAX_POS_PER_RECORD + 1);
        mDecodedPosCount = 0;
    }
}

void PositionListSegmentDecoder::Init(
        const ByteSliceList* posList, tf_t totalTF,
        uint32_t posListBegin, uint8_t compressMode,
        NormalInDocState* state)
{
    mTotalTf = totalTF;
    mPosEncoder = EncoderProvider::GetInstance()->GetPosListEncoder(
            compressMode);
    mPosPayloadEncoder = EncoderProvider::GetInstance()->GetPosPayloadEncoder(
            compressMode);

    if (mOption.HasTfBitmap())
    {
        InitPositionBitmapReader(posList, posListBegin, state);
    }

    if (!mOption.HasPositionList())
    {
        return;
    }

    mPosListReader.Open(const_cast<ByteSliceList*>(posList));
    mPosListReader.Seek(posListBegin);
    uint32_t posSkipListLen = mPosListReader.ReadVUInt32();
    // read pos list length
    mPosListReader.ReadVUInt32();

    uint32_t posSkipListStart = mPosListReader.Tell();
    mPosListBegin = posSkipListStart + posSkipListLen;

    if (mOption.HasTfBitmap())
    {
        InitPositionBitmapBlockBuffer(mPosListReader, totalTF,
                posSkipListStart, posSkipListLen, state);
    }
    else
    {
        InitPositionSkipList(posList, totalTF, posSkipListStart, 
                             posSkipListLen, state);
    }

    mPosListReader.Seek(mPosListBegin);
    state->SetCompressMode(ShortListOptimizeUtil::GetPosCompressMode(compressMode));
}

bool PositionListSegmentDecoder::SkipTo(
        ttf_t currentTTF, NormalInDocState* state)
{
    if (!mOption.HasPositionList() || mOption.HasTfBitmap())
    {
        return true;
    }

    if ((int32_t)mDecodedPosCount > currentTTF)
    {
        state->SetOffsetInRecord(currentTTF - mPreRecordTTF);
        return true;
    }

    uint32_t offset, len;
    if (mPosSkipListReader == NULL || !mPosSkipListReader->SkipTo(
                    currentTTF + 1, mDecodedPosCount, offset, len))
    {
        return false;
    }
    state->SetRecordOffset(offset + mPosListBegin);
    mPreRecordTTF = mPosSkipListReader->GetPrevKey();
    state->SetOffsetInRecord(currentTTF - mPreRecordTTF);
    return true;
}

bool PositionListSegmentDecoder::LocateRecord(const index::NormalInDocState* state,
        uint32_t& termFrequence)
{
    uint32_t recordOffset;
    int32_t offsetInRecord;

    if (mOption.HasTfBitmap())
    {
        CalculateRecordOffsetByPosBitmap(state, termFrequence,
                recordOffset, offsetInRecord);
    }
    else
    {
        recordOffset = state->GetRecordOffset();
        offsetInRecord = state->GetOffsetInRecord();
        termFrequence = state->mTermFreq;
    }
    mRecordOffset = recordOffset;
    mOffsetInRecord = offsetInRecord;

    ByteSliceList* positionList = mPosListReader.GetByteSliceList();
    if (!mNeedReopen && mLastDecodeOffset == mRecordOffset)
    {
        // no need to relocate
        return false;
    }

    if (mNeedReopen || mLastDecodeOffset > mRecordOffset)
    {
        mPosListReader.Open(positionList);
        mLastDecodeOffset = 0;
    }
    mPosListReader.Seek(mRecordOffset);
    mNeedReopen = false;
    return true;
}

void PositionListSegmentDecoder::CalculateRecordOffsetByPosBitmap(
        const NormalInDocState *state,
        uint32_t& termFrequence,
        uint32_t& recordOffset,
        int32_t& offsetInRecord)
{
    assert(mBitmapReader);
    PosCountInfo info = mBitmapReader->GetPosCountInfo(
            state->GetSeekedDocCount());
    termFrequence = info.currentDocPosCount;
    uint32_t preAggPosCount = info.preDocAggPosCount;
    recordOffset = mRecordOffset;
    uint32_t decodeBlockPosCount = (mDecodedPosCount & MAX_POS_PER_RECORD_MASK);
    if (likely(decodeBlockPosCount == 0))
    {
        decodeBlockPosCount = MAX_POS_PER_RECORD;
    }

    if (preAggPosCount >= mDecodedPosCount 
        || preAggPosCount < mDecodedPosCount - decodeBlockPosCount)
    {
        uint32_t blockIndex = preAggPosCount
                              >> MAX_POS_PER_RECORD_BIT_NUM;
        uint32_t offset = blockIndex == 0 ? 0 
                          : mPosBitmapBlockBuffer[blockIndex - 1];
        mDecodedPosCount = blockIndex == mPosBitmapBlockCount ? mTotalTf
                           : (blockIndex + 1) << MAX_POS_PER_RECORD_BIT_NUM;
        decodeBlockPosCount = (mDecodedPosCount & MAX_POS_PER_RECORD_MASK);
        if (likely(decodeBlockPosCount == 0))
        {
            decodeBlockPosCount = MAX_POS_PER_RECORD;
        }
        recordOffset = mPosListBegin + offset;
    }
    offsetInRecord = preAggPosCount - (mDecodedPosCount - decodeBlockPosCount);
}

uint32_t PositionListSegmentDecoder::DecodeRecord(
        pos_t* posBuffer, uint32_t posBufferLen,
        pospayload_t* payloadBuffer, uint32_t payloadBufferLen)
{
    mLastDecodeOffset = mPosListReader.Tell();

    uint32_t posCount = mPosEncoder->Decode(posBuffer, 
            posBufferLen, mPosListReader);

    if (mOption.HasPositionPayload())
    {
        uint32_t payloadCount = mPosPayloadEncoder->Decode(
                payloadBuffer, payloadBufferLen, mPosListReader);
        if (payloadCount != posCount)
        {
            INDEXLIB_THROW(misc::IndexCollapsedException, "Pos payload list collapsed.");
        }
    }
    return posCount;
}


IE_NAMESPACE_END(index);
