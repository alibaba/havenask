#include "indexlib/index/normal/inverted_index/format/skiplist/pair_value_skip_list_reader.h"
#include "indexlib/common/numeric_compress/encoder_provider.h"
#include <sstream>

using namespace std;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PairValueSkipListReader);

PairValueSkipListReader::PairValueSkipListReader(bool isReferenceCompress)
    : mCurrentKey(0)
    , mCurrentValue(0)
    , mPrevKey(0)
    , mPrevValue(0)
    , mCurrentCursor(0)
    , mNumInBuffer(0)
    , mKeyBufferBase(mKeyBuffer)
    , mValueBufferBase(mValueBuffer)
    , mIsReferenceCompress(isReferenceCompress)
{
}

PairValueSkipListReader::PairValueSkipListReader(const PairValueSkipListReader& other)
    : mCurrentKey(other.mCurrentKey)
    , mCurrentValue(other.mCurrentValue)
    , mPrevKey(other.mPrevKey)
    , mPrevValue(other.mPrevValue)
    , mCurrentCursor(0)
    , mNumInBuffer(0)
    , mKeyBufferBase(mKeyBuffer)
    , mValueBufferBase(mValueBuffer)
    , mIsReferenceCompress(other.mIsReferenceCompress)
{
}

PairValueSkipListReader::~PairValueSkipListReader()
{
}

void PairValueSkipListReader::Load(const util::ByteSliceList* byteSliceList,
                                uint32_t start, uint32_t end, const uint32_t& itemCount)
{
    SkipListReader::Load(byteSliceList, start, end);
    mSkippedItemCount = -1;
    mCurrentKey = 0;
    mCurrentValue = 0;
    mPrevKey = 0;
    mPrevValue = 0;
    mCurrentCursor = 0;
    mNumInBuffer = 0;
    mKeyBufferBase = mKeyBuffer;
    mValueBufferBase = mValueBuffer;
    if (start >= end)
    {
        return;
    }
    if (itemCount <= MAX_UNCOMPRESSED_SKIP_LIST_SIZE)
    {
        mByteSliceReader.Read(mKeyBuffer, itemCount * sizeof(mKeyBuffer[0]));
        mByteSliceReader.Read(mValueBuffer, itemCount * sizeof(mValueBuffer[0]));
        
        mNumInBuffer = itemCount;
        assert(mEnd == mByteSliceReader.Tell());
    }
}

bool PairValueSkipListReader::SkipTo(uint32_t queryKey, uint32_t& key, uint32_t& prevKey,
                                  uint32_t& value, uint32_t& delta)
{
    assert(mCurrentKey <= queryKey);

    uint32_t localPrevKey, localPrevValue;
    uint32_t currentKey = mCurrentKey;
    uint32_t currentValue = mCurrentValue;
    uint32_t currentCursor = mCurrentCursor;
    int32_t skippedItemCount = mSkippedItemCount;
    uint32_t numInBuffer = mNumInBuffer;
    while (true)
    {
        // TODO: skippedItemCount should not add after skipto failed
        skippedItemCount++;

        localPrevKey = currentKey;
        localPrevValue = currentValue;

        if (currentCursor >= numInBuffer)
        {
            if (!LoadBuffer())
            {
                break;
            }
            currentCursor = mCurrentCursor;
            numInBuffer = mNumInBuffer;
        }
        currentKey = mIsReferenceCompress ? 
                     mKeyBufferBase[currentCursor] : 
                     mKeyBufferBase[currentCursor] + currentKey; 
        currentValue += mValueBufferBase[currentCursor];
        currentCursor++;

        if (currentKey >= queryKey)
        {
            key = currentKey;
            prevKey = mPrevKey = localPrevKey;
            value = mPrevValue = localPrevValue;
            delta = currentValue - localPrevValue;
            mCurrentKey = currentKey;
            mCurrentValue = currentValue;
            mCurrentCursor = currentCursor;
            mSkippedItemCount = skippedItemCount;

            return true;
        }
    }

    mCurrentKey = currentKey;
    mCurrentValue = currentValue;
    mCurrentCursor = currentCursor;

    mSkippedItemCount = skippedItemCount;
    return false;
}

bool PairValueSkipListReader::LoadBuffer()
{
    uint32_t end = mByteSliceReader.Tell();
    if (end < mEnd)
    {
        CompressMode mode = mIsReferenceCompress ? 
                            REFERENCE_COMPRESS_MODE : PFOR_DELTA_COMPRESS_MODE;
        mKeyBufferBase = mKeyBuffer;
        const Int32Encoder* keyEncoder =
            EncoderProvider::GetInstance()->GetSkipListEncoder(mode);
        uint32_t keyNum = keyEncoder->DecodeMayCopy(mKeyBufferBase, 
                sizeof(mKeyBuffer) / sizeof(mKeyBuffer[0]), mByteSliceReader);
        
        mValueBufferBase = mValueBuffer;
        const Int32Encoder* offsetEncoder =
            EncoderProvider::GetInstance()->GetSkipListEncoder(mode);
        uint32_t valueNum = offsetEncoder->DecodeMayCopy(mValueBufferBase,
                sizeof(mValueBuffer) / sizeof(mValueBuffer[0]), mByteSliceReader);
        
        if (keyNum != valueNum)
        {
            stringstream ss;
            ss << "SKipList decode error,"
               << "keyNum = " << keyNum
               << "offsetNum = " << valueNum;
            INDEXLIB_THROW(misc::IndexCollapsedException, "%s", ss.str().c_str());
        }
        mNumInBuffer = keyNum;
        mCurrentCursor = 0;        
        return true;
    }
    return false;
}

uint32_t PairValueSkipListReader::GetLastValueInBuffer() const
{
    uint32_t lastValueInBuffer = mCurrentValue;
    uint32_t currentCursor = mCurrentCursor;
    while (currentCursor < mNumInBuffer)
    {
        lastValueInBuffer = mIsReferenceCompress ? 
                            mValueBufferBase[currentCursor] : 
                            mValueBufferBase[currentCursor] + lastValueInBuffer;
        currentCursor++;
    }
    return lastValueInBuffer;
}

uint32_t PairValueSkipListReader::GetLastKeyInBuffer() const
{
    uint32_t lastKeyInBuffer = mCurrentKey;
    uint32_t currentCursor = mCurrentCursor;
    while (currentCursor < mNumInBuffer)
    {
        lastKeyInBuffer = mIsReferenceCompress ? 
                          mKeyBufferBase[currentCursor] : 
                          mKeyBufferBase[currentCursor] + lastKeyInBuffer;
        currentCursor++;
    }
    return lastKeyInBuffer;
}

IE_NAMESPACE_END(index);

