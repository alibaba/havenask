#include "indexlib/index/normal/inverted_index/format/skiplist/tri_value_skip_list_reader.h"
#include "indexlib/common/numeric_compress/encoder_provider.h"
#include <sstream>

using namespace std;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TriValueSkipListReader);

TriValueSkipListReader::TriValueSkipListReader(bool isReferenceCompress)
    : mCurrentDocId(0)
    , mCurrentOffset(0)
    , mCurrentTTF(0)
    , mPrevDocId(0)
    , mPrevOffset(0)
    , mPrevTTF(0)
    , mCurrentCursor(0)
    , mNumInBuffer(0)
{
    assert(!isReferenceCompress);
}

TriValueSkipListReader::TriValueSkipListReader(
        const TriValueSkipListReader& other)
    : mCurrentDocId(other.mCurrentDocId)
    , mCurrentOffset(other.mCurrentOffset)
    , mCurrentTTF(other.mCurrentTTF)
    , mPrevDocId(other.mPrevDocId)
    , mPrevOffset(other.mPrevOffset)
    , mPrevTTF(other.mPrevTTF)
    , mCurrentCursor(0)
    , mNumInBuffer(0)
{
}

TriValueSkipListReader::~TriValueSkipListReader()
{
}

void TriValueSkipListReader::Load(
        const util::ByteSliceList* byteSliceList,
        uint32_t start, uint32_t end, const uint32_t &itemCount)
{
    SkipListReader::Load(byteSliceList, start, end);
    mSkippedItemCount = -1;
    mCurrentDocId = 0;
    mCurrentOffset = 0;
    mCurrentTTF = 0;
    mPrevDocId = 0;
    mPrevOffset = 0;
    mPrevTTF = 0;
    mCurrentCursor = 0;
    mNumInBuffer = 0;

    if (start >= end)
    {
        return;
    }
    if (itemCount <= MAX_UNCOMPRESSED_SKIP_LIST_SIZE)
    {
        mByteSliceReader.Read(mDocIdBuffer, itemCount * sizeof(mDocIdBuffer[0]));
        mByteSliceReader.Read(mTTFBuffer, (itemCount - 1) * sizeof(mTTFBuffer[0]));
        mByteSliceReader.Read(mOffsetBuffer, (itemCount - 1) * sizeof(mOffsetBuffer[0]));
        
        mNumInBuffer = itemCount;
        assert(mEnd == mByteSliceReader.Tell());
    }
}

bool TriValueSkipListReader::SkipTo(uint32_t queryDocId, uint32_t& docId, 
        uint32_t& prevDocId, uint32_t& offset, uint32_t& delta)
{
    assert(mCurrentDocId <= queryDocId);

    int32_t skippedItemCount = mSkippedItemCount;
    uint32_t currentCursor = mCurrentCursor;
    uint32_t currentDocId = mCurrentDocId;
    uint32_t currentOffset = mCurrentOffset;
    uint32_t currentTTF = mCurrentTTF;
    uint32_t numInBuffer = mNumInBuffer;

    uint32_t localPrevDocId = mPrevDocId;
    uint32_t localPrevOffset = mPrevOffset;
    uint32_t localPrevTTF = mPrevTTF;

    while (true)
    {
        // TODO: skippedItemCount should not add after skipto failed
        skippedItemCount++;

        localPrevDocId = currentDocId;
        localPrevOffset = currentOffset;
        localPrevTTF = currentTTF;

        if (currentCursor >= numInBuffer)
        {
            if (!LoadBuffer())
            {
                break;
            }
            numInBuffer = mNumInBuffer;
            currentCursor = mCurrentCursor;
        }
        currentDocId += mDocIdBuffer[currentCursor];
        currentOffset += mOffsetBuffer[currentCursor];
        currentTTF += mTTFBuffer[currentCursor];

        currentCursor++;

        if (currentDocId >= queryDocId)
        {
            docId = currentDocId;
            prevDocId = mPrevDocId = localPrevDocId;
            offset = mPrevOffset = localPrevOffset;
            delta = currentOffset - localPrevOffset;
            mPrevTTF = localPrevTTF;

            mCurrentDocId = currentDocId;
            mCurrentCursor = currentCursor;
            mCurrentOffset = currentOffset;
            mCurrentTTF = currentTTF;
            mSkippedItemCount = skippedItemCount;
            return true;
        }
    }

    mCurrentDocId = currentDocId;
    mCurrentTTF = currentTTF;
    mCurrentOffset = currentOffset;
    mCurrentCursor = currentCursor;
    mSkippedItemCount = skippedItemCount;
    return false;
}

bool TriValueSkipListReader::LoadBuffer()
{
    if (mByteSliceReader.Tell() < mEnd)
    {
        const Int32Encoder* docIdEncoder =
            EncoderProvider::GetInstance()->GetSkipListEncoder();
        uint32_t docNum = docIdEncoder->Decode((uint32_t*)mDocIdBuffer, 
                sizeof(mDocIdBuffer) / sizeof(mDocIdBuffer[0]),
                mByteSliceReader);
        
        const Int32Encoder* tfEncoder =
            EncoderProvider::GetInstance()->GetSkipListEncoder();
        uint32_t ttfNum = tfEncoder->Decode(mTTFBuffer,
                sizeof(mTTFBuffer) / sizeof(mTTFBuffer[0]), mByteSliceReader);

        const Int32Encoder* offsetEncoder =
            EncoderProvider::GetInstance()->GetSkipListEncoder();
        uint32_t lenNum = offsetEncoder->Decode(mOffsetBuffer,
                sizeof(mOffsetBuffer) / sizeof(mOffsetBuffer[0]),
                mByteSliceReader);
        
        if (docNum != ttfNum || ttfNum != lenNum)
        {
            stringstream ss;
            ss << "TriValueSKipList decode error,"
               << "docNum = " << docNum
               << "ttfNum = " << ttfNum
               << "offsetNum = " << lenNum;
            INDEXLIB_THROW(misc::IndexCollapsedException, "%s", ss.str().c_str());
        }
        mNumInBuffer = docNum;
        mCurrentCursor = 0;        
        return true;
    }
    return false;
}


IE_NAMESPACE_END(index);

