
#include "indexlib/index/normal/inverted_index/format/skiplist/in_mem_tri_value_skip_list_reader.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemTriValueSkipListReader);

InMemTriValueSkipListReader::InMemTriValueSkipListReader(Pool *sessionPool) 
    : mSessionPool(sessionPool)
    , mSkipListBuffer(NULL)
{
}

InMemTriValueSkipListReader::~InMemTriValueSkipListReader() 
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mSkipListBuffer);
}

void InMemTriValueSkipListReader::Load(BufferedByteSlice* postingBuffer)
{
    mSkippedItemCount = -1;
    mCurrentDocId = 0;
    mCurrentOffset = 0;
    mCurrentTTF = 0;
    mPrevDocId = 0;
    mPrevOffset = 0;
    mPrevTTF = 0;
    mCurrentCursor = 0;
    mNumInBuffer = 0;

    BufferedByteSlice* skipListBuffer = IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool,
            BufferedByteSlice, mSessionPool, mSessionPool);
    postingBuffer->SnapShot(skipListBuffer);
    mSkipListBuffer = skipListBuffer;
    mSkipListReader.Open(mSkipListBuffer);
}

bool InMemTriValueSkipListReader::LoadBuffer()
{
    size_t flushCount = mSkipListBuffer->GetTotalCount();
    FlushInfo flushInfo = mSkipListBuffer->GetFlushInfo();

    size_t decodeCount = SKIP_LIST_BUFFER_SIZE;
    // TODO: skip list reader should not use flushInfo.IsValidShortBuffer()
    if (flushInfo.GetCompressMode() == SHORT_LIST_COMPRESS_MODE
        && flushInfo.IsValidShortBuffer() == false)
    {
        decodeCount = flushCount;
    }
    if (decodeCount == 0)
    {
        return false;
    }

    size_t keyNum = 0;
    if (!mSkipListReader.Decode(mDocIdBuffer, decodeCount, keyNum))
    {
        return false;
    }

    size_t ttfNum = 0;
    if (!mSkipListReader.Decode(mTTFBuffer, decodeCount, ttfNum))
    {
        return false;
    }

    size_t valueNum = 0;
    if (!mSkipListReader.Decode(mOffsetBuffer, decodeCount, valueNum))
    {
        return false;
    }

    if (keyNum != ttfNum || ttfNum != valueNum)
    {
        stringstream ss;
        ss << "SKipList decode error,"
           << "keyNum = " << keyNum
           << "ttfNum = " << ttfNum
           << "offsetNum = " << valueNum;
        INDEXLIB_THROW(misc::IndexCollapsedException, "%s", ss.str().c_str());
    }
    mNumInBuffer = keyNum;
    mCurrentCursor = 0;
    return true;
}

uint32_t InMemTriValueSkipListReader::GetLastValueInBuffer() const
{
    uint32_t lastValueInBuffer = mCurrentOffset;
    uint32_t currentCursor = mCurrentCursor;
    while (currentCursor < mNumInBuffer)
    {
        lastValueInBuffer += mOffsetBuffer[currentCursor];
        currentCursor++;
    }
    return lastValueInBuffer;
}

uint32_t InMemTriValueSkipListReader::GetLastKeyInBuffer() const
{
    uint32_t lastKeyInBuffer = mCurrentDocId;
    uint32_t currentCursor = mCurrentCursor;
    while (currentCursor < mNumInBuffer)
    {
        lastKeyInBuffer += mDocIdBuffer[currentCursor];
        currentCursor++;
    }
    return lastKeyInBuffer;
}


IE_NAMESPACE_END(index);

