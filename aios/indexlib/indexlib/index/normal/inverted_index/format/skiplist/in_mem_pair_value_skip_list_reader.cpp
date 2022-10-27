
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/in_mem_pair_value_skip_list_reader.h"
#include <sstream>

using namespace std;
IE_NAMESPACE_USE(common);


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemPairValueSkipListReader);

InMemPairValueSkipListReader::InMemPairValueSkipListReader(
        autil::mem_pool::Pool *sessionPool,
        bool isReferenceCompress)
    : PairValueSkipListReader(isReferenceCompress)
    , mSessionPool(sessionPool)
    , mSkipListBuffer(NULL)
{
}

InMemPairValueSkipListReader::~InMemPairValueSkipListReader()
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mSkipListBuffer);
}

void InMemPairValueSkipListReader::Load(
        index::BufferedByteSlice* postingBuffer)
{
    mSkippedItemCount = -1;
    mCurrentKey = 0;
    mCurrentValue = 0;
    mPrevKey = 0;
    mPrevValue = 0;
    mCurrentCursor = 0;
    mNumInBuffer = 0;

    BufferedByteSlice* skipListBuffer = IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool,
            BufferedByteSlice, mSessionPool, mSessionPool);
    postingBuffer->SnapShot(skipListBuffer);

    mSkipListBuffer = skipListBuffer;
    mSkipListReader.Open(mSkipListBuffer);
}

bool InMemPairValueSkipListReader::LoadBuffer()
{
    size_t keyNum = 0;
    size_t flushCount = mSkipListBuffer->GetTotalCount();
    size_t decodeCount = ShortListOptimizeUtil::IsShortSkipList(flushCount) ? 
                         flushCount : SKIP_LIST_BUFFER_SIZE;
    if (!mSkipListReader.Decode(mKeyBuffer, decodeCount, keyNum))
    {
        return false;
    }
    size_t valueNum = 0;
    if (!mSkipListReader.Decode(mValueBuffer, decodeCount, valueNum))
    {
        return false;
    }
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

IE_NAMESPACE_END(index);

