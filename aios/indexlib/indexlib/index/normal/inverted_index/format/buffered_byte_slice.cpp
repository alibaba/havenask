#include "indexlib/index/normal/inverted_index/format/buffered_byte_slice.h"

using namespace std;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BufferedByteSlice);

BufferedByteSlice::BufferedByteSlice(Pool* byteSlicePool, Pool* bufferPool)
    : mBuffer(bufferPool)
    , mPostingListWriter(byteSlicePool)
{
}

BufferedByteSlice::~BufferedByteSlice() 
{
}

bool BufferedByteSlice::Init(const common::MultiValue* value)
{
    mBuffer.Init(value);
    return true;
}

size_t BufferedByteSlice::Flush(uint8_t compressMode)
{
    // TODO: used for async dump
    // bufferPool will be released in InMemorySegmentWriter, 
    // mBuffer is unreadable at this time
    if (mBuffer.Size() == 0)
    {
        return 0;
    }

    size_t flushSize = DoFlush(compressMode);
    MEMORY_BARRIER();

    FlushInfo tempFlushInfo;
    tempFlushInfo.SetFlushCount(mFlushInfo.GetFlushCount() + mBuffer.Size());
    tempFlushInfo.SetFlushLength(mFlushInfo.GetFlushLength() + flushSize);
    tempFlushInfo.SetCompressMode(compressMode);
    tempFlushInfo.SetIsValidShortBuffer(false);

    MEMORY_BARRIER();
    mFlushInfo = tempFlushInfo;
    MEMORY_BARRIER();

    mBuffer.Clear();
    return flushSize;
}

size_t BufferedByteSlice::DoFlush(uint8_t compressMode)
{
    uint32_t flushSize = 0;
    const AtomicValueVector& atomicValues = GetMultiValue()->GetAtomicValues();

    for (size_t i = 0; i < atomicValues.size(); ++i)
    {
        AtomicValue* atomicValue = atomicValues[i];
        uint8_t* buffer = mBuffer.GetRow(atomicValue->GetLocation());
        flushSize += atomicValue->Encode(compressMode, mPostingListWriter, 
                buffer, mBuffer.Size() * atomicValue->GetSize());
    }

    return flushSize;
}

void BufferedByteSlice::SnapShot(BufferedByteSlice* buffer) const 
{
    buffer->Init(GetMultiValue());

    SnapShotByteSlice(buffer);
    mBuffer.SnapShot(buffer->mBuffer);

    if (mFlushInfo.GetFlushLength() > buffer->mFlushInfo.GetFlushLength())
    {
        buffer->mBuffer.Clear();
        SnapShotByteSlice(buffer);
    }
}

void BufferedByteSlice::SnapShotByteSlice(BufferedByteSlice* buffer) const
{
    buffer->mFlushInfo = mFlushInfo;
    mPostingListWriter.SnapShot(buffer->mPostingListWriter);
}

IE_NAMESPACE_END(index);

