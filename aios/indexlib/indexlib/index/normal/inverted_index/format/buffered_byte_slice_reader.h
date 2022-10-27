#ifndef __INDEXLIB_BUFFERED_BYTE_SLICE_READER_H
#define __INDEXLIB_BUFFERED_BYTE_SLICE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/buffered_byte_slice.h"

IE_NAMESPACE_BEGIN(index);

class BufferedByteSliceReader
{
public:
    BufferedByteSliceReader();
    ~BufferedByteSliceReader();

public:
    void Open(const BufferedByteSlice* bufferedByteSlice);
    
    /*
     * pos <= ByteSliceSize
     */
    void Seek(uint32_t pos)
    {
        mByteSliceReader.Seek(pos);
        mLocationCursor = 0;
        mShortBufferCursor = 0;
    }

    uint32_t Tell() const
    { return mByteSliceReader.Tell(); }

    //TODO: now user should confirm buffer actual count
    template<typename T>
    bool Decode(T *buffer, size_t count, size_t& decodeCount);

private:
    common::AtomicValue* CurrentValue();
    void IncValueCursor();

    bool IsValidShortBuffer() const
    {
        uint32_t bufferSize = mBufferedByteSlice->GetBufferSize();
        return bufferSize > 0
            && mShortBufferCursor < mMultiValue->GetAtomicValueSize()
            && mBufferedByteSlice->IsShortBufferValid();
    }

private:
    uint8_t mLocationCursor;
    uint8_t mShortBufferCursor;
    common::ByteSliceReader mByteSliceReader;
    const BufferedByteSlice* mBufferedByteSlice;
    const common::MultiValue* mMultiValue;

private:
    friend class BufferedByteSliceReaderTest;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BufferedByteSliceReader);

template<typename T>
bool BufferedByteSliceReader::Decode(T* buffer, size_t count, size_t& decodeCount)
{
    //TODO:
    if (count == 0) return false;

    FlushInfo flushInfo = mBufferedByteSlice->GetFlushInfo();
    uint32_t byteSliceSize = flushInfo.GetFlushLength();

    if (mByteSliceReader.Tell() >= byteSliceSize && !IsValidShortBuffer())
    {
        return false;
    }

    common::AtomicValue* currentValue = CurrentValue();
    assert(common::ValueTypeTraits<T>::TYPE == currentValue->GetType());

    if (mByteSliceReader.Tell() >= byteSliceSize)
    {
        size_t bufferSize = mBufferedByteSlice->GetBufferSize();
        assert(bufferSize <= count);

        const common::ShortBuffer& shortBuffer = mBufferedByteSlice->GetBuffer();
        const T* src = shortBuffer.GetRowTyped<T>(currentValue->GetLocation());

        memcpy((void *)buffer, (const void *)src, bufferSize * sizeof(T));
        
        decodeCount = bufferSize;
        mShortBufferCursor++;
    }
    else
    {
        decodeCount = currentValue->Decode(flushInfo.GetCompressMode(), 
                (uint8_t*)buffer, count * sizeof(T), mByteSliceReader);
    }

    IncValueCursor();

    return true;
}

inline common::AtomicValue* BufferedByteSliceReader::CurrentValue()
{
    const common::AtomicValueVector& values = mMultiValue->GetAtomicValues();
    assert(mLocationCursor < mMultiValue->GetAtomicValueSize());
    return values[mLocationCursor];
}

inline void BufferedByteSliceReader::IncValueCursor()
{
    mLocationCursor++;
    if (mLocationCursor == mMultiValue->GetAtomicValueSize())
    {
        mLocationCursor = 0;
    }
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUFFERED_BYTE_SLICE_READER_H
