#ifndef __INDEXLIB_BYTE_SLICE_READER_H
#define __INDEXLIB_BYTE_SLICE_READER_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/file_system/block_byte_slice_list.h"

IE_NAMESPACE_BEGIN(common);
class ByteSliceReader
{
public:
    static const int BYTE_SLICE_EOF = -1;

public:
    ByteSliceReader();
    ByteSliceReader(util::ByteSliceList* sliceList);

public:
    uint8_t ReadByte();
    int16_t ReadInt16();
    int32_t ReadInt32();
    uint32_t ReadUInt32();
    int64_t ReadInt64();
    uint64_t ReadUInt64();
    int32_t ReadVInt32();
    uint32_t ReadVUInt32();

    size_t Read(void* value, size_t len);

    /*
     * Parameter value may be modified, so you should call the function
     * with a temporary pointer to the destination buffer.
     */
    size_t ReadMayCopy(void*& value, size_t len);

    int32_t PeekInt32();

    size_t Seek(size_t offset);
    size_t Tell() const { return mGlobalOffset; }
    bool CurrentSliceEnough(size_t len)
    {
        return mCurrentSliceOffset + len <= GetSliceDataSize(mCurrentSlice);
    }
    uint8_t* GetCurrentSliceData()
    {
        return mCurrentSlice->data + mCurrentSliceOffset;
    }

public:
    void Open(util::ByteSliceList* sliceList);
    void Open(util::ByteSlice* slice);
    void Close();
    size_t GetSize() const { return mSliceList->GetTotalSize(); }

    util::ByteSliceList* GetByteSliceList() const { return mSliceList; }
    bool End() const { return Tell() + 1 >= mSize; }    

private:
    void SetSlice(util::ByteSlice* slice);

private:
    template <typename T>
    inline T ReadInt();

    void HandleError(const std::string& msg) const;
    inline util::ByteSlice* NextSlice(util::ByteSlice* byteSlice);
    inline size_t GetSliceDataSize(util::ByteSlice* byteSlice) const;
    inline util::ByteSlice* PrepareSlice(util::ByteSlice* byteSlice);

protected:
    util::ByteSlice* mCurrentSlice;
    size_t mCurrentSliceOffset;
    size_t mGlobalOffset;
    util::ByteSliceList* mSliceList;
    size_t mSize;
    bool mIsBlockByteSliceList;
};

DEFINE_SHARED_PTR(ByteSliceReader);

//////////////////////////////////////////////////////////////
//
inline uint8_t ByteSliceReader::ReadByte()
{
    if (mCurrentSliceOffset >= GetSliceDataSize(mCurrentSlice))
    {
        mCurrentSlice = NextSlice(mCurrentSlice);
        if (!mCurrentSlice)
        {
            HandleError("Read past EOF.");
        }
        mCurrentSliceOffset = 0;
    }
    uint8_t value = mCurrentSlice->data[mCurrentSliceOffset++];
    mGlobalOffset++;
    return value;
}

inline int16_t ByteSliceReader::ReadInt16()
{
    return ReadInt<int16_t>();
}

inline int32_t ByteSliceReader::ReadInt32()
{
    return ReadInt<int32_t>();
}

inline uint32_t ByteSliceReader::ReadUInt32()
{
    return ReadInt<uint32_t>();
}

inline int32_t ByteSliceReader::ReadVInt32()
{
    return (int32_t)ReadVUInt32();
}

inline uint32_t ByteSliceReader::ReadVUInt32()
{
    uint8_t byte = ReadByte();
    uint32_t value = byte & 0x7F;
    int shift = 7;
    
    while(byte & 0x80)
    {
        byte = ReadByte();
        value |= ((byte & 0x7F) << shift);
        shift += 7;
    }
    return value;
}

inline int64_t ByteSliceReader::ReadInt64()
{
    return ReadInt<int64_t>();
}

inline uint64_t ByteSliceReader::ReadUInt64()
{
    return ReadInt<uint64_t>();
}

inline int32_t ByteSliceReader::PeekInt32()
{
    assert(mCurrentSlice != NULL);

    if (mCurrentSliceOffset + sizeof(int32_t) <= GetSliceDataSize(mCurrentSlice))
    {
        return *((int32_t*)(mCurrentSlice->data + mCurrentSliceOffset));
    }

    uint8_t bytes[sizeof(int32_t)];
    char* buffer = (char*)bytes;
    util::ByteSlice* slice = mCurrentSlice;
    util::ByteSlice* nextSlice = NULL;
    size_t curSliceOff = mCurrentSliceOffset;
    for (size_t i = 0; i < sizeof(int32_t); ++i)
    {
        if (curSliceOff >= GetSliceDataSize(slice))
        {
            nextSlice = NextSlice(slice);
            if (nextSlice == NULL || nextSlice->data == NULL)
            {
                HandleError("Read past EOF.");
            }
            else
            {
                slice = nextSlice;
            }
            curSliceOff = 0;
        }
        bytes[i] = slice->data[curSliceOff++];
    }
    return *((int32_t*)buffer);
}

template <typename T>
inline T ByteSliceReader::ReadInt()
{
    auto currentSliceDataSize = GetSliceDataSize(mCurrentSlice);
    if (mCurrentSliceOffset + sizeof(T) <= currentSliceDataSize)
    {
        T value = *((T*)(mCurrentSlice->data + mCurrentSliceOffset));
        mCurrentSliceOffset += sizeof(T);
        mGlobalOffset += sizeof(T);
        return value;
    }

    uint8_t bytes[sizeof(T)];
    char* buffer = (char*)bytes;
    for (size_t i = 0; i < sizeof(T); ++i)
    {
        bytes[i] = ReadByte();
    }
    return *((T*)buffer);
}

inline util::ByteSlice* ByteSliceReader::NextSlice(util::ByteSlice* byteSlice)
{
    if (!mIsBlockByteSliceList)
    {
        return byteSlice->next;
    }
    auto blockSliceList = static_cast<file_system::BlockByteSliceList*>(mSliceList);
    return blockSliceList->GetNextSlice(byteSlice);
}

inline size_t ByteSliceReader::GetSliceDataSize(util::ByteSlice* byteSlice) const {
    if (!mIsBlockByteSliceList)
    {
        return byteSlice->size;
    }
    return byteSlice->dataSize;
}

inline util::ByteSlice* ByteSliceReader::PrepareSlice(util::ByteSlice* byteSlice) {
    if (!mIsBlockByteSliceList)
    {
        return byteSlice;
    }
    auto blockSliceList = static_cast<file_system::BlockByteSliceList*>(mSliceList);
    auto fileOffset = byteSlice->offset + mCurrentSliceOffset;
    auto newSlice = blockSliceList->GetSlice(fileOffset, byteSlice);
    mCurrentSliceOffset = fileOffset - newSlice->offset;
    return newSlice;
}

IE_NAMESPACE_END(common);
#endif //__INDEXENGINEBYTE_SLICE_READER_H
