#ifndef __INDEXLIB_BYTE_SLICE_WRITER_H
#define __INDEXLIB_BYTE_SLICE_WRITER_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include <autil/mem_pool/Pool.h>
#include "indexlib/util/byte_slice_list/byte_slice_list.h"
#include "indexlib/storage/file_writer.h"
#include "indexlib/file_system/file_writer.h"

IE_NAMESPACE_BEGIN(common);

class ByteSliceWriter
{
public:
    //TODO: delete
    void Dump(storage::FileWriter* file);
public:
    constexpr static uint32_t MIN_SLICE_SIZE = util::ByteSlice::GetHeadSize();
public:
    ByteSliceWriter(autil::mem_pool::Pool *pool = NULL,
                    uint32_t minSliceSize = MIN_SLICE_SIZE);
    ~ByteSliceWriter();
    
public:
    void WriteByte(uint8_t value);
    void WriteInt16(int16_t value);
    void WriteInt32(int32_t value);
    void WriteUInt32(uint32_t value);
    void Write(const void* value, size_t len);
    void Write(util::ByteSliceList& src);

    template<typename T>
    void Write(T value);
    
    /**
     * copy data from src[start, end) to *this
     */
    void Write(const util::ByteSliceList& src, uint32_t start, uint32_t end);
    uint32_t WriteVInt(uint32_t value);

    /** 
     * hold an int32 slot for later value updating
     */
    int32_t& HoldInt32();

    size_t GetSize() const;
    util::ByteSliceList* GetByteSliceList()
    { return mSliceList; }

    const util::ByteSliceList* GetByteSliceList() const
    { return mSliceList; }

    void Dump(const file_system::FileWriterPtr& file);

    //snapshot writer can't call this interface
    void Reset();
    void Close();

    //shallow copy, not own byte slice list
    void SnapShot(ByteSliceWriter& byteSliceWriter) const;

#ifdef PACK_INDEX_MEMORY_STAT
    uint32_t GetAllocatedSize() const
    {
        return mAllocatedSize;
    }
#endif

protected:
    // disable copy ctor
    ByteSliceWriter(const ByteSliceWriter& other);
    ByteSliceWriter& operator=(const ByteSliceWriter& other);

protected:
    util::ByteSlice* CreateSlice(uint32_t size);
    uint32_t GetIncrementedSliceSize(uint32_t prevSliceSize);

    template<typename T>
    util::ByteSlice* GetSliceForWrite();
    
    util::ByteSliceList* AllocateByteSliceList();
    void DeAllocateByteSliceList(util::ByteSliceList*& byteSliceList);

private:
    util::ByteSliceList* mSliceList;
    autil::mem_pool::Pool* mPool;
    uint32_t mLastSliceSize;
    bool mIsOwnSliceList;

#ifdef PACK_INDEX_MEMORY_STAT
    uint32_t mAllocatedSize;
#endif

private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<ByteSliceWriter> ByteSliceWriterPtr;

//////////////////////////////////////////////////////////////
//
inline void ByteSliceWriter::WriteByte(uint8_t value)
{
    util::ByteSlice* slice = GetSliceForWrite<uint8_t>();
    slice->data[slice->size++] = value;
    mSliceList->IncrementTotalSize(1);
}

inline void ByteSliceWriter::WriteInt16(int16_t value)
{
    util::ByteSlice* slice = GetSliceForWrite<int16_t>();
    *(int16_t*)(slice->data + slice->size) = value;
    slice->size += sizeof(int16_t);
    mSliceList->IncrementTotalSize(sizeof(int16_t));
}

inline void ByteSliceWriter::WriteInt32(int32_t value)
{
    util::ByteSlice* slice = GetSliceForWrite<int32_t>();
    *(int32_t*)(slice->data + slice->size) = value;
    slice->size += sizeof(int32_t);
    mSliceList->IncrementTotalSize(sizeof(int32_t));
}

template<typename T>
inline void ByteSliceWriter::Write(T value)
{
    util::ByteSlice* slice = GetSliceForWrite<T>();
    *(T*)(slice->data + slice->size) = value;
    slice->size += sizeof(T);
    mSliceList->IncrementTotalSize(sizeof(T));
}

inline uint32_t ByteSliceWriter::WriteVInt(uint32_t value)
{
    uint32_t len = 1;
    while (value > 0x7F)
    {
        WriteByte(0x80 | (value & 0x7F));
        value >>= 7;
        ++len;
    }

    WriteByte(value & 0x7F);
    return len;
}

inline void ByteSliceWriter::WriteUInt32(uint32_t value)
{
    util::ByteSlice* slice = GetSliceForWrite<uint32_t>();
    *(uint32_t*)(slice->data + slice->size) = value;
    slice->size += sizeof(uint32_t);
    mSliceList->IncrementTotalSize(sizeof(uint32_t));
}

inline int32_t& ByteSliceWriter::HoldInt32()
{
    util::ByteSlice* slice = GetSliceForWrite<int32_t>();
    
    int32_t& ret= *(int32_t*)(slice->data + slice->size);
    slice->size += sizeof(int32_t);
    mSliceList->IncrementTotalSize(sizeof(int32_t));
    return ret;
}

inline uint32_t ByteSliceWriter::GetIncrementedSliceSize(uint32_t lastSliceSize)
{
    uint32_t prev = lastSliceSize + util::ByteSlice::GetHeadSize();
    uint32_t sliceSize = prev + (prev >> 2) - util::ByteSlice::GetHeadSize();
    uint32_t chunkSize = (uint32_t)DEFAULT_CHUNK_SIZE * 1024 * 1024
                         - util::ByteSlice::GetHeadSize() - sizeof(autil::mem_pool::ChainedMemoryChunk);
    return sliceSize < chunkSize ? sliceSize : chunkSize;
}

template<typename T>
inline util::ByteSlice* ByteSliceWriter::GetSliceForWrite()
{
    util::ByteSlice* slice = mSliceList->GetTail();
    if (slice->size + sizeof(T) > mLastSliceSize)
    {
        mLastSliceSize = GetIncrementedSliceSize(mLastSliceSize);
        slice = CreateSlice(mLastSliceSize);
        mSliceList->Add(slice);
    }
    return slice;
}

inline util::ByteSliceList* ByteSliceWriter::AllocateByteSliceList()
{
    if (mPool)
    {
        void *buffer = mPool->allocate(sizeof(util::ByteSliceList));
        return new(buffer) util::ByteSliceList();
    }
    else
    {
        return new util::ByteSliceList();
    }
}

inline void ByteSliceWriter::DeAllocateByteSliceList(util::ByteSliceList*& byteSliceList)
{
    if (mPool)
    {
        byteSliceList->~ByteSliceList();
        byteSliceList = NULL;
    }
    else
    {
        delete byteSliceList;
        byteSliceList = NULL;
    }
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIBBYTE_SLICE_WRITER_H
