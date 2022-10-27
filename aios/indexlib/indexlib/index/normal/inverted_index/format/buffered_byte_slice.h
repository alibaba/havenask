#ifndef __INDEXLIB_BUFFERED_BYTE_SLICE_H
#define __INDEXLIB_BUFFERED_BYTE_SLICE_H

#include <tr1/memory>
#include <autil/mem_pool/RecyclePool.h>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/atomic_value.h"
#include "indexlib/common/multi_value.h"
#include "indexlib/common/short_buffer.h"
#include "indexlib/common/byte_slice_writer.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/index/normal/inverted_index/format/flush_info.h"

IE_NAMESPACE_BEGIN(index);

class BufferedByteSlice
{
public:
    //TODO: delete
    virtual void Dump(storage::FileWriter* file)
    { mPostingListWriter.Dump(file); }

public:
    BufferedByteSlice(autil::mem_pool::Pool* byteSlicePool,
                      autil::mem_pool::Pool* bufferPool);
    virtual ~BufferedByteSlice();

public:
    bool Init(const common::MultiValue* value);

    template<typename T>
    void PushBack(const common::AtomicValueTyped<T>* value) __ALWAYS_INLINE;

    template<typename T>
    void PushBack(uint8_t row, T value) __ALWAYS_INLINE;

    void EndPushBack() 
    { 
        mFlushInfo.SetIsValidShortBuffer(true);
        MEMORY_BARRIER();
        mBuffer.EndPushBack(); 
    }

    bool NeedFlush(uint8_t needFlushCount = MAX_DOC_PER_RECORD) const
    { return mBuffer.Size() == needFlushCount; }

    //TODO: only one compress_mode 
    virtual size_t Flush(uint8_t compressMode); //virtual for test

    virtual void Dump(const file_system::FileWriterPtr& file)
    { mPostingListWriter.Dump(file); }

    virtual size_t EstimateDumpSize() const
    { return mPostingListWriter.GetSize(); }

    size_t GetBufferSize() const { return mBuffer.Size(); }

    const common::ShortBuffer& GetBuffer() const { return mBuffer; }

    //TODO: now we only support 512M count
    //should be used in realtime build
    size_t GetTotalCount() const
    { return mFlushInfo.GetFlushCount() + mBuffer.Size(); }

    FlushInfo GetFlushInfo() const { return mFlushInfo; }

    const common::MultiValue* GetMultiValue() const
    { return mBuffer.GetMultiValue(); }

    const util::ByteSliceList* GetByteSliceList() const
    { return mPostingListWriter.GetByteSliceList(); }

    //SnapShot buffer is readonly 
    void SnapShot(BufferedByteSlice* buffer) const;

    bool IsShortBufferValid() const
    {
        return mFlushInfo.IsValidShortBuffer();
    }

    autil::mem_pool::Pool* GetBufferPool() const
    {
        return mBuffer.GetPool();
    }

protected:
    // template function
    virtual size_t DoFlush(uint8_t compressMode);

private:
    void SnapShotByteSlice(BufferedByteSlice* buffer) const;

public:
    util::ByteSliceList* GetByteSliceList()
    { return mPostingListWriter.GetByteSliceList(); }

protected:
    FlushInfo mFlushInfo;
    common::ShortBuffer mBuffer;
    common::ByteSliceWriter mPostingListWriter;

private:
    IE_LOG_DECLARE();

private:
    friend class BufferedByteSlicePerfTest;
};

DEFINE_SHARED_PTR(BufferedByteSlice);

//////////////////////////////////////////////////////////////////////

template<typename T>
inline void BufferedByteSlice::PushBack(const common::AtomicValueTyped<T>* value)
{
    assert(value);
    mBuffer.PushBack(value->GetLocation(), value->GetValue());
}

template<typename T>
inline void BufferedByteSlice::PushBack(uint8_t row, T value)
{
    mBuffer.PushBack(row, value);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUFFERED_BYTE_SLICE_H
