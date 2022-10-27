#ifndef __INDEXLIB_SHORT_BUFFER_H
#define __INDEXLIB_SHORT_BUFFER_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/multi_value.h"
#include "indexlib/common/atomic_value_typed.h"

IE_NAMESPACE_BEGIN(common);

/*
 * NOTICE:
 * max row count: 8; max column count: 128  
 * column allocate strategy: 2->16->128
 */
class ShortBuffer
{
public:
    typedef autil::mem_pool::Pool Pool;
public:
    // only support max row count is 8
    ShortBuffer(Pool* pool = NULL);
    ~ShortBuffer();

public:
    void Init(const MultiValue* multiValue)
    {
        mMultiValue = multiValue;
        const AtomicValueVector& values = multiValue->GetAtomicValues();
        for (size_t i = 0; i < values.size(); ++i)
        {
            mOffset[i] = values[i]->GetOffset();
        }
    }
    
    // return false, when push back to the same row repeatedly
    //                    and failed to allocate memory
    template<typename T>
    bool PushBack(uint8_t row, T value);

    void EndPushBack() { mSize++; }
    
    uint8_t Capacity() const { return mCapacity; }
    uint8_t Size() const { return mSize; }

    uint8_t GetRowCount() const 
    { return mMultiValue->GetAtomicValueSize(); }

    template<typename T>
    T* GetRowTyped(uint8_t n);

    template<typename T>
    const T* GetRowTyped(uint8_t n) const;

    uint8_t* GetRow(uint8_t n);

    template<typename T>
    T* operator[](uint8_t row) { return (T*)GetRow(row); }

    void Clear();
    bool SnapShot(ShortBuffer& buffer) const;

    Pool* GetPool() const { return mPool; }

    const MultiValue* GetMultiValue() const { return mMultiValue; }

private:
    void* Allocate(size_t size)
    { return mPool->allocate(size); }
    void Deallocate(void* buf, size_t size)
    { mPool->deallocate(buf, size); }

    bool Reallocate();
    static uint8_t AllocatePlan(uint8_t curCapacity);

    static void BufferMemoryCopy(uint8_t* dst, uint8_t dstColCount, 
                                 uint8_t* src, uint8_t srcColCount,
                                 const MultiValue* multiValue, uint8_t srcSize);

    bool IsFull() const { return mSize >= mCapacity; }

    static uint8_t* GetRow(uint8_t* buffer, uint8_t capacity, 
                           const AtomicValue* atomicValue);

    void ReleaseBuffer(uint8_t* buffer, uint8_t capacity);
    void Reserve(uint8_t size);

protected:
    uint8_t* volatile mBuffer;
    uint8_t mOffset[8];
    uint8_t volatile mCapacity;
    uint8_t volatile mSize;

    bool volatile mIsBufferValid;
    bool mHasPool;
    Pool* mPool;
    const MultiValue* mMultiValue;

private:
    IE_LOG_DECLARE();
    friend class ShortBufferTest;
};

DEFINE_SHARED_PTR(ShortBuffer);

////////////////////////////////////////////////////////////////////////

template<typename T>
inline bool ShortBuffer::PushBack(uint8_t row, T value)
{
    if (IsFull())
    {
        if (!Reallocate())
        {
            return false;
        }
    }
    assert(common::ValueTypeTraits<T>::TYPE 
           == mMultiValue->GetAtomicValue(row)->GetType());
    T *rowData = GetRowTyped<T>(row);
    if (!rowData)
    {
        return false;
    }
    rowData[mSize] = value;

    return true;
}

template<typename T>
inline T* ShortBuffer::GetRowTyped(uint8_t n)
{
    assert(n < GetRowCount());
    return (T*)(mBuffer + mOffset[n] * mCapacity);
}

template<typename T>
inline const T* ShortBuffer::GetRowTyped(uint8_t n) const
{
    assert(n < GetRowCount());
    return (const T*)(mBuffer + mOffset[n] * mCapacity);
}

inline uint8_t* ShortBuffer::GetRow(uint8_t n)
{
    AtomicValue* atomicValue = mMultiValue->GetAtomicValue(n);
    return GetRow((uint8_t*)mBuffer, mCapacity, atomicValue);
}

inline uint8_t* ShortBuffer::GetRow(uint8_t* buffer, uint8_t capacity, 
                             const AtomicValue* atomicValue)
{
    uint32_t offset = atomicValue->GetOffset();
    if (!offset) { return buffer; }
    return buffer + offset * capacity;
}

DEFINE_SHARED_PTR(ShortBuffer);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SHORT_BUFFER_H
