#ifndef __INDEXLIB_MATRIX_H
#define __INDEXLIB_MATRIX_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <stdlib.h>
#include <string.h>

IE_NAMESPACE_BEGIN(util);

/*
 * NOTICE:
 * max row count: 8; max column count: 128  
 * column allocate strategy: 2->16->128
 */
class Matrix
{
public:
    typedef uint32_t ValueType;

public:
    // only support max row count is 8
    Matrix(uint8_t rowCount);
    virtual ~Matrix();

public:
    // return false, when push back to the same row repeatedly
    //                    and failed to allocate memory
    bool PushBack(uint8_t row, ValueType value);
    
    uint8_t Capacity() const { return mCapacity; }
    uint8_t Size() const { return mSize; }

    // set before calling PushBack
    void SetRowCount(uint8_t rowCount) { mRowCount = rowCount; }
    uint8_t GetRowCount() const { return mRowCount; }

    ValueType* GetRow(uint8_t n);
    ValueType* operator[](uint8_t row) { return GetRow(row); }

    void Clear();
    bool SnapShot(Matrix& matrix) const;

public:
    template<typename T1, typename T2>
    static void CopyTypedValues(T1* dest, const T2* src, size_t n);

private:
    virtual void* Allocate(size_t size)
    { return malloc(size); }
    virtual void Deallocate(void* buf, size_t size)
    { if (buf) free(buf); }

    bool Reallocate();
    static uint8_t AllocatePlan(uint8_t curCapacity);

    static void BufferMemoryCopy(uint8_t* dst, uint8_t dstColCount, 
                                 uint8_t* src, uint8_t srcColCount,
                                 uint8_t rowCount, uint8_t srcSize);

    bool IsFull() const { return mSize >= mCapacity; }
    bool SetPushBackFlag(uint8_t row);
    void TryIncCursor();

    static ValueType* GetRow(uint8_t* buffer, uint8_t capacity, uint8_t n);
    void ReleaseBuffer(uint8_t* buffer, uint8_t capacity);
    void Reserve(uint8_t size);

private:
    volatile uint8_t *mBuffer;
    volatile uint8_t mSize;
    volatile uint8_t mCapacity;

    uint8_t mRowCount;
    uint8_t mPushBackFlag;

    IE_LOG_DECLARE();

protected:
    bool mHasPool;

private:
    friend class MatrixTest;
};

DEFINE_SHARED_PTR(Matrix);

////////////////////////////////////////////////////////////////////////

template<typename T1, typename T2>
void Matrix::CopyTypedValues(T1* dest, const T2* src, size_t n)
{
    for (size_t i = 0; i < n; ++i)
    {
        dest[i] = static_cast<T1>(src[i]);
    }
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_MATRIX_H
