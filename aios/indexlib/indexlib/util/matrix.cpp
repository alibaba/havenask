#include "indexlib/util/matrix.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, Matrix);

Matrix::Matrix(uint8_t rowCount)
    : mBuffer(NULL)
    , mSize(0)
    , mCapacity(0)
    , mRowCount(rowCount)
    , mPushBackFlag(0)
    , mHasPool(false)
{
}

Matrix::~Matrix()
{
    // here we assume this object and pool have the same life cycle
    // ReleaseBuffer((uint8_t*)mBuffer, mCapacity);
    if (!mHasPool)
    {
        ReleaseBuffer((uint8_t*)mBuffer, mCapacity);
    }
}

bool Matrix::PushBack(uint8_t row, ValueType value)
{
    if (IsFull())
    {
        if (!Reallocate())
        {
            return false;
        }
    }

    ValueType *rowData = GetRow(row);
    if (!rowData)
    {
        return false;
    }
    if (!SetPushBackFlag(row))
    {
        return false;
    }
    rowData[mSize] = value;

    TryIncCursor();
    return true;
}

bool Matrix::Reallocate()
{
    uint8_t oldCapacity = mCapacity;
    uint8_t* oldBuffer = (uint8_t*)mBuffer;
    uint8_t newCapacity = AllocatePlan(oldCapacity);
    if (newCapacity == oldCapacity)
    {
        return false;
    }
    size_t docItemSize = newCapacity * sizeof(ValueType) * mRowCount;

    uint8_t *newBuffer = (uint8_t*)Allocate(docItemSize);
    assert(newBuffer);

    BufferMemoryCopy(newBuffer, newCapacity, 
                     (uint8_t*)mBuffer, mCapacity,
                     mRowCount, mSize);

    mBuffer = newBuffer;

    MEMORY_BARRIER();

    mCapacity = newCapacity;

    ReleaseBuffer(oldBuffer, oldCapacity);
    
    return true;
}

void Matrix::ReleaseBuffer(uint8_t* buffer, uint8_t capacity)
{
    if (buffer == NULL || capacity == 0)
    {
        return;
    }
    size_t bufferSize = capacity * mRowCount; 
    Deallocate((void*)buffer, bufferSize);

}

void Matrix::BufferMemoryCopy(uint8_t* dst, uint8_t dstColCount, 
                              uint8_t* src, uint8_t srcColCount,
                              uint8_t rowCount, uint8_t srcSize)
{
    if (src == NULL || srcSize == 0)
    {
        return;
    }
    for (uint8_t i = 0; i < rowCount; ++i)
    {
        memcpy(GetRow(dst, dstColCount, i), GetRow(src, srcColCount, i), 
               srcSize * sizeof(ValueType));
    }
}

Matrix::ValueType* Matrix::GetRow(uint8_t n)
{
    if (n >= mRowCount)
    {
        return NULL;
    }
    return GetRow((uint8_t*)mBuffer, mCapacity, n);
}

Matrix::ValueType* Matrix::GetRow(uint8_t* buffer, uint8_t capacity, uint8_t n)
{
    return (ValueType*)buffer + capacity * n;     
}

void Matrix::Clear()
{
    mPushBackFlag = 0;
    mSize = 0;
}

bool Matrix::SnapShot(Matrix& matrix) const
{
    matrix.Clear();

    if (matrix.GetRowCount() != mRowCount)
    {
        return false;
    }
    if (mBuffer == NULL)
    {
        return true;
    }

    // here we assume buffer size only can be increased
    uint8_t sizeSnapShot = mSize;

    // sizeSnapShot <= capacitySnapShot in recycle pool
    matrix.Reserve(sizeSnapShot); 

    uint8_t capacitySnapShot = 0;
    volatile uint8_t* bufferSnapShot = mBuffer;
    do
    {
        capacitySnapShot = mCapacity;

        MEMORY_BARRIER();

        bufferSnapShot = mBuffer;

        BufferMemoryCopy((uint8_t*)matrix.mBuffer, matrix.mCapacity, 
                         (uint8_t*)bufferSnapShot, capacitySnapShot, 
                         mRowCount, sizeSnapShot);
    }
    while (mBuffer != bufferSnapShot
           || mCapacity > capacitySnapShot); // copy again when capacity changed

    matrix.mSize = sizeSnapShot;
    return true;
}

void Matrix::Reserve(uint8_t capacity) 
{
    if (mBuffer != NULL)
    {
        if (mCapacity >= capacity) return;
    }

    size_t docItemSize = capacity * sizeof(ValueType) * mRowCount;
    
    uint8_t *newBuffer = (uint8_t*)Allocate(docItemSize);

    if (mBuffer != NULL)
    {
        BufferMemoryCopy(newBuffer, capacity, 
                         (uint8_t*)mBuffer, mCapacity,
                         mRowCount, mSize);
        ReleaseBuffer((uint8_t*)mBuffer, mCapacity);
    }

    mBuffer = newBuffer;
    mCapacity = capacity;
}

bool Matrix::SetPushBackFlag(uint8_t row)
{
    assert(row < mRowCount);
    uint8_t rowFlag = (uint8_t)1 << row;
    if (mPushBackFlag & rowFlag)
    {
        // already pushed this row
        return false;
    }
    mPushBackFlag |= rowFlag;
    return true;
}

void Matrix::TryIncCursor()
{
    if (mPushBackFlag == (((uint8_t)1 << mRowCount) - 1))
    {
        mSize++;
        mPushBackFlag = 0;
    }
}

uint8_t Matrix::AllocatePlan(uint8_t curCapacity)
{
    if (curCapacity < 2)
    {
        return 2;
    }
    else if (curCapacity < 16)
    {
        return 16;
    }
    return MAX_DOC_PER_RECORD;
}

IE_NAMESPACE_END(util);

