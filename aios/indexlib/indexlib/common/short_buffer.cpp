#include "indexlib/common/short_buffer.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, ShortBuffer);

ShortBuffer::ShortBuffer(Pool* pool)
    : mBuffer(NULL)
    , mCapacity(0)
    , mSize(0)
    , mIsBufferValid(true)
    , mHasPool(false)
    , mPool(pool)
    , mMultiValue(NULL)
{
    if (!mPool)
    {
        mPool = new Pool;
        mHasPool = true;
    }
}

ShortBuffer::~ShortBuffer()
{
    // here we assume this object and pool have the same life cycle
    // ReleaseBuffer((uint8_t*)mBuffer, mCapacity);
    if (!mHasPool)
    {
        //ReleaseBuffer((uint8_t*)mBuffer, mCapacity);
    }

    if (mHasPool)
    {
        DELETE_AND_SET_NULL(mPool);
    }
}

bool ShortBuffer::Reallocate()
{
    uint8_t oldCapacity = mCapacity;
    uint8_t* oldBuffer = (uint8_t*)mBuffer;
    uint8_t newCapacity = AllocatePlan(oldCapacity);
    if (newCapacity == oldCapacity)
    {
        return false;
    }
    size_t docItemSize = newCapacity * mMultiValue->GetTotalSize();

    uint8_t *newBuffer = (uint8_t*)Allocate(docItemSize);
    assert(newBuffer);

    BufferMemoryCopy(newBuffer, newCapacity, 
                     (uint8_t*)mBuffer, mCapacity,
                     mMultiValue, mSize);

    mIsBufferValid = false;

    MEMORY_BARRIER();

    mBuffer = newBuffer;

    MEMORY_BARRIER();

    mCapacity = newCapacity;

    MEMORY_BARRIER();

    mIsBufferValid = true;

    ReleaseBuffer(oldBuffer, oldCapacity);
    
    return true;
}

void ShortBuffer::ReleaseBuffer(uint8_t* buffer, uint8_t capacity)
{
    if (buffer == NULL || capacity == 0)
    {
        return;
    }
    size_t bufferSize = capacity * mMultiValue->GetTotalSize(); 
    Deallocate((void*)buffer, bufferSize);

}

void ShortBuffer::BufferMemoryCopy(uint8_t* dst, uint8_t dstColCount, 
                                   uint8_t* src, uint8_t srcColCount,
                                   const MultiValue* multiValue, uint8_t srcSize)
{
    if (src == NULL || srcSize == 0)
    {
        return;
    }
    for (uint8_t i = 0; i < multiValue->GetAtomicValueSize(); ++i)
    {
        const AtomicValue* atomicValue = multiValue->GetAtomicValue(i);
        memcpy(GetRow(dst, dstColCount, atomicValue), 
               GetRow(src, srcColCount, atomicValue),
               srcSize * atomicValue->GetSize());
    }
}

void ShortBuffer::Clear()
{
    mSize = 0;
}

bool ShortBuffer::SnapShot(ShortBuffer& shortBuffer) const
{
    shortBuffer.Clear();

    if (shortBuffer.GetRowCount() != mMultiValue->GetAtomicValueSize())
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
    shortBuffer.Reserve(sizeSnapShot); 

    uint8_t capacitySnapShot = 0;
    uint8_t* bufferSnapShot = mBuffer;
    do
    {
        capacitySnapShot = mCapacity;

        MEMORY_BARRIER();

        bufferSnapShot = mBuffer;

        BufferMemoryCopy((uint8_t*)shortBuffer.mBuffer, shortBuffer.mCapacity, 
                         (uint8_t*)bufferSnapShot, capacitySnapShot, 
                         mMultiValue, sizeSnapShot);
    }
    while (!mIsBufferValid
           || mBuffer != bufferSnapShot
           || mCapacity > capacitySnapShot); 

    shortBuffer.mSize = sizeSnapShot;
    return true;
}

void ShortBuffer::Reserve(uint8_t capacity) 
{
    if (mBuffer != NULL)
    {
        if (mCapacity >= capacity) return;
    }

    size_t docItemSize = capacity * mMultiValue->GetTotalSize();
    
    uint8_t *newBuffer = (uint8_t*)Allocate(docItemSize);

    if (mBuffer != NULL)
    {
        BufferMemoryCopy(newBuffer, capacity, 
                         (uint8_t*)mBuffer, mCapacity,
                         mMultiValue, mSize);
        ReleaseBuffer((uint8_t*)mBuffer, mCapacity);
    }

    mBuffer = newBuffer;
    mCapacity = capacity;
}

uint8_t ShortBuffer::AllocatePlan(uint8_t curCapacity)
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

IE_NAMESPACE_END(common);

