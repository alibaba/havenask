#ifndef __INDEXLIB_MEM_BUFFER_H
#define __INDEXLIB_MEM_BUFFER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/simple_pool.h"

IE_NAMESPACE_BEGIN(util);

class MemBuffer
{
public:
    MemBuffer(SimplePool* pool = NULL)
        : mBuffer(NULL)
        , mSize(0)
        , mSimplePool(pool)
    {}

    MemBuffer(size_t initSize, SimplePool* pool = NULL)
        : mBuffer(NULL)
        , mSize(0)
        , mSimplePool(pool)
    {
        Reserve(initSize);
    }

    MemBuffer(const MemBuffer& other)
        : mBuffer(NULL)
        , mSize(0)
        , mSimplePool(other.mSimplePool)
    {
        Reserve(other.mSize);
        if (mBuffer)
        {
            memcpy(mBuffer, other.mBuffer, mSize);
        }
    }

    ~MemBuffer()
    {
        Release();
    }

public:
    void Reserve(size_t size)
    {
        if (size <= mSize)
        {
            return;
        }

        Release();
        mBuffer = Allocate(size);
        mSize = size;
    }

    char* GetBuffer() { return mBuffer; }
    const char* GetBuffer() const { return mBuffer; }
    size_t GetBufferSize() const { return mSize; }

    void Release()
    {
        if (!mBuffer)
        {
            return;
        }

        if (mSimplePool)
        {
            mSimplePool->deallocate(mBuffer, mSize);
        }
        else
        {
            delete[] mBuffer;
        }
        mSize = 0;
        mBuffer = NULL;
    }

private:
    char* Allocate(size_t size)
    {
        char* buf = NULL;
        if (mSimplePool)
        {
            buf = (char*)mSimplePool->allocate(size);
        }
        else
        {
            buf = new char[size];
        }
        return buf;
    }

private:
    char* mBuffer;
    size_t mSize;
    SimplePool* mSimplePool;
};

DEFINE_SHARED_PTR(MemBuffer);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_MEM_BUFFER_H
