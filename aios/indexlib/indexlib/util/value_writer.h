#ifndef __INDEXLIB_VALUE_WRITER_H
#define __INDEXLIB_VALUE_WRITER_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/mmap_pool.h"

IE_NAMESPACE_BEGIN(util);

template<typename ValueOffsetType>
class ValueWriter
{
public:
    ValueWriter()
        : mReserveBytes(0)
        , mUsedBytes(0)
        , mBaseAddr(NULL)
        , mPool(NULL)
    { }
    ~ValueWriter()
    {
        if (mPool)
        {
            mPool->deallocate(mBaseAddr, mReserveBytes);
            DELETE_AND_SET_NULL(mPool);
        }
    }

    void Init(ValueOffsetType reserveBytes)
    {
        if (!mPool)
        {
            mPool = new util::MmapPool();
        }
        mReserveBytes = reserveBytes;
        mBaseAddr = static_cast<char*>(mPool->allocate(reserveBytes));
        if (!mBaseAddr)
        {
            INDEXLIB_FATAL_ERROR(OutOfMemory, "Value writer mmap mem [%lu bytes] failed",
                    (size_t)reserveBytes);
        }
    }

    void Init(char* baseAddr, ValueOffsetType reserveBytes)
    {
        assert(!mPool);
        assert(baseAddr);

        mReserveBytes = reserveBytes;
        mBaseAddr = baseAddr;
    }

    char* GetValue(ValueOffsetType offset)
    {
        assert(offset < mUsedBytes);
        return mBaseAddr + offset;
    }

    const char* GetValue(ValueOffsetType offset) const
    {
        assert(offset < mUsedBytes);
        return mBaseAddr + offset;
    }

    const char* GetBaseAddress() const { return mBaseAddr; }
    ValueOffsetType GetUsedBytes() const { return mUsedBytes; }
    ValueOffsetType GetReserveBytes() const {return mReserveBytes; }
    
public:
    ValueOffsetType Append(const autil::ConstString& value)
    {
        if (mUsedBytes + value.size() > mReserveBytes)
        {
            INDEXLIB_FATAL_ERROR(OutOfMemory,
                    "value writer exceed mem reserve, reserve size [%lu], used bytes [%lu]"
                    ", value size [%lu]", (size_t)mReserveBytes, (size_t)mUsedBytes, value.size());
        }
        char *addr = mBaseAddr + mUsedBytes;
        memcpy(addr, value.data(), value.size());
        ValueOffsetType ret = mUsedBytes;
        MEMORY_BARRIER();
        mUsedBytes += value.size();
        return ret;
    }
    
private:
    ValueOffsetType mReserveBytes;
    ValueOffsetType mUsedBytes;
    char* mBaseAddr;
    util::MmapPool* mPool;
    
private:
    IE_LOG_DECLARE();
};
IE_LOG_SETUP_TEMPLATE(util, ValueWriter);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_VALUE_WRITER_H
