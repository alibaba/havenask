#ifndef __INDEXLIB_READ_OPTION_H
#define __INDEXLIB_READ_OPTION_H
#include "indexlib/storage/fslib_option.h"

IE_NAMESPACE_BEGIN(file_system);

struct BlockAccessCounter
{
    int64_t blockCacheHitCount = 0;
    int64_t blockCacheMissCount = 0;
    int64_t blockCacheReadLatency = 0;
    int64_t blockCacheIOCount = 0;
    int64_t blockCacheIODataSize = 0;

    BlockAccessCounter() = default;
    void Reset()
    {
        *this = BlockAccessCounter();
    }
};

struct ReadOption
{
    BlockAccessCounter* blockCounter = nullptr;
    int advice = storage::IO_ADVICE_NORMAL;

    ReadOption() = default;
};


IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_READ_OPTION_H
