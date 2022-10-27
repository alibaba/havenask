#ifndef __INDEXLIB_BLOCK_CACHE_METRICS_H
#define __INDEXLIB_BLOCK_CACHE_METRICS_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(file_system);

struct BlockCacheMetrics
{
    std::string name;
    uint64_t totalAccessCount;
    uint64_t totalHitCount;
    uint32_t last1000HitCount;

    BlockCacheMetrics()
        : totalAccessCount(0)
        , totalHitCount(0)
        , last1000HitCount(0)
    { }   
};
typedef std::vector<BlockCacheMetrics> BlockCacheMetricsVec;

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_BLOCK_CACHE_METRICS_H
