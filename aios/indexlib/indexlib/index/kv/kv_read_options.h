#ifndef __INDEXLIB_KV_READ_OPTIONS_H
#define __INDEXLIB_KV_READ_OPTIONS_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"

IE_NAMESPACE_BEGIN(index);
class KVMetricsCollector;
struct KVReadOptions
{
    uint64_t timestamp = 0;
    TableSearchCacheType searchCacheType = tsc_default;
    autil::mem_pool::Pool* pool = NULL;
    std::string fieldName; // valid when multi field
    attrid_t attrId = INVALID_ATTRID;
    KVMetricsCollector* metricsCollector = NULL;
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_KV_READ_OPTIONS_H
