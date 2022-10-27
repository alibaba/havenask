#ifndef __INDEXLIB_KKV_ITERATOR_IMPL_BASE_H
#define __INDEXLIB_KKV_ITERATOR_IMPL_BASE_H

#include <tr1/memory>
#include <tr1/unordered_set>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index);

typedef uint64_t PKeyType;
class KKVIndexOptions;
class KVMetricsCollector;
class KKVIteratorImplBase
{
public:
    enum IterStatus
    {
        NONE,
        BUILDING,
        BUILT,
        DONE,
    };

    inline KKVIteratorImplBase(autil::mem_pool::Pool* pool,
                               PKeyType pKey, KKVIndexOptions* indexOptions,
                               uint64_t minimumTsInSecond,
                               KVMetricsCollector* metricsCollector)
    {
    }

    virtual ~KKVIteratorImplBase() {
    }

public:
    bool IsSorted() const {
        assert(false);
        return false;
    }
    PKeyType GetLiteralPKeyHash() const {
        assert(false);
        return 0;
    }
    PKeyType GetPKeyHash() const {
        assert(false);
        return 0;
    }

    inline bool IsValid() const { assert(false); return false; }
};

DEFINE_SHARED_PTR(KKVIteratorImplBase);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_KKV_ITERATOR_IMPL_BASE_H
