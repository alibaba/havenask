#ifndef __INDEXLIB_MEMORY_QUOTA_CONTROLLER_H
#define __INDEXLIB_MEMORY_QUOTA_CONTROLLER_H

#include <tr1/memory>
#include <atomic>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

class MemoryQuotaController
{
public:
    MemoryQuotaController(int64_t totalQuota);
    virtual ~MemoryQuotaController();

public:
    void Allocate(int64_t quota)
    {
        assert(quota >= 0);
        IE_LOG(TRACE1, "MemoryQuotaController[%p] Allocate[%ld], FreeQuota[%ld]",
               this, quota, mLeftQuota.load());
        mLeftQuota.fetch_sub(quota);
    }

    virtual bool TryAllocate(int64_t quota)
    {
        IE_LOG(TRACE1, "MemoryQuotaController[%p] TryAllocate[%ld], FreeQuota[%ld]",
               this, quota, mLeftQuota.load());
        int64_t leftQuota = mLeftQuota.load();
        int64_t targetLeftQuota = 0;
        do
        {
            if (leftQuota < quota)
            {
                return false;
            }
            targetLeftQuota = leftQuota - quota;
        }
        while(!mLeftQuota.compare_exchange_weak(leftQuota, targetLeftQuota));
        return true;
    }

    void Free(int64_t quota)
    {
        assert(quota >= 0);
        IE_LOG(TRACE1 , "MemoryQuotaController[%p] Free[%ld], FreeQuota[%ld]",
               this, quota, mLeftQuota.load());
        mLeftQuota.fetch_add(quota);
    }

    int64_t GetFreeQuota() const
    {
        return mLeftQuota.load();
    }
    
private:
    std::atomic<int64_t> mLeftQuota;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MemoryQuotaController);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_MEMORY_QUOTA_CONTROLLER_H
