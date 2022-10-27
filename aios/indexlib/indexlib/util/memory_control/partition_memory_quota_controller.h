#ifndef __INDEXLIB_PARTITION_MEMORY_QUOTA_CONTROLLER_H
#define __INDEXLIB_PARTITION_MEMORY_QUOTA_CONTROLLER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/memory_control/memory_quota_controller.h"
#include "indexlib/util/memory_control/quota_control.h"

IE_NAMESPACE_BEGIN(util);

class PartitionMemoryQuotaController
{
public:
    PartitionMemoryQuotaController(const MemoryQuotaControllerPtr& memController,
                                   const std::string& name = "default_partition")
        : mName(name)
        , mTotalMemoryController(memController)
        , mUsedQuota(0)
    {}
    ~PartitionMemoryQuotaController()
    {
        assert(mUsedQuota.load() == 0);
        mTotalMemoryController->Free(mUsedQuota.load());
    }
public:
    void Allocate(int64_t quota)
    {
        assert(quota >= 0);
        if (quota <= 0)
        {
            return;
        }
        mTotalMemoryController->Allocate(quota);
        mUsedQuota.fetch_add(quota);
    }

    bool TryAllocate(int64_t quota)
    {
        if (mTotalMemoryController->TryAllocate(quota))
        {
            mUsedQuota.fetch_add(quota);
            return true;
        }
        return false;
    }

    void Free(int64_t quota)
    {
        assert(quota >= 0);
        if (quota <= 0)
        {
            return;
        }
        mTotalMemoryController->Free(quota);
        mUsedQuota.fetch_sub(quota);
    }

    int64_t GetFreeQuota() const
    {
        return mTotalMemoryController->GetFreeQuota();
    }

    int64_t GetUsedQuota() const
    {
        return mUsedQuota.load();
    }

    const std::string& GetName() const
    { return mName; }

public:
    // TODO: remove this
    const QuotaControlPtr& GetBuildMemoryQuotaControler() const
    {
        return mBuildMemoryQuotaControler;
    }
    void SetBuildMemoryQuotaControler(const QuotaControlPtr& buildMemoryQuotaControler)
    {
        mBuildMemoryQuotaControler = buildMemoryQuotaControler;
    }
private:
    std::string mName;
    MemoryQuotaControllerPtr mTotalMemoryController;
    std::atomic<int64_t> mUsedQuota;
    QuotaControlPtr mBuildMemoryQuotaControler;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionMemoryQuotaController);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_PARTITION_MEMORY_QUOTA_CONTROLLER_H
