#ifndef __INDEXLIB_BLOCK_MEMORY_QUOTA_CONTROLLER_H
#define __INDEXLIB_BLOCK_MEMORY_QUOTA_CONTROLLER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/memory_control/partition_memory_quota_controller.h"

IE_NAMESPACE_BEGIN(util);

class BlockMemoryQuotaController
{
public:
    BlockMemoryQuotaController(
        const PartitionMemoryQuotaControllerPtr& partitionMemController,
        std::string name = "default_block_memory_quota_controll")
        : mName(name)
        , mLeftQuota(0)
        , mUsedQuota(0)
        , mPartitionMemController(partitionMemController)
    {}
    ~BlockMemoryQuotaController() 
    {
        DoFree(mLeftQuota.load());
        assert(mUsedQuota.load() == 0);
    }

public:
    void Allocate(int64_t quota)
    {
        assert(quota >= 0);
        mLeftQuota.fetch_sub(quota);
        if (mLeftQuota.load() >= 0)
        {
            return;
        }
        int64_t leftQuota = mLeftQuota.load();
        int64_t targetQuota = 0;
        int64_t allocateSize = 0;
        do
        {
            allocateSize = CalculateAllocateSize(leftQuota);
            targetQuota = leftQuota + allocateSize;
        }
        while(!mLeftQuota.compare_exchange_weak(leftQuota, targetQuota));
        DoAllocate(allocateSize);
    }

    void ShrinkToFit()
    {
        Free(0);
    }

    bool Reserve(int64_t quota)
    {
        assert(quota >= 0);
        if (quota > GetFreeQuota())
        {
            return false;
        }
        int64_t reserveSize = (quota + BLOCK_SIZE - 1) & (~BLOCK_SIZE_MASK);
        if (!mPartitionMemController->TryAllocate(reserveSize))
        {
            return false;
        }
        mLeftQuota.fetch_add(reserveSize);
        mUsedQuota.fetch_add(reserveSize);
        return true;
    }

    void Free(int64_t quota)
    {
        assert(quota >= 0);
        mLeftQuota.fetch_add(quota);
        int64_t leftQuota = mLeftQuota.load();
        int64_t freeSize = 0;
        int64_t targetQuota = 0;
        do 
        {
            freeSize = CalulateFreeSize(leftQuota);
            targetQuota = leftQuota - freeSize;
        }
        while(!mLeftQuota.compare_exchange_weak(leftQuota, targetQuota));
        DoFree(freeSize);
    }

    int64_t GetUsedQuota() const
    { return mUsedQuota.load(); }

    int64_t GetAllocatedQuota() const
    { return mUsedQuota.load() - mLeftQuota.load(); }

    const std::string& GetName() const
    { return mName; }

    const PartitionMemoryQuotaControllerPtr& GetPartitionMemoryQuotaController() const
    { return mPartitionMemController; }

    int64_t GetFreeQuota() const
    { return mPartitionMemController->GetFreeQuota(); }

public:
    static const int64_t BLOCK_SIZE = 4 * 1024 * 1024;
    static const int64_t BLOCK_SIZE_MASK = BLOCK_SIZE - 1;
private:
    int64_t CalculateAllocateSize(int64_t leftQuota)
    {
        if (leftQuota >= 0)
        {
            return 0;
        }
        return (-leftQuota + BLOCK_SIZE - 1) & (~BLOCK_SIZE_MASK);
    }

    int64_t CalulateFreeSize(int64_t leftQuota)
    {
        if (leftQuota <= 0)
        {
            return 0;
        }
        return leftQuota & (~BLOCK_SIZE_MASK);
    }

    void DoAllocate(int64_t quota)
    {
        mPartitionMemController->Allocate(quota);
        mUsedQuota.fetch_add(quota);
    }

    void DoFree(int64_t quota)
    {
        mPartitionMemController->Free(quota);
        mUsedQuota.fetch_sub(quota);
    }

private:
    std::string mName;
    std::atomic<int64_t> mLeftQuota;
    // quota allocated from mPartitionMemController, including quota in mLeftQuota
    std::atomic<int64_t> mUsedQuota;
    PartitionMemoryQuotaControllerPtr mPartitionMemController;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BlockMemoryQuotaController);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BLOCK_MEMORY_QUOTA_CONTROLLER_H
