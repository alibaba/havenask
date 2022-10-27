#ifndef __INDEXLIB_FILE_NODE_MEMORY_CONTROLLER_H
#define __INDEXLIB_FILE_NODE_MEMORY_CONTROLLER_H

#include <tr1/memory>
#include <atomic>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/memory_control/block_memory_quota_controller.h"

IE_NAMESPACE_BEGIN(util);

template<typename T>
class SimpleMemoryQuotaControllerType
{
public:
    SimpleMemoryQuotaControllerType(const BlockMemoryQuotaControllerPtr& memController)
        : mUsedQuota(0)
        , mMemController(memController)
    {
        assert(memController);
    }
    
    ~SimpleMemoryQuotaControllerType()
    {
        mMemController->Free(mUsedQuota);
    }

public:
    void Allocate(int64_t quota)
    {
        mUsedQuota += quota;
        mMemController->Allocate(quota);
    }

    void Free(int64_t quota)
    {
        mUsedQuota -= quota;
        mMemController->Free(quota);
    }

    int64_t GetFreeQuota() const
    { return mMemController->GetFreeQuota(); }

    int64_t GetUsedQuota() const
    { return mUsedQuota; }

    const BlockMemoryQuotaControllerPtr& GetBlockMemoryController() const
    { return mMemController; }

private:
    T mUsedQuota;
    BlockMemoryQuotaControllerPtr mMemController;

private:
    IE_LOG_DECLARE();
};

typedef SimpleMemoryQuotaControllerType<std::atomic<int64_t>> SimpleMemoryQuotaController;
typedef SimpleMemoryQuotaControllerType<int64_t> UnsafeSimpleMemoryQuotaController;

DEFINE_SHARED_PTR(SimpleMemoryQuotaController);
DEFINE_SHARED_PTR(UnsafeSimpleMemoryQuotaController);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_FILE_NODE_MEMORY_CONTROLLER_H
