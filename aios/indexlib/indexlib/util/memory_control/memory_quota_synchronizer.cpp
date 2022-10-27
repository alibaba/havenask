#include "indexlib/util/memory_control/memory_quota_synchronizer.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, MemoryQuotaSynchronizer);

MemoryQuotaSynchronizer::MemoryQuotaSynchronizer(
        const BlockMemoryQuotaControllerPtr& memController)
    : mSimpleMemControl(memController)
    , mLastMemoryUse(0)
{
}

MemoryQuotaSynchronizer::~MemoryQuotaSynchronizer() 
{
}

void MemoryQuotaSynchronizer::SyncMemoryQuota(int64_t memoryUse)
{
    ScopedLock lock(mLock);
    if (memoryUse == mLastMemoryUse)
    {
        return;
    }
    
    if (memoryUse > mLastMemoryUse)
    {
        mSimpleMemControl.Allocate(memoryUse - mLastMemoryUse);
    }
    else
    {
        mSimpleMemControl.Free(mLastMemoryUse - memoryUse);
    }
    mLastMemoryUse = memoryUse;
}

IE_NAMESPACE_END(util);

