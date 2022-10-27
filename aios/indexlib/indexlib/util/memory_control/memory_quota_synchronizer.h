#ifndef __INDEXLIB_MEMORY_QUOTA_SYNCHRONIZER_H
#define __INDEXLIB_MEMORY_QUOTA_SYNCHRONIZER_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/memory_control/simple_memory_quota_controller.h"

IE_NAMESPACE_BEGIN(util);

class MemoryQuotaSynchronizer
{
public:
    MemoryQuotaSynchronizer(const BlockMemoryQuotaControllerPtr& memController);
    ~MemoryQuotaSynchronizer();

public:
    void SyncMemoryQuota(int64_t memoryUse);
    int64_t GetFreeQuota() const { return mSimpleMemControl.GetFreeQuota(); }

private:
    SimpleMemoryQuotaController mSimpleMemControl;
    int64_t mLastMemoryUse;
    autil::ThreadMutex mLock;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MemoryQuotaSynchronizer);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_MEMORY_QUOTA_SYNCHRONIZER_H
