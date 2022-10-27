#ifndef __INDEXLIB_MEMORY_RESERVER_H
#define __INDEXLIB_MEMORY_RESERVER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/memory_control/block_memory_quota_controller.h"

IE_NAMESPACE_BEGIN(util);

class MemoryReserver
{
public:
    MemoryReserver(const std::string& name,
                   const BlockMemoryQuotaControllerPtr& memController);
    ~MemoryReserver();
public:
    bool Reserve(int64_t quota);
    int64_t GetFreeQuota() const { return mMemController->GetFreeQuota(); }

private:
    std::string mName;
    BlockMemoryQuotaControllerPtr mMemController;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MemoryReserver);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_MEMORY_RESERVER_H
