#include "indexlib/util/memory_control/memory_reserver.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, MemoryReserver);

MemoryReserver::MemoryReserver(const string& name,
                               const BlockMemoryQuotaControllerPtr& memController)
    : mName(name)
    , mMemController(memController)
{
}

MemoryReserver::~MemoryReserver() 
{
    mMemController->ShrinkToFit();
}

bool MemoryReserver::Reserve(int64_t quota)
{
    if (quota == 0)
    {
        IE_LOG(INFO, "[%s] Reserve [0], OK", mName.c_str());
        return true;
    }
    const PartitionMemoryQuotaControllerPtr& partMemController =
        mMemController->GetPartitionMemoryQuotaController();
    IE_LOG(INFO, "[%s] Reserve [%ld], current FreeQuota [%ld] "
           "Partition[%s] used quota [%ld], current block[%s] used quota [%ld]",
           mName.c_str(), quota, mMemController->GetFreeQuota(),
           partMemController->GetName().c_str(), partMemController->GetUsedQuota(),
           mMemController->GetName().c_str(), mMemController->GetUsedQuota());
    if (mMemController->Reserve(quota))
    {
        IE_LOG(INFO, "Reserve quota [%ld] sucuess, FreeQuota [%ld]",
               quota, mMemController->GetFreeQuota());
        return true;
    }
    IE_LOG(WARN, "Reserve quota [%ld] failed, FreeQuota [%ld]",
           quota, mMemController->GetFreeQuota());
    return false;
}

IE_NAMESPACE_END(util);

