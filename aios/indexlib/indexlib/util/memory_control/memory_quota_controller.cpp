#include "indexlib/util/memory_control/memory_quota_controller.h"

IE_NAMESPACE_BEGIN(util);

IE_LOG_SETUP(util, MemoryQuotaController);

MemoryQuotaController::MemoryQuotaController(int64_t totalQuota)
    : mLeftQuota(totalQuota)
{
    IE_LOG(INFO, "Create MemoryQuotaController [%p] with totalQuota[%ld]", this, totalQuota);
}

MemoryQuotaController::~MemoryQuotaController()
{
    IE_LOG(INFO, "Release MemoryQuotaController [%p] with FreeQuota[%ld]",
           this, mLeftQuota.load());
}


IE_NAMESPACE_END(util);

