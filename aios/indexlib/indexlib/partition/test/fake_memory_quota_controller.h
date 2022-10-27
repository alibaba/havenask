#ifndef __INDEXLIB_MOCK_MEMORY_QUOTA_CONTROLLER_H
#define __INDEXLIB_MOCK_MEMORY_QUOTA_CONTROLLER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/memory_control/memory_quota_controller.h"

IE_NAMESPACE_BEGIN(partition);

class FakeMemoryQuotaController : public util::MemoryQuotaController
{
public:
    FakeMemoryQuotaController(int64_t totalQuota, int64_t normalCounter, int64_t continueFailIdx)
        : MemoryQuotaController(totalQuota)
        , mNormalCounter(normalCounter)
        , mContinueFailIdx(continueFailIdx)
    {}
    ~FakeMemoryQuotaController() {}
public:
    bool TryAllocate(int64_t quota)
    {
        if (quota == 0)
        {
            return true;
        }
        mNormalCounter--;
        if (mNormalCounter == 0)
        {
            return false;
        }
        mContinueFailIdx--;
        if (mContinueFailIdx == 0)
        {
            return false;
        }
        return true;
    }

    bool HasTriggeredFirstError() { return mNormalCounter <=0; }
    bool HasTriggeredSecondError() { return mContinueFailIdx <=0; }

private:
    int64_t mNormalCounter;
    int64_t mContinueFailIdx;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeMemoryQuotaController);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_MOCK_MEMORY_QUOTA_CONTROLLER_H
