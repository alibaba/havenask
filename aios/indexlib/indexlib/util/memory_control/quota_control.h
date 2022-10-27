#ifndef __INDEXLIB_QUOTA_CONTROL_H
#define __INDEXLIB_QUOTA_CONTROL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

class QuotaControl
{
public:
    QuotaControl(size_t totalQuota);
    ~QuotaControl();
public:
    size_t GetTotalQuota() const
    { return mTotalQuota; }
    size_t GetLeftQuota() const
    { return mLeftQuota; }

    bool AllocateQuota(size_t quota)
    {
        if (quota > mLeftQuota)
        {
            return false;
        }
        mLeftQuota -= quota;
        return true;
    }

private:
    size_t mTotalQuota;
    size_t mLeftQuota;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(QuotaControl);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_QUOTA_CONTROL_H
