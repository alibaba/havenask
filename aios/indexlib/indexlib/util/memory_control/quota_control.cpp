#include "indexlib/util/memory_control/quota_control.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, QuotaControl);

QuotaControl::QuotaControl(size_t totalQuota)
    : mTotalQuota(totalQuota)
    , mLeftQuota(totalQuota)
{
}

QuotaControl::~QuotaControl() 
{
}

IE_NAMESPACE_END(util);

