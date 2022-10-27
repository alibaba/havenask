#include "indexlib/util/memory_control/test/quota_control_unittest.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, QuotaControlTest);

QuotaControlTest::QuotaControlTest()
{
}

QuotaControlTest::~QuotaControlTest()
{
}

void QuotaControlTest::CaseSetUp()
{
}

void QuotaControlTest::CaseTearDown()
{
}

void QuotaControlTest::TestSimpleProcess()
{
    QuotaControl quotaControl(100);
    ASSERT_EQ((size_t)100, quotaControl.GetTotalQuota());
    ASSERT_TRUE(quotaControl.AllocateQuota(10));
    ASSERT_EQ((size_t)90, quotaControl.GetLeftQuota());
    ASSERT_FALSE(quotaControl.AllocateQuota(200));
}

IE_NAMESPACE_END(util);

