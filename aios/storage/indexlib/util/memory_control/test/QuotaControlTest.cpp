#include "indexlib/util/memory_control/QuotaControl.h"

#include "autil/Log.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace util {

class QuotaControlTest : public INDEXLIB_TESTBASE
{
public:
    QuotaControlTest();
    ~QuotaControlTest();

    DECLARE_CLASS_NAME(QuotaControlTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(QuotaControlTest, TestSimpleProcess);

AUTIL_LOG_SETUP(indexlib.util, QuotaControlTest);

QuotaControlTest::QuotaControlTest() {}

QuotaControlTest::~QuotaControlTest() {}

void QuotaControlTest::CaseSetUp() {}

void QuotaControlTest::CaseTearDown() {}

void QuotaControlTest::TestSimpleProcess()
{
    QuotaControl quotaControl(100);
    ASSERT_EQ((size_t)100, quotaControl.GetTotalQuota());
    ASSERT_TRUE(quotaControl.AllocateQuota(10));
    ASSERT_EQ((size_t)90, quotaControl.GetLeftQuota());
    ASSERT_FALSE(quotaControl.AllocateQuota(200));
}
}} // namespace indexlib::util
