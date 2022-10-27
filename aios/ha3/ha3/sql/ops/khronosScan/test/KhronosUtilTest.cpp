#include <unittest/unittest.h>
#include <autil/StringUtil.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/test/test.h>
#include <ha3/sql/ops/khronosScan/KhronosDataConditionVisitor.h>
#include <ha3/sql/ops/khronosScan/KhronosUtil.h>

using namespace std;
using namespace testing;
using namespace khronos::search;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil_rapidjson;

BEGIN_HA3_NAMESPACE(sql);

class KhronosUtilTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    autil::mem_pool::Pool _pool;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(khronosScan, KhronosUtilTest);

void KhronosUtilTest::setUp() {
}

void KhronosUtilTest::tearDown() {
}

TEST_F(KhronosUtilTest, makeWildcardToRegex_Success) {
    string wildcard = R"(aa\.aa.*aa)";
    string regex = KhronosUtil::makeWildcardToRegex(wildcard);
    ASSERT_EQ(R"(^aa\\\.aa\..*aa$)", regex);
}

TEST_F(KhronosUtilTest, updateTimeRange_Success_NegativeValue) {
    int64_t value = -1800;
    {
        // timestamp >= now - 0.5h
        string op = SQL_GE_OP;
        TimeRange tsRange = TimeRange::createAllTimeRange();
        auto now = TimeUtility::currentTimeInSeconds();
        KhronosUtil::updateTimeRange(op, value, tsRange);
        ASSERT_LE(now + value, tsRange.begin);
    }

    {
        // timestamp > now - 0.5h
        string op = SQL_GT_OP;
        TimeRange tsRange = TimeRange::createAllTimeRange();
        auto now = TimeUtility::currentTimeInSeconds();
        KhronosUtil::updateTimeRange(op, value, tsRange);
        ASSERT_LT(now + value, tsRange.begin);
    }
    {
        // timestamp <= now - 0.5h
        string op = SQL_LE_OP;
        TimeRange tsRange = TimeRange::createAllTimeRange();
        KhronosUtil::updateTimeRange(op, value, tsRange);
        auto now = TimeUtility::currentTimeInSeconds();
        ASSERT_GE(now + value + 1, tsRange.end);
    }
    {
        // timestamp < now - 0.5h
        string op = SQL_LT_OP;
        TimeRange tsRange = TimeRange::createAllTimeRange();
        auto now = TimeUtility::currentTimeInSeconds();
        KhronosUtil::updateTimeRange(op, value, tsRange);
        ASSERT_GT(now + value + 1, tsRange.end);
    }
}

END_HA3_NAMESPACE();
