#include "indexlib/config/BackgroundTaskConfig.h"

#include "autil/EnvUtil.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace config {

class BackgroundTaskConfigTest : public TESTBASE
{
public:
    BackgroundTaskConfigTest();
    ~BackgroundTaskConfigTest();

public:
    void setUp() override {}
    void tearDown() override {}
    void TestDefaultValue();
};

BackgroundTaskConfigTest::BackgroundTaskConfigTest() {}

BackgroundTaskConfigTest::~BackgroundTaskConfigTest() {}

TEST_F(BackgroundTaskConfigTest, TestDefaultValue)
{
    autil::EnvUtil::unsetEnv("IS_TEST_MODE");
    BackgroundTaskConfig config;
    ASSERT_EQ(10 * 1000, config.GetCleanResourceIntervalMs());
    ASSERT_EQ(60 * 60 * 1000u, config.GetRenewFenceLeaseIntervalMs());
    ASSERT_EQ(10 * 1000u, config.GetAsyncDumpIntervalMs());
    ASSERT_EQ(60 * 60 * 1000u, config.GetDumpIntervalMs());
    ASSERT_EQ(10 * 1000u, config.GetReportMetricsIntervalMs());
    ASSERT_EQ(60 * 1000u, config.GetMergeIntervalMs());
    ASSERT_EQ(1000u, config.GetMemReclaimIntervalMs());
    autil::EnvUtil::setEnv("IS_TEST_MODE", "true", true);
}

TEST_F(BackgroundTaskConfigTest, testInitFromEnv)
{
    {
        autil::EnvGuard guard("INDEXLIB_BACKGROUND_TASK_CONFIG", "");
        BackgroundTaskConfig config;
        ASSERT_TRUE(config.initFromEnv());
    }

    {
        autil::EnvGuard guard("INDEXLIB_BACKGROUND_TASK_CONFIG", "error json");
        BackgroundTaskConfig config;
        ASSERT_FALSE(config.initFromEnv());
    }

    {
        std::string jsonStr = R"doc({
"clean_resource_interval_ms" : -1,
"renew_fence_lease_interval_ms": -1,
"dump_interval_ms" : 100,
"async_dump_interval_ms" : 456,
"merge_interval_ms" : 789,
"mem_reclaim_interval_ms": 1000000
})doc";
        autil::EnvGuard guard("INDEXLIB_BACKGROUND_TASK_CONFIG", jsonStr);
        BackgroundTaskConfig config;
        ASSERT_TRUE(config.initFromEnv());

        EXPECT_EQ(-1, config.GetCleanResourceIntervalMs());
        EXPECT_EQ(-1, config.GetRenewFenceLeaseIntervalMs());
        EXPECT_EQ(100, config.GetDumpIntervalMs());
        EXPECT_EQ(10, config.GetReportMetricsIntervalMs());
        EXPECT_EQ(456, config.GetAsyncDumpIntervalMs());
        EXPECT_EQ(789, config.GetMergeIntervalMs());
        EXPECT_EQ(1000000, config.GetMemReclaimIntervalMs());
    }
}

}} // namespace indexlibv2::config
