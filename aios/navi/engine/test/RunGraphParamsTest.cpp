#include "unittest/unittest.h"
#include "navi/engine/RunGraphParams.h"

using namespace std;
using namespace testing;

namespace navi {

class RunGraphParamsTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void RunGraphParamsTest::setUp() {
}

void RunGraphParamsTest::tearDown() {
}

TEST_F(RunGraphParamsTest, testSimple) {
    {
        RunGraphParams params;
        EXPECT_FALSE(params.collectMetric());
        EXPECT_FALSE(params.collectPerf());
        EXPECT_EQ("DISABLE", params.getTraceLevelStr());

        params.setDebugParamStr("");

        EXPECT_FALSE(params.collectMetric());
        EXPECT_FALSE(params.collectPerf());
        EXPECT_EQ("DISABLE", params.getTraceLevelStr());
    }
    {
        RunGraphParams params;
        EXPECT_FALSE(params.collectMetric());
        EXPECT_FALSE(params.collectPerf());
        EXPECT_EQ("DISABLE", params.getTraceLevelStr());

        params.setDebugParamStr("abc");

        EXPECT_FALSE(params.collectMetric());
        EXPECT_FALSE(params.collectPerf());
        EXPECT_EQ("DISABLE", params.getTraceLevelStr());
    }
    {
        RunGraphParams params;
        EXPECT_FALSE(params.collectMetric());
        EXPECT_FALSE(params.collectPerf());
        EXPECT_EQ("DISABLE", params.getTraceLevelStr());

        params.setDebugParamStr("abc:true");

        EXPECT_FALSE(params.collectMetric());
        EXPECT_FALSE(params.collectPerf());
        EXPECT_EQ("DISABLE", params.getTraceLevelStr());
    }
    {
        RunGraphParams params;
        EXPECT_FALSE(params.collectMetric());
        EXPECT_FALSE(params.collectPerf());
        EXPECT_EQ("DISABLE", params.getTraceLevelStr());

        params.setDebugParamStr("456,navi_trace:debug,navi_metric:true,abc:true,navi_perf:true, 124");

        EXPECT_TRUE(params.collectMetric());
        EXPECT_TRUE(params.collectPerf());
        EXPECT_EQ("DEBUG", params.getTraceLevelStr());
    }
    {
        RunGraphParams params;
        params.setCollectMetric(true);
        params.setCollectPerf(true);
        params.setTraceLevel("info");

        params.setDebugParamStr("123, navi_trace:debug,navi_metric:false,abc:true,navi_perf:false");

        EXPECT_FALSE(params.collectMetric());
        EXPECT_FALSE(params.collectPerf());
        EXPECT_EQ("DEBUG", params.getTraceLevelStr());
    }
    {
        RunGraphParams params;
        params.setCollectMetric(true);
        params.setCollectPerf(true);
        params.setTraceLevel("info");
        EXPECT_TRUE(params.getTraceBtFilter().empty());

        params.setDebugParamStr("123, navi_trace:debug,navi_metric:false,abc:true,navi_perf:false, navi_bt_filter:aios/navi/engine/RunGraphParamsTest.cpp@18, navi_bt_filter:aios/navi/engine/RunGraphParams.cpp@19");

        EXPECT_FALSE(params.collectMetric());
        EXPECT_FALSE(params.collectPerf());
        EXPECT_EQ("DEBUG", params.getTraceLevelStr());
        const auto &btFilters = params.getTraceBtFilter();
        EXPECT_EQ(2u, btFilters.size());
        EXPECT_EQ("aios/navi/engine/RunGraphParamsTest.cpp", btFilters[0].first);
        EXPECT_EQ(18, btFilters[0].second);
        EXPECT_EQ("aios/navi/engine/RunGraphParams.cpp", btFilters[1].first);
        EXPECT_EQ(19, btFilters[1].second);
    }
    {
        RunGraphParams params;
        params.setCollectMetric(true);
        params.setCollectPerf(true);
        params.setTraceLevel("info");
        EXPECT_TRUE(params.getTraceBtFilter().empty());

        params.setDebugParamStr("123, navi_trace:debug:3,navi_metric:false:true,abc:true,navi_perf:false:::, navi_bt_filter:aios/navi/engine/RunGraphParamsTest.cpp@18:::, navi_bt_filter:aios/navi/engine/RunGraphParams.cpp@19::abc:");

        EXPECT_TRUE(params.collectMetric());
        EXPECT_FALSE(params.collectPerf());
        EXPECT_EQ("INFO", params.getTraceLevelStr());
        const auto &btFilters = params.getTraceBtFilter();
        EXPECT_EQ(1u, btFilters.size());
        EXPECT_EQ("aios/navi/engine/RunGraphParamsTest.cpp", btFilters[0].first);
        EXPECT_EQ(18, btFilters[0].second);
    }
}

}
