#include "sql/config/NaviConfigAdapter.h"

#include <cstddef>
#include <map>
#include <string>
#include <unistd.h>

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "multi_call/config/MultiCallConfig.h"
#include "navi/common.h"
#include "navi/config/NaviConfig.h"
#include "navi/log/NaviLoggerProvider.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;

namespace sql {

class NaviConfigAdapterTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(sql, NaviConfigAdapterTest);

void NaviConfigAdapterTest::setUp() {}

void NaviConfigAdapterTest::tearDown() {}

TEST_F(NaviConfigAdapterTest, testParseThreadPoolConfigThreadNum) {
    {
        EnvGuard _("naviThreadNum", "10");
        NaviConfigAdapter adapter;
        adapter.parseThreadPoolConfigThreadNum();
        ASSERT_EQ(10, adapter._threadNum);
    }
    {
        EnvGuard _0("naviThreadNum", "10");
        EnvGuard _1("threadNumScaleFactor", "1.1");
        NaviConfigAdapter adapter;
        navi::NaviLoggerProvider provider("WARN");
        // todo check trace
        adapter.parseThreadPoolConfigThreadNum();
        ASSERT_EQ(10, adapter._threadNum);
    }
    {
        EnvGuard _0("naviThreadNum", "0");
        EnvGuard _1("threadNumScaleFactor", "1.1");
        NaviConfigAdapter adapter;
        navi::NaviLoggerProvider provider("WARN");
        // todo check trace
        adapter.parseThreadPoolConfigThreadNum();
        ASSERT_EQ(0, adapter._threadNum);
    }
    {
        EnvGuard _0("naviThreadNum", "-10");
        EnvGuard _1("threadNumScaleFactor", "1.1");
        NaviConfigAdapter adapter;
        navi::NaviLoggerProvider provider("WARN");
        // todo check trace
        adapter.parseThreadPoolConfigThreadNum();
        ASSERT_EQ(-10, adapter._threadNum);
    }
    {
        EnvGuard _("threadNumScaleFactor", "1.5");
        NaviConfigAdapter adapter;
        adapter.parseThreadPoolConfigThreadNum();
        ASSERT_EQ((size_t)(1.5 * sysconf(_SC_NPROCESSORS_ONLN)), adapter._threadNum);
    }
    {
        NaviConfigAdapter adapter;
        adapter.parseThreadPoolConfigThreadNum();
        ASSERT_EQ(0, adapter._threadNum);
    }
}

TEST_F(NaviConfigAdapterTest, testParseExtraQueueConfig) {
    std::string config = "slow_queue|1|10|1;fast_queue|4|10|4";
    EnvGuard _("naviExtraTaskQueue", config);
    NaviConfigAdapter adapter;
    ASSERT_TRUE(adapter.parseExtraTaskQueueConfig());
    auto &queueMap = adapter._extraTaskQueueMap;
    ASSERT_EQ(2, queueMap.size());
    ASSERT_EQ(1, queueMap["slow_queue"].threadNum);
    ASSERT_EQ(10, queueMap["slow_queue"].queueSize);
    ASSERT_EQ(1, queueMap["slow_queue"].processingSize);
    ASSERT_EQ(4, queueMap["fast_queue"].threadNum);
    ASSERT_EQ(10, queueMap["fast_queue"].queueSize);
    ASSERT_EQ(4, queueMap["fast_queue"].processingSize);
}

TEST_F(NaviConfigAdapterTest, testParseExtraQueueConfig_empty) {
    std::string config = "";
    EnvGuard _("naviExtraTaskQueue", config);
    NaviConfigAdapter adapter;
    ASSERT_TRUE(adapter.parseExtraTaskQueueConfig());
    ASSERT_EQ(0, adapter._extraTaskQueueMap.size());
}

TEST_F(NaviConfigAdapterTest, testParseExtraQueueConfig_error) {
    {
        std::string config = " ";
        EnvGuard _("naviExtraTaskQueue", config);
        NaviConfigAdapter adapter;
        ASSERT_FALSE(adapter.parseExtraTaskQueueConfig());
    }
    {
        std::string config = "queue|1|10;queue2|1|1|1";
        EnvGuard _("naviExtraTaskQueue", config);
        NaviConfigAdapter adapter;
        ASSERT_FALSE(adapter.parseExtraTaskQueueConfig());
        ASSERT_EQ(0, adapter._extraTaskQueueMap.size());
    }
    {
        std::string config = "queue|1|1|1;queue|1|1|1";
        EnvGuard _("naviExtraTaskQueue", config);
        NaviConfigAdapter adapter;
        ASSERT_FALSE(adapter.parseExtraTaskQueueConfig());
    }
}

TEST_F(NaviConfigAdapterTest, parseThreadPoolConfigQueueConfig) {
    {
        EnvGuard _("naviQueueSize", "1111");
        NaviConfigAdapter adapter;
        adapter.parseThreadPoolConfigQueueConfig();
        ASSERT_EQ(1111, adapter._queueSize);
    }
    {
        NaviConfigAdapter adapter;
        adapter.parseThreadPoolConfigQueueConfig();
        ASSERT_EQ(navi::DEFAULT_QUEUE_SIZE, adapter._queueSize);
    }
    {
        EnvGuard _("naviProcessingSize", "20");
        NaviConfigAdapter adapter;
        adapter.parseThreadPoolConfigQueueConfig();
        ASSERT_EQ(20, adapter._processingSize);
    }
    {
        NaviConfigAdapter adapter;
        adapter.parseThreadPoolConfigQueueConfig();
        ASSERT_EQ(navi::DEFAULT_PROCESSING_SIZE, adapter._processingSize);
    }
}

TEST_F(NaviConfigAdapterTest, testParseLogConfig) {
    {
        NaviConfigAdapter adapter;
        adapter.parseLogConfig();
        ASSERT_EQ(NaviConfigAdapter::DEFAULT_LOG_LEVEL, adapter._logLevel);
        ASSERT_EQ(NaviConfigAdapter::DEFAULT_LOG_KEEP_COUNT, adapter._logKeepCount);
    }
    {
        EnvGuard _("naviLogLevel", "ERROR");
        NaviConfigAdapter adapter;
        adapter.parseLogConfig();
        ASSERT_EQ("ERROR", adapter._logLevel);
    }
    {
        EnvGuard _("naviLogKeepCount", "20");
        NaviConfigAdapter adapter;
        adapter.parseLogConfig();
        ASSERT_EQ(20, adapter._logKeepCount);
    }
}

TEST_F(NaviConfigAdapterTest, testParseGrpcConfig) {
    NaviConfigAdapter adapter;
    EnvGuard a("grpcProtocolThreadNum", "1");
    EnvGuard b("grpcProtocolKeepAliveInterval", "1000");
    EnvGuard c("grpcProtocolKeepAliveTimeout", "500");
    adapter.parseGrpcConfig();
    ASSERT_EQ(1, adapter._grpcProtocolThreadNum);
    ASSERT_EQ(1000, adapter._grpcProtocolKeepAliveInterval);
    ASSERT_EQ(500, adapter._grpcProtocolKeepAliveTimeout);
}

TEST_F(NaviConfigAdapterTest, testSetGigClientConfig) {
    NaviConfigAdapter adapter;
    adapter._grpcProtocolThreadNum = 2;
    adapter._grpcProtocolKeepAliveInterval = 1000;
    adapter._grpcProtocolKeepAliveTimeout = 500;

    multi_call::MultiCallConfig mcConfig;
    adapter.setGigClientConfig(mcConfig, {});
    auto &protocolConf = adapter._mcConfig.connectConf["grpc_stream"];
    ASSERT_EQ(2, protocolConf.threadNum);
    ASSERT_EQ(1000, protocolConf.keepAliveInterval);
    ASSERT_EQ(500, protocolConf.keepAliveTimeout);
}

TEST_F(NaviConfigAdapterTest, testParseMiscConfig) {
    {
        EnvGuard _("naviDisablePerf", "true");
        NaviConfigAdapter adapter;
        adapter.parseMiscConfig();
        ASSERT_TRUE(adapter._disablePerf);
    }
    {
        NaviConfigAdapter adapter;
        adapter.parseMiscConfig();
        ASSERT_FALSE(adapter._disablePerf);
    }
}

} // namespace sql
