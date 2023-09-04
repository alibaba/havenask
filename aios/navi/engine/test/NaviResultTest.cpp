#include "unittest/unittest.h"
#include "navi/engine/NaviResult.h"

using namespace std;
using namespace testing;
using namespace autil;
using namespace multi_call;

namespace navi {

class NaviResultTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void NaviResultTest::setUp() {
}

void NaviResultTest::tearDown() {
}

static void compare(const LoggingEvent &lhs, const LoggingEvent &rhs) {
    ASSERT_EQ(lhs.name, rhs.name);
    ASSERT_EQ(lhs.object, rhs.object);
    ASSERT_EQ(lhs.message, rhs.message);
    ASSERT_EQ(lhs.func, rhs.func);
}

TEST_F(NaviResultTest, testSimple) {
    DataBuffer dataBuffer;
    auto logEvent = LoggingEvent("a", (void *)0xDEADBEEF, "c", "d", LOG_LEVEL_DEBUG, "e", 1, "f");
    GigStreamRpcInfo rpcInfo;
    rpcInfo.partId = 10;
    rpcInfo.spec = "10.10.10.11:1111";
    rpcInfo.sendCount = 1111;
    rpcInfo.receiveCount = 2222;
    GigStreamRpcInfoVec rpcInfoVec = {rpcInfo};

    {
        NaviResult naviResult;
        naviResult.ec = EC_ABORT;
        naviResult.errorEvent = logEvent;
        naviResult.errorBackTrace = "bt";
        naviResult.traceCollector.append(logEvent);
        naviResult.graphMetric._kernelMetricMap["aaa"] = {};
        naviResult.collectMetric = true;
        naviResult.rpcInfoMap.emplace(make_pair("test_src", "test_dst"), rpcInfoVec);
        naviResult.serialize(dataBuffer);
    }
    {
        NaviResult naviResult;
        naviResult.deserialize(dataBuffer);
        ASSERT_EQ(EC_ABORT
                  , naviResult.ec);
        ASSERT_NO_FATAL_FAILURE(compare(logEvent, naviResult.errorEvent));
        ASSERT_EQ("bt", naviResult.errorBackTrace);
        ASSERT_EQ(1, naviResult.traceCollector.eventSize());
        ASSERT_NO_FATAL_FAILURE(compare(logEvent, *naviResult.traceCollector._eventList.begin()));
        ASSERT_TRUE(naviResult.collectMetric);
        ASSERT_EQ(1, naviResult.graphMetric._kernelMetricMap.count("aaa"));
        auto it = naviResult.rpcInfoMap.find(make_pair("test_src", "test_dst"));
        ASSERT_NE(naviResult.rpcInfoMap.end(), it);
        ASSERT_EQ(1, it->second.size());
        ASSERT_EQ(rpcInfo, it->second[0]);
    }
}

}
