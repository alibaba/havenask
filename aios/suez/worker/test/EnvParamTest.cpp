#include "suez/worker/EnvParam.h"

#include "autil/EnvUtil.h"
#include "suez/sdk/CmdLineDefine.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class EnvParamTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void EnvParamTest::setUp() {}

void EnvParamTest::tearDown() {}

TEST_F(EnvParamTest, testParseKMonitorTags) {
    {
        string input = "";
        map<string, string> tagsMap;
        ASSERT_TRUE(EnvParam::parseKMonitorTags(input, tagsMap));
        string output = EnvParam::tagsToString(tagsMap);
        ASSERT_EQ("", output);
    }
    {
        string input = "a^b";
        map<string, string> tagsMap;
        ASSERT_TRUE(EnvParam::parseKMonitorTags(input, tagsMap));
        string output = EnvParam::tagsToString(tagsMap);
        ASSERT_EQ("a^b", output);
    }
    {
        // override
        string input = "a^b@a^c";
        map<string, string> tagsMap;
        ASSERT_TRUE(EnvParam::parseKMonitorTags(input, tagsMap));
        string output = EnvParam::tagsToString(tagsMap);
        ASSERT_EQ("a^c", output);
    }
    {
        // with blank
        string input = " a^b  ";
        map<string, string> tagsMap;
        ASSERT_TRUE(EnvParam::parseKMonitorTags(input, tagsMap));
        string output = EnvParam::tagsToString(tagsMap);
        ASSERT_EQ("a^b", output);
    }
    {
        string input = "c^d@a^b";
        map<string, string> tagsMap;
        ASSERT_TRUE(EnvParam::parseKMonitorTags(input, tagsMap));
        string output = EnvParam::tagsToString(tagsMap);
        ASSERT_EQ("a^b@c^d", output);
    }
    {
        string input = " c^d@ a^ b ";
        map<string, string> tagsMap;
        ASSERT_TRUE(EnvParam::parseKMonitorTags(input, tagsMap));
        string output = EnvParam::tagsToString(tagsMap);
        ASSERT_EQ("a^b@c^d", output);
    }
    {
        // trim by split
        string input = "a^b@";
        map<string, string> tagsMap;
        ASSERT_TRUE(EnvParam::parseKMonitorTags(input, tagsMap));
        string output = EnvParam::tagsToString(tagsMap);
        ASSERT_EQ("a^b", output);
    }
}

TEST_F(EnvParamTest, testParseKMonitorTags_Failed) {
    {
        string input = "a@c^d";
        map<string, string> tagsMap;
        ASSERT_FALSE(EnvParam::parseKMonitorTags(input, tagsMap));
    }
    {
        string input = "^^";
        map<string, string> tagsMap;
        ASSERT_FALSE(EnvParam::parseKMonitorTags(input, tagsMap));
    }
}

TEST_F(EnvParamTest, testTagsToString) {
    {
        map<string, string> tagsMap = {};
        string output = EnvParam::tagsToString(tagsMap);
        ASSERT_EQ("", output);
    }
    {
        map<string, string> tagsMap = {{"a", "b"}};
        string output = EnvParam::tagsToString(tagsMap);
        ASSERT_EQ("a^b", output);
    }
    {
        map<string, string> tagsMap = {{"a", "b"}, {"c", "d"}};
        string output = EnvParam::tagsToString(tagsMap);
        ASSERT_EQ("a^b@c^d", output);
    }
}

TEST_F(EnvParamTest, testTryExtractPosIntValueFromEnv) {
    EnvParam param;
    ASSERT_EQ(10, param.dpThreadNum);
    autil::EnvGuard env1(DP_THREAD_NUM, "20");
    ASSERT_TRUE(param.tryExtractPosIntValueFromEnv(DP_THREAD_NUM, param.dpThreadNum));
    ASSERT_EQ(20, param.dpThreadNum);
    autil::EnvGuard env2(DP_THREAD_NUM, "1");
    ASSERT_TRUE(param.tryExtractPosIntValueFromEnv(DP_THREAD_NUM, param.dpThreadNum));
    ASSERT_EQ(1, param.dpThreadNum);
    autil::EnvGuard env3(DP_THREAD_NUM, "0");
    ASSERT_FALSE(param.tryExtractPosIntValueFromEnv(DP_THREAD_NUM, param.dpThreadNum));
    autil::EnvGuard env4(DP_THREAD_NUM, "-1");
    ASSERT_FALSE(param.tryExtractPosIntValueFromEnv(DP_THREAD_NUM, param.dpThreadNum));
    autil::EnvGuard env5(DP_THREAD_NUM, "abc");
    ASSERT_FALSE(param.tryExtractPosIntValueFromEnv(DP_THREAD_NUM, param.dpThreadNum));
}

TEST_F(EnvParamTest, testInit) {
    autil::EnvGuard guard1("HIPPO_DP2_SLAVE_PORT", "12345");
    autil::EnvGuard guard4("debugMode", "true");
    autil::EnvGuard guard5("localMode", "true");
    EnvParam param;
    ASSERT_FALSE(param.debugMode);
    ASSERT_FALSE(param.localMode);
    ASSERT_TRUE(param.init());
    ASSERT_TRUE(param.debugMode);
    ASSERT_TRUE(param.localMode);
    ASSERT_EQ("12345", param.hippoDp2SlavePort);
    // TODO: check other parameters
}

TEST_F(EnvParamTest, testInitRdma) {
    autil::EnvGuard guardDeps("HIPPO_DP2_SLAVE_PORT", "12345");
    {
        // empty init
        EnvParam param;
        EXPECT_EQ("", param.rdmaPort);
        EXPECT_EQ(8, param.rdmaRpcThreadNum);
        EXPECT_EQ(1000, param.rdmaRpcQueueSize);
        EXPECT_EQ(2, param.rdmaIoThreadNum);
        EXPECT_EQ(5, param.rdmaWorkerThreadNum);

        ASSERT_TRUE(param.init());
        EXPECT_EQ("", param.rdmaPort);
        EXPECT_EQ(8, param.rdmaRpcThreadNum);
        EXPECT_EQ(1000, param.rdmaRpcQueueSize);
        EXPECT_EQ(2, param.rdmaIoThreadNum);
        EXPECT_EQ(5, param.rdmaWorkerThreadNum);
    }
    {
        // init on env param
        autil::EnvGuard guard0("rdmaPort", "12345");
        autil::EnvGuard guard1("rdmaRpcThreadNum", "10");
        autil::EnvGuard guard2("rdmaRpcQueueSize", "2000");
        autil::EnvGuard guard3("rdmaWorkerThreadNum", "10");
        autil::EnvGuard guard4("rdmaIoThreadNum", "10");

        EnvParam param;
        EXPECT_EQ("", param.rdmaPort);
        EXPECT_EQ(8, param.rdmaRpcThreadNum);
        EXPECT_EQ(1000, param.rdmaRpcQueueSize);
        EXPECT_EQ(2, param.rdmaIoThreadNum);
        EXPECT_EQ(5, param.rdmaWorkerThreadNum);

        ASSERT_TRUE(param.init());
        EXPECT_EQ("12345", param.rdmaPort);
        EXPECT_EQ(10, param.rdmaRpcThreadNum);
        EXPECT_EQ(2000, param.rdmaRpcQueueSize);
        EXPECT_EQ(10, param.rdmaIoThreadNum);
        EXPECT_EQ(10, param.rdmaWorkerThreadNum);
    }
}

} // namespace suez
