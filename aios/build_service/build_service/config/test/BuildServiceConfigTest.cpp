#include "build_service/test/unittest.h"
#include "build_service/config/BuildServiceConfig.h"

using namespace std;
using namespace testing;

namespace build_service {
namespace config {

class BuildServiceConfigTest : public BUILD_SERVICE_TESTBASE {};

TEST_F(BuildServiceConfigTest, testSimple) {
    BuildServiceConfig config;
    EXPECT_EQ(10086, config.amonitorPort);
    config.userName = "user";
    config.serviceName = "service";
    config._indexRoot = "index";
    config.swiftRoot = "swift";
    config.hippoRoot = "hippo";
    config.zkRoot = "zk";
    EXPECT_TRUE(config.validate());

#define VALIDATE_EMPTY_FIELD(field)             \
    do {                                        \
        BuildServiceConfig testConfig = config; \
        testConfig.field = "";                  \
        EXPECT_FALSE(testConfig.validate());    \
    } while (0)

    VALIDATE_EMPTY_FIELD(userName);
    VALIDATE_EMPTY_FIELD(serviceName);
    VALIDATE_EMPTY_FIELD(_indexRoot);
    VALIDATE_EMPTY_FIELD(swiftRoot);
    VALIDATE_EMPTY_FIELD(hippoRoot);
    VALIDATE_EMPTY_FIELD(zkRoot);
}

TEST_F(BuildServiceConfigTest, testGetBuilderIndexRoot) {
    BuildServiceConfig config;
    config._indexRoot = "index";
    config._fullBuilderTmpRoot = "tmp";
    EXPECT_EQ("tmp", config.getBuilderIndexRoot(true));
    EXPECT_EQ("index", config.getBuilderIndexRoot(false));
    config._fullBuilderTmpRoot = "";
    EXPECT_EQ("index", config.getBuilderIndexRoot(true));
    EXPECT_EQ("index", config.getBuilderIndexRoot(false));
}
    
TEST_F(BuildServiceConfigTest, testGetApplicationId) {
    BuildServiceConfig config;
    config.userName = "user";
    config.serviceName = "service";
    EXPECT_EQ("user_service", config.getApplicationId());
}

TEST_F(BuildServiceConfigTest, testGetCounterConfig) {
    string jsonStr = "{"
                     "\"user_name\" : \"user\","
                     "\"service_name\" : \"service\","
                     "\"index_root\" : \"index\"," 
                     "\"swift_root\" : \"swift\"," 
                     "\"hippo_root\" : \"hippo\"," 
                     "\"zookeeper_root\" : \"zk\"" 
                     "}";    
    BuildServiceConfig config;
    FromJsonString(config, jsonStr);
    EXPECT_EQ("user_service", config.getApplicationId());
    EXPECT_TRUE(config.validate());
    EXPECT_EQ(string("zookeeper"), config.counterConfig.position);
    CounterConfig &counterConfig = config.counterConfig;
    counterConfig.position = "redis";
    counterConfig.params["hostname"] = "aaa.aaa.bbb.ccc";
    counterConfig.params["port"] = "99";            
    counterConfig.params["password"] = "some-password"; 
    ASSERT_TRUE(config.validate());
    jsonStr = ToJsonString(config);
    BuildServiceConfig newConfig;
    FromJsonString(newConfig, jsonStr);
    ASSERT_TRUE(newConfig.validate());

    EXPECT_EQ(string("user"), newConfig.userName);
    EXPECT_EQ(string("service"), newConfig.serviceName);
    EXPECT_EQ(string("index"), newConfig.getIndexRoot());
    EXPECT_EQ(string("swift"), newConfig.swiftRoot);
    EXPECT_EQ(string("hippo"), newConfig.hippoRoot);
    EXPECT_EQ(string("zk"), newConfig.zkRoot);
    CounterConfig &newCounterConfig = newConfig.counterConfig;
    EXPECT_EQ(string("redis"), counterConfig.position);
    EXPECT_EQ(string("aaa.aaa.bbb.ccc"), counterConfig.params["hostname"]);
    EXPECT_EQ(string("99"), counterConfig.params["port"]);
    EXPECT_EQ(string("some-password"), counterConfig.params["password"]);
}

}
}
