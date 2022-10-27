#include "build_service/test/unittest.h"
#include "build_service/config/CounterConfig.h"

using namespace std;
using namespace testing;
using namespace autil::legacy;

namespace build_service {
namespace config {

class CounterConfigTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void CounterConfigTest::setUp() {
}

void CounterConfigTest::tearDown() {
}

TEST_F(CounterConfigTest, testJsonize) {
    {
        CounterConfig counterConfig;
        ASSERT_NO_THROW(FromJsonString(counterConfig, "{}"));
        EXPECT_EQ(string("zookeeper"), counterConfig.position);
    }
    {
        CounterConfig counterConfig;
        string jsonStr = ToJsonString(counterConfig);
        printf("counter json str: %s", jsonStr.c_str());

        CounterConfig newCounterConfig;
        ASSERT_NO_THROW(FromJsonString(newCounterConfig, jsonStr));
        EXPECT_EQ(string("zookeeper"), newCounterConfig.position);
    }
    {
        CounterConfig counterConfig;
        counterConfig.position = "redis";
        ASSERT_FALSE(counterConfig.validate());        
        counterConfig.params["hostname"] = "aaa.aaa.bbb.ccc";
        counterConfig.params["port"] = "non-integer";
        counterConfig.params["password"] = "some-password"; 
        ASSERT_FALSE(counterConfig.validate());
        counterConfig.params["port"] = "99";
        counterConfig.params["key_ttl"] = "3600"; 
        ASSERT_TRUE(counterConfig.validate());
        string jsonStr = ToJsonString(counterConfig);
        printf("counter config str: %s", jsonStr.c_str());
        CounterConfig newCounterConfig;
        FromJsonString(newCounterConfig, jsonStr);
        ASSERT_EQ(newCounterConfig, counterConfig);
    }
}

}
}
