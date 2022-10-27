#include "build_service/test/unittest.h"
#include "build_service/common/CounterRedisSynchronizer.h"
#include "build_service/util/RedisClient.h"
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/util/counter/accumulative_counter.h>

using namespace std;
using namespace testing;
BS_NAMESPACE_USE(util);
IE_NAMESPACE_USE(util);

namespace build_service {
namespace common {

class CounterRedisSynchronizerTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    string _host = "test";
    uint16_t _port =  6379;
    string _pass = "Bstest123";
    RedisClientPtr _redisClient;
};

void CounterRedisSynchronizerTest::setUp() {
    RedisInitParam param(_host, _port, _pass);
    _redisClient.reset(new RedisClient());
    ASSERT_TRUE(_redisClient->connect(param));
    
    CounterMapPtr counterMap(new CounterMap());
    counterMap->GetAccCounter("bs.testcounter1")->Increase(10);
    counterMap->GetAccCounter("bs.testcounter2")->Increase(20);
    RedisClient::ErrorCode errorCode;
    _redisClient->setHash("testKey", "testHash", counterMap->ToJsonString(), errorCode);
    ASSERT_EQ(RedisClient::RC_OK, errorCode);
}

void CounterRedisSynchronizerTest::tearDown() {
    if (_redisClient) {
        RedisClient::ErrorCode errorCode;
        _redisClient->removeKey("testKey", errorCode);
    }
}

TEST_F(CounterRedisSynchronizerTest, testSimple) {
    RedisInitParam invalidParam;
    bool valueExist;
    CounterMapPtr counterMap = CounterRedisSynchronizer::loadCounterMap(
            invalidParam, "testKey", "testHash", valueExist);
    ASSERT_FALSE(counterMap);

    RedisInitParam validParam(_host, _port, _pass);
    counterMap = CounterRedisSynchronizer::loadCounterMap(
            validParam, "test-non-exist-key", "testHash", valueExist);
    ASSERT_TRUE(counterMap);
    ASSERT_FALSE(valueExist);

    counterMap = CounterRedisSynchronizer::loadCounterMap(
            validParam, "testKey", "testHash", valueExist);
    ASSERT_TRUE(counterMap);
    ASSERT_TRUE(valueExist);

    auto test1 = counterMap->GetAccCounter("bs.testcounter1");
    auto test2 = counterMap->GetAccCounter("bs.testcounter2");
    EXPECT_EQ(10, test1->Get()); 
    EXPECT_EQ(20, test2->Get());

    CounterRedisSynchronizer syner;
    ASSERT_TRUE(syner.init(counterMap, validParam, "testKey", "testHash"));
    EXPECT_EQ(string("testKey"), syner._key);
    EXPECT_EQ(string("testHash"), syner._field);

    // test sync
    counterMap = syner.getCounterMap();
    ASSERT_TRUE(counterMap);
    // testcounter1 set to 11
    // testcounter2 set to 21
    counterMap->GetAccCounter("bs.testcounter1")->Increase(1);
    counterMap->GetAccCounter("bs.testcounter2")->Increase(1);

    ASSERT_TRUE(syner.sync());
    {
        CounterMapPtr newCounterMap(new CounterMap());
        RedisClient::ErrorCode errorCode;
        newCounterMap->FromJsonString(_redisClient->getHash("testKey", "testHash", errorCode));
        EXPECT_EQ(11, newCounterMap->GetAccCounter("bs.testcounter1")->Get());
        EXPECT_EQ(21, newCounterMap->GetAccCounter("bs.testcounter2")->Get()); 
    }

    // test begin sync
    // testcounter1 set to 12
    // testcounter2 set to 22    
    counterMap->GetAccCounter("bs.testcounter1")->Increase(1);
    counterMap->GetAccCounter("bs.testcounter2")->Increase(1);

    ASSERT_TRUE(syner.beginSync(2));
    sleep(2);
    {
        CounterMapPtr newCounterMap(new CounterMap());
        RedisClient::ErrorCode errorCode;
        newCounterMap->FromJsonString(_redisClient->getHash("testKey", "testHash", errorCode));
        EXPECT_EQ(12, newCounterMap->GetAccCounter("bs.testcounter1")->Get());
        EXPECT_EQ(22, newCounterMap->GetAccCounter("bs.testcounter2")->Get()); 
    }
}

TEST_F(CounterRedisSynchronizerTest, testExpireTime) {
    RedisInitParam validParam(_host, _port, _pass);
    RedisClient redisClient;
    ASSERT_TRUE(redisClient.connect(validParam));
    RedisClient::ErrorCode ec;
    string testKey = "testKey";
    string testField = "testField";
    redisClient.removeKey(testKey, ec);

    CounterMapPtr counterMap(new CounterMap());
    counterMap->GetAccCounter("bs.f1")->Increase(1); 
    counterMap->GetAccCounter("bs.f1")->Increase(1);

    int ttlInSecond = 3;
    CounterRedisSynchronizer syner;
    ASSERT_TRUE(syner.init(counterMap, validParam, testKey, testField, ttlInSecond));
    EXPECT_TRUE(syner.sync());
    sleep(2);
    EXPECT_TRUE(syner.sync());
    sleep(2);
    EXPECT_TRUE(syner.sync());
    sleep(2);
    EXPECT_TRUE(syner.sync());
    redisClient.getHash(testKey, ec);
    EXPECT_EQ(RedisClient::RC_OK, ec);
    sleep(4);
    redisClient.getHash(testKey, ec);
    EXPECT_EQ(RedisClient::RC_HASH_NONEXIST, ec);
}

}
}
