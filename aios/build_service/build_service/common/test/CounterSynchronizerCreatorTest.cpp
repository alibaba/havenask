#include "build_service/test/unittest.h"
#include "build_service/common/CounterSynchronizerCreator.h"
#include "build_service/common/CounterRedisSynchronizer.h"
#include "build_service/util/FileUtil.h"
#include "indexlib/util/counter/state_counter.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/counter/accumulative_counter.h"
#include "build_service/util/RedisClient.h"
#include "build_service/common/CounterFileSynchronizer.h"
using namespace std;
using namespace testing;
BS_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
BS_NAMESPACE_USE(util);

namespace build_service {
namespace common {

class CounterSynchronizerCreatorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void CounterSynchronizerCreatorTest::setUp() {
}

void CounterSynchronizerCreatorTest::tearDown() {
}

TEST_F(CounterSynchronizerCreatorTest, testSimple) {
    CounterConfigPtr counterConfig(new CounterConfig());
    counterConfig->position = "redis";
    auto& kvMap = counterConfig->params;
    kvMap[COUNTER_PARAM_HOSTNAME] = "test";
    kvMap[COUNTER_PARAM_PORT] = "6379";
    kvMap[COUNTER_PARAM_PASSWORD] = "Bstest123";
    kvMap[COUNTER_PARAM_REDIS_KEY] = "some-key";
    kvMap[COUNTER_PARAM_REDIS_FIELD] = "some-field";

    bool loadFromExisted;
    CounterMapPtr counterMap = CounterSynchronizerCreator::loadCounterMap(
            counterConfig, loadFromExisted);
    ASSERT_TRUE(counterMap);
    ASSERT_FALSE(loadFromExisted);
    
    CounterSynchronizer *syner = CounterSynchronizerCreator::create(counterMap, counterConfig);
    ASSERT_TRUE(syner);

    CounterRedisSynchronizer *typeSyner = dynamic_cast<CounterRedisSynchronizer*>(syner);
    ASSERT_TRUE(typeSyner);
    ASSERT_EQ(-1, typeSyner->_ttlInSecond);

    delete syner;
}

TEST_F(CounterSynchronizerCreatorTest, testValidateCounterConfig) {
    CounterConfigPtr counterConfigNull;
    ASSERT_FALSE(CounterSynchronizerCreator::validateCounterConfig(counterConfigNull)); //counterConfig is null

    CounterConfigPtr counterConfigInvalid(new CounterConfig);
    counterConfigInvalid->position = "invalidposition";
    ASSERT_FALSE(CounterSynchronizerCreator::validateCounterConfig(counterConfigInvalid));    //counterConfigzk is invalid
    
    CounterConfigPtr counterConfigzk(new CounterConfig);
    counterConfigzk->position = "zookeeper";
    auto& kvMapzk = counterConfigzk->params;
    ASSERT_FALSE(CounterSynchronizerCreator::validateCounterConfig(counterConfigzk));    //no filepath

    kvMapzk[COUNTER_PARAM_FILEPATH] = FileUtil::joinFilePath(GET_TEST_DATA_PATH(), "counterFile"); 
    ASSERT_TRUE(CounterSynchronizerCreator::validateCounterConfig(counterConfigzk));    //counterConfigzk is OK
    
    CounterConfigPtr counterConfigRedis(new CounterConfig);
    auto& kvMapRedis=counterConfigRedis->params;
    counterConfigRedis->position = "redis";
    kvMapRedis = counterConfigRedis->params;
    kvMapRedis[COUNTER_PARAM_HOSTNAME] = "rm-tatbstesting.redis.rdstest.tbsite.net";
    kvMapRedis[COUNTER_PARAM_PORT] = "6379";
    kvMapRedis[COUNTER_PARAM_PASSWORD] = "Bstest123";
    ASSERT_FALSE(CounterSynchronizerCreator::validateCounterConfig(counterConfigRedis));//no COUNTER_PARAM_REDIS_KEY

    kvMapRedis[COUNTER_PARAM_REDIS_KEY] = "some-key";
    ASSERT_FALSE(CounterSynchronizerCreator::validateCounterConfig(counterConfigRedis));//no COUNTER_PARAM_REDIS_FIELD

    kvMapRedis[COUNTER_PARAM_REDIS_FIELD] = "some-field";
    ASSERT_TRUE(CounterSynchronizerCreator::validateCounterConfig(counterConfigRedis));//counterConfigrd is OK
}

TEST_F(CounterSynchronizerCreatorTest, testLoadCounterMap) {
    CounterConfigPtr counterConfigInvalid(new CounterConfig);
    bool loadFromExisted = false;
    counterConfigInvalid->position = "invalidposition";
    ASSERT_FALSE(CounterSynchronizerCreator::loadCounterMap(counterConfigInvalid, loadFromExisted));//counterconfig is invalid

    CounterConfigPtr counterConfigzk(new CounterConfig);
    counterConfigzk->position = "zookeeper";
    string counterFilePath = FileUtil::joinFilePath(GET_TEST_DATA_PATH(), "counterFile");
    auto& kvMapzk = counterConfigzk->params;
    kvMapzk[COUNTER_PARAM_FILEPATH] = counterFilePath;
    CounterMapPtr counterMap(new CounterMap());
    counterMap->GetAccCounter("bs.test1")->Increase(10);
    counterMap->GetAccCounter("bs.test2")->Increase(10);
    FileUtil::writeFile(counterFilePath, counterMap->ToJsonString());
    //ASSERT_EQ(counterMap, CounterSynchronizerCreator::loadCounterMap(counterConfigzk, loadFromExisted));
    
    ASSERT_EQ(int64_t(10), CounterSynchronizerCreator::loadCounterMap(counterConfigzk, loadFromExisted)->GetAccCounter("bs.test1")->Get());
    ASSERT_TRUE(loadFromExisted);//counterConfigzk is OK

    CounterConfigPtr counterConfigRedis(new CounterConfig);
    auto& kvMapRedis = counterConfigRedis->params;
    counterConfigRedis->position = "redis";
    kvMapRedis[COUNTER_PARAM_HOSTNAME] = "rm-tatbstesting.redis.rdstest.tbsite.net";
    kvMapRedis[COUNTER_PARAM_PORT] = "6379";
    kvMapRedis[COUNTER_PARAM_PASSWORD] = "Bstest123";
    kvMapRedis[COUNTER_PARAM_REDIS_KEY] = "some-key";
    kvMapRedis[COUNTER_PARAM_REDIS_FIELD] = "some-field";
    RedisClient client;
    RedisInitParam redisParam("rm-tatbstesting.redis.rdstest.tbsite.net", (uint16_t)6379, "Bstest123");
    client.connect(redisParam);
    RedisClient::ErrorCode errorCode;
    client.removeKey("some-key", errorCode);
    client.setHash("some-key", "some-field", counterMap->ToJsonString(), errorCode);
    ASSERT_TRUE(CounterSynchronizerCreator::loadCounterMap(counterConfigRedis, loadFromExisted));//counterconfigrd is OK  
    ASSERT_TRUE(loadFromExisted);
    client.removeKey("some-key", errorCode);
}
    

TEST_F(CounterSynchronizerCreatorTest, testCreate) {
    CounterConfigPtr counterConfigInvalid(new CounterConfig);
    CounterMapPtr counterMap(new CounterMap);
    counterConfigInvalid->position = "invalidposition";
    ASSERT_FALSE(CounterSynchronizerCreator::create(counterMap, counterConfigInvalid));//counterConfig is invalid

    CounterConfigPtr counterConfigzk(new CounterConfig);
    counterConfigzk->position = "zookeeper";
    ASSERT_FALSE(CounterSynchronizerCreator::create(counterMap, counterConfigzk));//counterConfigzk don't have filepath'

    auto& kvMapzk = counterConfigzk->params;
    kvMapzk[COUNTER_PARAM_FILEPATH] = FileUtil::joinFilePath(GET_TEST_DATA_PATH(), "counterFile"); 
    auto syner = CounterSynchronizerCreator::create(counterMap, counterConfigzk);
    ASSERT_TRUE(syner);
    auto counterFileSyner = dynamic_cast<CounterFileSynchronizer*>(syner);
    ASSERT_TRUE(counterFileSyner);
    delete syner;

    CounterConfigPtr counterConfigRedis(new CounterConfig);
    auto& kvMapRedis = counterConfigRedis->params;
    counterConfigRedis->position = "redis";
    kvMapRedis = counterConfigRedis->params;
    kvMapRedis[COUNTER_PARAM_HOSTNAME] = "rm-tatbstesting.redis.rdstest.tbsite.net";
    kvMapRedis[COUNTER_PARAM_PORT] = "6379";
    kvMapRedis[COUNTER_PARAM_PASSWORD] = "Bstest123";
    kvMapRedis[COUNTER_PARAM_REDIS_KEY] = "some-key";
    //kvMap[COUNTER_PARAM_REDIS_FIELD] = "some-field";
    ASSERT_FALSE(CounterSynchronizerCreator::create(counterMap, counterConfigRedis));

    kvMapRedis[COUNTER_PARAM_REDIS_FIELD] = "some-field";
    auto synerRedis = CounterSynchronizerCreator::create(counterMap, counterConfigRedis);//counterConfigrd is OK
    ASSERT_TRUE(synerRedis);
    auto counterRedisSyner = dynamic_cast<CounterRedisSynchronizer*>(synerRedis);
    ASSERT_TRUE(counterRedisSyner);
    delete counterRedisSyner;
}

}
}
