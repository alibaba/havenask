#include "build_service/util/RedisClient.h"

#include <hiredis/hiredis.h>

#include "build_service/test/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace util {

class RedisClientTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void RedisClientTest::setUp() {}

void RedisClientTest::tearDown() {}

// DISABLE case for visit url fail
TEST_F(RedisClientTest, DISABLED_testHiRedis)
{
    const char* hostname = "rm-tatbstesting.redis.rdstest.tbsite.net";
    const int port = 6379;
    const char* passwd = "Bstest123";
    struct timeval timeout = {1, 500000};
    redisContext* c = redisConnectWithTimeout(hostname, port, timeout);
    if (c == NULL || c->err) {
        if (c) {
            printf("Connection error: %s\n", c->errstr);
            redisFree(c);

        } else {
            printf("Connection error: can't allocate redis context\n");
        }
        ASSERT_TRUE(false);
    }
    redisReply* reply = static_cast<redisReply*>(redisCommand(c, "AUTH %s", passwd));
    if (reply->type == REDIS_REPLY_ERROR) {
        printf("Auth failed: %s\n", reply->str);
        freeReplyObject(reply);
        ASSERT_TRUE(false);
    }

    freeReplyObject(reply);
    reply = static_cast<redisReply*>(redisCommand(c, "PING"));
    ASSERT_EQ(REDIS_REPLY_STATUS, reply->type);
    ASSERT_EQ(string("PONG"), string(reply->str));
    freeReplyObject(reply);

    reply = static_cast<redisReply*>(redisCommand(c, "hset mykey f1 v1"));
    ASSERT_EQ(REDIS_REPLY_INTEGER, reply->type);
    freeReplyObject(reply);

    reply = static_cast<redisReply*>(redisCommand(c, "hset mykey field2 value2"));
    ASSERT_EQ(REDIS_REPLY_INTEGER, reply->type);
    freeReplyObject(reply);

    reply = static_cast<redisReply*>(redisCommand(c, "hset mykey justKey"));
    ASSERT_EQ(REDIS_REPLY_ERROR, reply->type);
    freeReplyObject(reply);

    reply = static_cast<redisReply*>(redisCommand(c, "hgetall mykey"));
    ASSERT_EQ(REDIS_REPLY_ARRAY, reply->type);

    vector<string> expectValues = {"f1", "v1", "field2", "value2"};
    ASSERT_EQ(expectValues.size(), reply->elements);

    for (size_t i = 0; i < reply->elements; ++i) {
        redisReply* subReply = reply->element[i];
        EXPECT_EQ(REDIS_REPLY_STRING, subReply->type);
        EXPECT_EQ(expectValues[i], string(subReply->str, subReply->len));
    }

    freeReplyObject(reply);
    redisFree(c);
}

TEST_F(RedisClientTest, testSimple)
{
    string hostname = "test";
    const int port = 6379;
    string passwd = "Bstest123";
    int timeout = 3; // set 3 seconds timeout
    RedisClient client;
    if (!client.connect(RedisInitParam(hostname, port, passwd), timeout)) {
        printf("Connection error, %s", client.getLastError().c_str());
        ASSERT_TRUE(false);
    }

    RedisClient::ErrorCode errorCode;
    client.setHash("myhash2", "myfield1", "value1", errorCode);
    ASSERT_EQ(RedisClient::RC_OK, errorCode);

    client.setHash("myhash2", "myfield2", "value2", errorCode);
    ASSERT_EQ(RedisClient::RC_OK, errorCode);

    string value = client.getHash("myhash2", "myfield1", errorCode);
    ASSERT_EQ(RedisClient::RC_OK, errorCode);
    EXPECT_EQ(string("value1"), value);

    value = client.getHash("myhash2", "myfield2", errorCode);
    ASSERT_EQ(RedisClient::RC_OK, errorCode);
    EXPECT_EQ(string("value2"), value);

    value = client.getHash("myhash2", "non-exist-key", errorCode);
    ASSERT_EQ(RedisClient::RC_HASH_FIELD_NONEXIST, errorCode);
    printf("error: %s\n", client.getLastError().c_str());

    auto roleCounterMap = client.getHash("myhash2", errorCode);
    ASSERT_EQ(RedisClient::RC_OK, errorCode);

    map<string, string> expectedMap = {{"myfield1", "value1"}, {"myfield2", "value2"}};
    ASSERT_EQ(expectedMap, roleCounterMap);

    auto nonExistMap = client.getHash("non-exist-hash", errorCode);
    ASSERT_EQ(RedisClient::RC_HASH_NONEXIST, errorCode);
    printf("error: %s\n", client.getLastError().c_str());
}

TEST_F(RedisClientTest, testExpire)
{
    string hostname = "test";
    const int port = 6379;
    string passwd = "Bstest123";
    int timeout = 3; // set 3 seconds timeout
    RedisClient client;
    if (!client.connect(RedisInitParam(hostname, port, passwd), timeout)) {
        printf("Connection error, %s", client.getLastError().c_str());
        ASSERT_TRUE(false);
    }
    RedisClient::ErrorCode errorCode;
    client.removeKey("tempKey", errorCode);
    string fieldValue;
    { // test key expires
        client.setHash("tempKey", "tempField", "tempValue", errorCode);
        ASSERT_EQ(RedisClient::RC_OK, errorCode);
        ASSERT_EQ(1, client.expireKey("tempKey", 5, errorCode));
        fieldValue = client.getHash("tempKey", "tempField", errorCode);
        ASSERT_EQ(RedisClient::RC_OK, errorCode);
        ASSERT_EQ(string("tempValue"), fieldValue);
        sleep(8);
        client.getHash("tempKey", "tempField", errorCode);
        ASSERT_EQ(RedisClient::RC_HASH_FIELD_NONEXIST, errorCode);
    }
    { // test prolong expire time
        client.setHash("tempKey", "tempField", "tempValue", errorCode);
        ASSERT_EQ(RedisClient::RC_OK, errorCode);
        ASSERT_EQ(1, client.expireKey("tempKey", 5, errorCode));
        fieldValue = client.getHash("tempKey", "tempField", errorCode);
        ASSERT_EQ(RedisClient::RC_OK, errorCode);
        ASSERT_EQ(string("tempValue"), fieldValue);
        fieldValue = client.setHash("tempKey", "tempField", "tempValue1", errorCode);
        ASSERT_EQ(1, client.expireKey("tempKey", 10, errorCode));
        sleep(5);
        fieldValue = client.getHash("tempKey", "tempField", errorCode);
        ASSERT_EQ(RedisClient::RC_OK, errorCode);
        ASSERT_EQ(string("tempValue1"), fieldValue);
    }
    client.removeKey("tempKey", errorCode);
}

}} // namespace build_service::util
