#include "iquan/config/ClientConfig.h"

#include <map>
#include <string>
#include <utility>

#include "autil/Log.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/test/JsonTestUtil.h"
#include "iquan/common/Common.h"
#include "iquan/common/IquanException.h"
#include "iquan/common/Status.h"
#include "iquan/common/Utils.h"
#include "unittest/unittest.h"

using namespace testing;

namespace iquan {

class ClientConfigTest : public TESTBASE {
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(iquan, ClientConfigTest);

TEST_F(ClientConfigTest, testDefault) {
    {
        ClientConfig clientConfig;
        ASSERT_EQ(4, clientConfig.cacheConfigs.size());

        ASSERT_TRUE(clientConfig.cacheConfigs.find(IQUAN_SQL_PARSE)
                    != clientConfig.cacheConfigs.end());
        ASSERT_TRUE(clientConfig.cacheConfigs.find(IQUAN_REL_TRANSFORM)
                    != clientConfig.cacheConfigs.end());
        ASSERT_TRUE(clientConfig.cacheConfigs.find(IQUAN_REL_POST_OPTIMIZE)
                    != clientConfig.cacheConfigs.end());
        ASSERT_TRUE(clientConfig.cacheConfigs.find(IQUAN_JNI_POST_OPTIMIZE)
                    != clientConfig.cacheConfigs.end());

        auto iter = clientConfig.cacheConfigs.find(IQUAN_JNI_POST_OPTIMIZE);
        CacheConfig cacheConfig = iter->second;
        ASSERT_EQ(100 * 1024 * 1024, cacheConfig.maxSize);
        ASSERT_TRUE(clientConfig.isValid());
    }

    {
        std::string clientStr = R"json(
{
    "debug_flag" : true,
    "cache_config" : {
        "rel.post.optimize" : {
            "initial_capacity" : 123,
            "concurrency_level" : 456,
            "maximum_size" : 789
        }
    }
})json";

        ClientConfig clientConfig;
        Status status = Utils::fromJson(clientConfig, clientStr);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_EQ(1, clientConfig.cacheConfigs.size());
        ASSERT_TRUE(clientConfig.isValid());
    }
}

TEST_F(ClientConfigTest, testCacheConfig) {
    CacheConfig config;
    ASSERT_TRUE(config.isValid());
    ASSERT_EQ(1024, config.initialCapcity);
    ASSERT_EQ(8, config.concurrencyLevel);
    ASSERT_EQ(4096, config.maxSize);

    config.initialCapcity = 1;
    ASSERT_TRUE(config.isValid());
    config.concurrencyLevel = 1;
    ASSERT_TRUE(config.isValid());
    config.maxSize = 1;
    ASSERT_TRUE(config.isValid());
    ASSERT_TRUE(config.isValid());

    CacheConfig config2 = config;
    ASSERT_TRUE(config2.isValid());
    ASSERT_EQ(1, config2.initialCapcity);
    ASSERT_EQ(1, config2.concurrencyLevel);
    ASSERT_EQ(1, config2.maxSize);

    std::string actualStr;
    std::string expectedStr = R"json({
"concurrency_level":
  1,
"initial_capacity":
  1,
"maximum_size":
  1
})json";
    Status status = Utils::toJson(config2, actualStr, false);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    ASSERT_NO_FATAL_FAILURE(
        autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedStr, actualStr));

    CacheConfig config3;
    status = Utils::fromJson(config3, expectedStr);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    ASSERT_TRUE(config3.isValid());
    ASSERT_EQ(1, config3.initialCapcity);
    ASSERT_EQ(1, config3.concurrencyLevel);
    ASSERT_EQ(1, config3.maxSize);
}

TEST_F(ClientConfigTest, testClientConfig) {
    ClientConfig clientConfig;
    ASSERT_TRUE(clientConfig.isValid());

    CacheConfig cacheConfig;
    cacheConfig.initialCapcity = 100;
    cacheConfig.concurrencyLevel = 200;
    cacheConfig.maxSize = 300;

    clientConfig.debugFlag = true;
    clientConfig.cacheConfigs.clear();
    clientConfig.cacheConfigs["key"] = cacheConfig;
    ASSERT_TRUE(clientConfig.isValid());

    std::string expectedStr = R"json({
"cache_config":
  {
  "key":
    {
    "concurrency_level":
      200,
    "initial_capacity":
      100,
    "maximum_size":
      300
    }
  },
"debug_flag":
  true
})json";
    std::string actualStr;
    Status status = Utils::toJson(clientConfig, actualStr, false);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    ASSERT_NO_FATAL_FAILURE(
        autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedStr, actualStr));

    ClientConfig clientConfig2;
    status = Utils::fromJson(clientConfig2, actualStr);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
}

} // namespace iquan
