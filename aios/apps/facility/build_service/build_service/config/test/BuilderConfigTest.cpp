#include "build_service/config/BuilderConfig.h"

#include "build_service/test/unittest.h"

using namespace std;
using namespace autil::legacy;

namespace build_service { namespace config {

class BuilderConfigTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void BuilderConfigTest::setUp() {}

void BuilderConfigTest::tearDown() {}

TEST_F(BuilderConfigTest, testSimple)
{
    BuilderConfig config;
    ASSERT_FALSE(config.sortBuild);
    ASSERT_EQ(BuilderConfig::DEFAULT_SORT_QUEUE_SIZE, config.sortQueueSize);
    ASSERT_EQ(BuilderConfig::INVALID_SORT_QUEUE_MEM, config.sortQueueMem);
}

TEST_F(BuilderConfigTest, testDefault)
{
    string jsonStr = "{}";
    BuilderConfig config;
    FromJsonString(config, jsonStr);
    ASSERT_FALSE(config.sortBuild);
    ASSERT_EQ(BuilderConfig::DEFAULT_SORT_QUEUE_SIZE, config.sortQueueSize);
    ASSERT_EQ(BuilderConfig::INVALID_SORT_QUEUE_MEM, config.sortQueueMem);
    ASSERT_EQ(0, config.sortDescriptions.size());
    ASSERT_EQ(0, config.buildQpsLimit);

    string jsonStr2 = ToJsonString(config);
    BuilderConfig config2;
    FromJsonString(config2, jsonStr2);
    ASSERT_EQ(config, config2);
}

TEST_F(BuilderConfigTest, testJsonize)
{
    string jsonStr = "{"
                     "\"sort_build\" : true,"
                     "\"build_qps_limit\" : 100,"
                     "\"sort_queue_size\" : 2000000,"
                     "\"sort_queue_mem\" : 100,"
                     "\"sort_descriptions\" : [{"
                     "\"sort_field\" : \"category\","
                     "\"sort_pattern\" : \"asc\""
                     "}]"
                     "}";

    BuilderConfig config;
    FromJsonString(config, jsonStr);
    ASSERT_TRUE(config.sortBuild);
    ASSERT_EQ(2000000u, config.sortQueueSize);
    ASSERT_EQ(int64_t(100), config.sortQueueMem);
    ASSERT_EQ(size_t(1), config.sortDescriptions.size());
    ASSERT_EQ("category", config.sortDescriptions[0].GetSortFieldName());
    ASSERT_EQ(indexlibv2::config::sp_asc, config.sortDescriptions[0].GetSortPattern());
    ASSERT_EQ(100, config.buildQpsLimit);

    string jsonStr2 = ToJsonString(config);
    BuilderConfig config2;
    FromJsonString(config2, jsonStr2);
    ASSERT_EQ(config, config2);
}

}} // namespace build_service::config
