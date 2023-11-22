#include "build_service/config/ControlConfig.h"

#include <iosfwd>
#include <string>

#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace config {

class ControlConfigTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void ControlConfigTest::setUp() {}

void ControlConfigTest::tearDown() {}

TEST_F(ControlConfigTest, testSimple)
{
    string jsonStr = R"( {
        "is_inc_processor_exist" : false,
        "clusters" : [
            { "cluster_name" : "summary", "is_inc_processor_exist" : false },
            { "cluster_name" : "buyer", "is_inc_processor_exist" : true }
        ]
    } )";

    ControlConfig config;
    FromJsonString(config, jsonStr);

    ASSERT_FALSE(config.isIncProcessorExist("summary"));
    ASSERT_TRUE(config.isIncProcessorExist("buyer"));
    ASSERT_FALSE(config.isIncProcessorExist("other"));

    jsonStr = R"( {
        "is_inc_processor_exist" : true,
        "clusters" : [
            { "cluster_name" : "summary", "is_inc_processor_exist" : false },
            { "cluster_name" : "buyer", "is_inc_processor_exist" : false }
        ]
    } )";

    ControlConfig config1;
    FromJsonString(config1, jsonStr);
    ASSERT_FALSE(config1.isIncProcessorExist("summary"));
    ASSERT_FALSE(config1.isIncProcessorExist("buyer"));
    ASSERT_TRUE(config1.isIncProcessorExist("other"));

    jsonStr = R"( {
        "is_inc_processor_exist" : true,
        "clusters" : [
            { "cluster_name" : "summary", "is_inc_processor_exist" : false },
            { "cluster_name" : "summary", "is_inc_processor_exist" : true }
        ]
    } )";
    ControlConfig config2;
    ASSERT_ANY_THROW(FromJsonString(config2, jsonStr));
}
TEST_F(ControlConfigTest, testDataLinkMode)
{
    {
        // is_inc_processor_exist = false can be derived as FP_INP_MODE
        string jsonStr = R"( {
        "is_inc_processor_exist" : false,
        "clusters" : [
            { "cluster_name" : "summary", "is_inc_processor_exist" : false },
            { "cluster_name" : "buyer", "is_inc_processor_exist" : true }
          ]
        } )";

        ControlConfig config;
        FromJsonString(config, jsonStr);
        ASSERT_EQ(ControlConfig::DataLinkMode::FP_INP_MODE, config.getDataLinkMode());
        ASSERT_TRUE(config.isIncProcessorExist("buyer"));
        ASSERT_FALSE(config.isIncProcessorExist("summary"));
    }
    {
        // FP_INP_MODE will rewrite is_inc_processor_exist to false
        string jsonStr = R"( {
        "data_link_mode" : "FP_INP",
        "is_inc_processor_exist" : true,
        "clusters" : [
          ]
        } )";

        ControlConfig config;
        FromJsonString(config, jsonStr);
        ASSERT_EQ(ControlConfig::DataLinkMode::FP_INP_MODE, config.getDataLinkMode());
        ASSERT_FALSE(config.isIncProcessorExist("buyer"));
        ASSERT_FALSE(config.isIncProcessorExist("summary"));
    }
    {
        // default normal mode
        string jsonStr = R"( {
        "clusters" : []
        } )";

        ControlConfig config;
        FromJsonString(config, jsonStr);
        ASSERT_EQ(ControlConfig::DataLinkMode::NORMAL_MODE, config.getDataLinkMode());
        ASSERT_TRUE(config.isIncProcessorExist("buyer"));
        ASSERT_TRUE(config.isIncProcessorExist("summary"));
    }
}
}} // namespace build_service::config
