#include "indexlib/config/BuildConfig.h"

#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace config {

class BuildConfigTest : public TESTBASE
{
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(BuildConfigTest, TestSimpleProcess) {}

TEST_F(BuildConfigTest, TestJsonizeCheck)
{
    BuildConfig buildConfig;
    // BuildConfig
    ASSERT_THROW(FromJsonString(buildConfig, R"( {"max_doc_count":0 } )"), autil::legacy::ParameterInvalidException);
    ASSERT_THROW(FromJsonString(buildConfig, R"( {"keep_version_count":0 } )"),
                 autil::legacy::ParameterInvalidException);
}

TEST_F(BuildConfigTest, TestJsonize)
{
    BuildConfig buildConfig;
    std::string configStr = R"(
        {
            "keep_version_count":10,
            "keep_version_hour":72,
            "fence_timestamp_tolerant_deviation" : 3600000000
        }
    )";
    ASSERT_NO_THROW(FromJsonString(buildConfig, configStr));
    ASSERT_EQ(72, buildConfig.GetKeepVersionHour());
    ASSERT_EQ(10, buildConfig.GetKeepVersionCount());
    ASSERT_EQ(3600000000l, buildConfig.GetFenceTsTolerantDeviation());
}

}} // namespace indexlibv2::config
