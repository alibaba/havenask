#include "indexlib/config/BuildOptionConfig.h"

#include "indexlib/config/SortDescription.h"
#include "indexlib/config/TabletSchema.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace config {

class BuildOptionConfigTest : public TESTBASE
{
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(BuildOptionConfigTest, TestUsage)
{
    {
        BuildOptionConfig buildOptionConfig;
        string jsonStr = R"(
        {
            "sort_build" : false
        })";
        FromJsonString(buildOptionConfig, jsonStr);
        ASSERT_FALSE(buildOptionConfig.IsSortBuild());
    }
    {
        BuildOptionConfig buildOptionConfig;
        string jsonStr = R"(
        {
            "sort_build" : true,
            "sort_descriptions": 
            [
                {
                    "sort_field" : "category",
                    "sort_pattern" : "desc"
                },
                {
                    "sort_field" : "ends",
                    "sort_pattern" : "asc"
                }
            ]
        })";
        FromJsonString(buildOptionConfig, jsonStr);
        ASSERT_TRUE(buildOptionConfig.IsSortBuild());
        config::SortDescriptions sortDescs = {
            config::SortDescription("category", config::sp_desc),
            config::SortDescription("ends", config::sp_asc),
        };
        ASSERT_EQ(2, buildOptionConfig.GetSortDescriptions().size());
        ASSERT_EQ(sortDescs, buildOptionConfig.GetSortDescriptions());
    }
}

}} // namespace indexlibv2::config
