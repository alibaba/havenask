#include "indexlib/config/MergeConfig.h"

#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace config {

class MergeConfigTest : public TESTBASE
{
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(MergeConfigTest, TestSimpleProcess) {}

TEST_F(MergeConfigTest, TestJsonizeCheck)
{
    MergeConfig mergeConfig;
    ASSERT_THROW(FromJsonString(mergeConfig, R"( {"max_merge_memory_use":0 } )"),
                 autil::legacy::ParameterInvalidException);
    ASSERT_THROW(FromJsonString(mergeConfig, R"( {"merge_thread_count":0 } )"),
                 autil::legacy::ParameterInvalidException);
    ASSERT_THROW(FromJsonString(mergeConfig, R"( {"merge_thread_count":21 } )"),
                 autil::legacy::ParameterInvalidException);
}

}} // namespace indexlibv2::config
