#include "build_service/test/unittest.h"
#include "build_service/config/BuildRuleConfig.h"

using namespace std;
using namespace testing;

namespace build_service {
namespace config {

class BuildRuleConfigTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void BuildRuleConfigTest::setUp() {
}

void BuildRuleConfigTest::tearDown() {
}

TEST_F(BuildRuleConfigTest, testJsonize) {
    BuildRuleConfig config;
    config.partitionCount = 10;
    config.buildParallelNum = 12;
    config.mergeParallelNum = 20;
    config.mapReduceRatio = 11;
    config.needPartition = false;

    string str = ToJsonString(config);
    BuildRuleConfig other;
    ASSERT_NO_THROW(FromJsonString(other, str));

    EXPECT_EQ(config.partitionCount, other.partitionCount);
    EXPECT_EQ(config.buildParallelNum, other.buildParallelNum);
    EXPECT_EQ(config.mergeParallelNum , other.mergeParallelNum);
    EXPECT_EQ(config.mapReduceRatio, other.mapReduceRatio);
    EXPECT_EQ(config.needPartition, other.needPartition);
}

#define testValidate(param)                     \
    {                                           \
        BuildRuleConfig config;                 \
        param = 0;                              \
        EXPECT_FALSE(config.validate());        \
    }

TEST_F(BuildRuleConfigTest, testValidate) {
    testValidate(config.partitionCount);
    testValidate(config.buildParallelNum);
    testValidate(config.mergeParallelNum);
    testValidate(config.mapReduceRatio);
    testValidate(config.partitionSplitNum);
}

}
}
