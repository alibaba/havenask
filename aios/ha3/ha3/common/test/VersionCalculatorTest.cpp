#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/common/VersionCalculator.h>

using namespace std;
using namespace testing;

BEGIN_HA3_NAMESPACE(common);

class VersionCalculatorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, VersionCalculatorTest);

void VersionCalculatorTest::setUp() {
}

void VersionCalculatorTest::tearDown() {
}

TEST_F(VersionCalculatorTest, testSimple) {
    auto v11 = VersionCalculator::calcVersion("workerConfigVersion", "", "", "");
    auto v12 = VersionCalculator::calcVersion("workerConfigVersion2", "", "", "");
    EXPECT_TRUE(v11 != v12);

    auto v21 = VersionCalculator::calcVersion("", "dataVersion", "", "");
    EXPECT_TRUE(v21 != v11);
    EXPECT_TRUE(v21 != v12);

    auto v22 = VersionCalculator::calcVersion("", "dataVersion2", "", "");
    EXPECT_TRUE(v22 != v21);
    EXPECT_TRUE(v22 != v11);
    EXPECT_TRUE(v22 != v12);

    auto v23 = VersionCalculator::calcVersion("", "dataVersion2", "", "full version");
    EXPECT_TRUE(v23 != v22);
    EXPECT_TRUE(v23 != v21);
    EXPECT_TRUE(v23 != v11);
    EXPECT_TRUE(v23 != v12);

    auto v24 = VersionCalculator::calcVersion("", "dataVersion2", "remote config path", "full version");
    EXPECT_TRUE(v24 == v23);

    auto v31 = VersionCalculator::calcVersion("", "", "remote config path", "");
    EXPECT_TRUE(v31 != v24);
    EXPECT_TRUE(v31 != v23);
    EXPECT_TRUE(v31 != v22);
    EXPECT_TRUE(v31 != v21);

    auto v32 = VersionCalculator::calcVersion("", "", "remote config path 1", "full version 1");
    EXPECT_TRUE(v32 != v31);

    auto v33 = VersionCalculator::calcVersion("", "", "remote config path 2", "full version 1");
    EXPECT_TRUE(v33 != v32);
    EXPECT_TRUE(v33 != v31);

    auto v34 = VersionCalculator::calcVersion("", "", "remote config path 2", "full version 2");
    EXPECT_TRUE(v34 != v33);
    EXPECT_TRUE(v34 != v32);
    EXPECT_TRUE(v34 != v31);
}

END_HA3_NAMESPACE();

