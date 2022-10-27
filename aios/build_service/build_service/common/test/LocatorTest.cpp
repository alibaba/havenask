#include "build_service/test/unittest.h"
#include "build_service/common/Locator.h"

using namespace std;
using namespace testing;

namespace build_service {
namespace common {

class LocatorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void LocatorTest::setUp() {
}

void LocatorTest::tearDown() {
}

TEST_F(LocatorTest, testOperator) {
    Locator locator;
    EXPECT_EQ(int64_t(-1), Locator().getOffset());
    EXPECT_EQ(uint64_t(0), Locator().getSrc());

    Locator lo1;
    lo1.setOffset(2);
    lo1.setSrc(1);
    EXPECT_EQ(uint64_t(1), lo1.getSrc());
    EXPECT_EQ(int64_t(2), lo1.getOffset());

    EXPECT_TRUE(Locator(1) != Locator(2));
    EXPECT_TRUE(Locator(3) ==  Locator(3));
}

TEST_F(LocatorTest, testToStringAndFromString) {
    Locator lo1(1, 100);
    string lo1Str = lo1.toString();
    ASSERT_TRUE(lo1.fromString(lo1Str));
    EXPECT_EQ(uint64_t(1), lo1.getSrc());
    EXPECT_EQ(int64_t(100), lo1.getOffset());

    // test from other locator
    Locator lo2(1000, 2000);
    string lo2Str = lo2.toString();
    EXPECT_TRUE(lo1.fromString(lo2Str));
    EXPECT_FALSE(lo1.fromString(string("777aa")));

}

}
}
