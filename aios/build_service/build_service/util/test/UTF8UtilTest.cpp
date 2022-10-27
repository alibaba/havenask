#include "build_service/test/unittest.h"
#include "build_service/util/UTF8Util.h"

using namespace std;
using namespace testing;

namespace build_service {
namespace util {

class UTF8UtilTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void UTF8UtilTest::setUp() {
}

void UTF8UtilTest::tearDown() {
}

TEST_F(UTF8UtilTest, testGetNextCharUTF8) {
    string str = "a中国bcd";
    size_t start = 0, end = str.length();
    size_t len = 0;

    EXPECT_TRUE(UTF8Util::getNextCharUTF8(str.data(), start, end, len));
    EXPECT_EQ(size_t(1), len);

    start += len;
    EXPECT_TRUE(UTF8Util::getNextCharUTF8(str.data(), start, end, len));
    EXPECT_EQ(size_t(3), len);
}

}
}
