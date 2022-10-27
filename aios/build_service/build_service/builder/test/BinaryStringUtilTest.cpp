#include "build_service/test/unittest.h"
#include "build_service/builder/BinaryStringUtil.h"

using namespace std;
using namespace autil;
using namespace testing;

namespace build_service {
namespace builder {

class BinaryStringUtilTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    template <typename T>
    void compareBinaryString(T a, T b) {
        string result1;
        BinaryStringUtil::toString<T>(a, result1);
        string result2;
        BinaryStringUtil::toString<T>(b, result2);
        ASSERT_TRUE(result1 < result2);

        string result3;
        BinaryStringUtil::toInvertString<T>(a, result3);
        string result4;
        BinaryStringUtil::toInvertString<T>(b, result4);
        ASSERT_TRUE(result3 > result4);
    }

    template <typename T>
    void compareBinaryStringFail(T a, T b) {
        string result1;
        BinaryStringUtil::toString<T>(a, result1);
        string result2;
        BinaryStringUtil::toString<T>(b, result2);
        ASSERT_TRUE(!(result1 < result2));

        string result3;
        BinaryStringUtil::toInvertString<T>(a, result3);
        string result4;
        BinaryStringUtil::toInvertString<T>(b, result4);
        ASSERT_TRUE(!(result3 > result4));
    }
};

void BinaryStringUtilTest::setUp() {
}

void BinaryStringUtilTest::tearDown() {
}

TEST_F(BinaryStringUtilTest, testSimple) {
    compareBinaryStringFail<uint32_t>(0, 0);
    compareBinaryString<uint32_t>(0, 1);
    compareBinaryStringFail<uint32_t>(1, 0);

    compareBinaryStringFail<uint32_t>(1000, 1000);
    compareBinaryString<uint32_t>(1000, 1001);
    compareBinaryStringFail<uint32_t>(1001, 1000);

    compareBinaryString<uint32_t>(1001, 10000001);
    compareBinaryString<uint32_t>(10000000, 10000001);
    compareBinaryString<uint32_t>(0x7FFFFFFF, 0x80000000);
}

TEST_F(BinaryStringUtilTest, testInteger) {
    //negative number
    compareBinaryStringFail<int32_t>(-1000, -1000);
    compareBinaryString<int32_t>(-1001, -1000);
    compareBinaryString<int32_t>(-1000000001, -1001);
    compareBinaryString<int32_t>(-1000000001, -1000000000);

    //0
    compareBinaryString<int32_t>(-1, 0);
}

TEST_F(BinaryStringUtilTest, testDouble) {
    //positive number
    compareBinaryStringFail<double>(1.1, 1.1);
    compareBinaryString<double>(1.1, 1.2);
    compareBinaryStringFail<double>(1.2, 1.1);

    //negative number
    compareBinaryStringFail<double>(-0.1515, -0.1515);
    compareBinaryString<double>(-0.1515, 0.1515);
    compareBinaryString<double>(-0.1516, 0.1515);
    compareBinaryString<double>(-0.5151515152, -0.5151515151);
    compareBinaryStringFail<double>(-0.5151515151, -0.5151515152);

    //0
    compareBinaryString<double>(0.0, 0.1);
    compareBinaryString<double>(-0.0, 0.0);
    compareBinaryString<double>(-0.1, -0.0);
    compareBinaryString<double>(0.5151515151, 100000.5151515152);
    compareBinaryString<double>(-100000.5151515152, -0.5151515151);
}

TEST_F(BinaryStringUtilTest, testMultiTypes) {
    compareBinaryString<float>(-0.1516, 0.1515);
    compareBinaryString<float>(-0.1516, -0.1515);
    compareBinaryString<float>(0.1515, 0.1516);
}

}
}
