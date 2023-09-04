#include "indexlib/base/BinaryStringUtil.h"

#include "unittest/unittest.h"

namespace indexlibv2::base {

class BinaryStringUtilTest : public TESTBASE
{
public:
    BinaryStringUtilTest() = default;
    ~BinaryStringUtilTest() = default;

public:
    void setUp() override;
    void tearDown() override;

protected:
    template <typename T>
    void compareBinaryString(T a, T b)
    {
        std::string result1 = BinaryStringUtil::toString<T>(a);
        std::string result2 = BinaryStringUtil::toString<T>(b);
        ASSERT_TRUE(result1 < result2);

        std::string result3 = BinaryStringUtil::toInvertString<T>(a);
        std::string result4 = BinaryStringUtil::toInvertString<T>(b);
        ASSERT_TRUE(result3 > result4);
    }

    template <typename T>
    void compareBinaryStringFail(T a, T b)
    {
        std::string result1 = BinaryStringUtil::toString<T>(a);
        std::string result2 = BinaryStringUtil::toString<T>(b);
        ASSERT_FALSE(result1 < result2);

        std::string result3 = BinaryStringUtil::toInvertString<T>(a);
        std::string result4 = BinaryStringUtil::toInvertString<T>(b);
        ASSERT_FALSE(result3 > result4);
    }
};

void BinaryStringUtilTest::setUp() {}

void BinaryStringUtilTest::tearDown() {}

TEST_F(BinaryStringUtilTest, testSimple)
{
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

TEST_F(BinaryStringUtilTest, testInteger)
{
    // negative number
    compareBinaryStringFail<int32_t>(-1000, -1000);
    compareBinaryString<int32_t>(-1001, -1000);
    compareBinaryString<int32_t>(-1000000001, -1001);
    compareBinaryString<int32_t>(-1000000001, -1000000000);

    // 0
    compareBinaryString<int32_t>(-1, 0);
}

TEST_F(BinaryStringUtilTest, testDouble)
{
    // positive number
    compareBinaryStringFail<double>(1.1, 1.1);
    compareBinaryString<double>(1.1, 1.2);
    compareBinaryStringFail<double>(1.2, 1.1);

    // negative number
    compareBinaryStringFail<double>(-0.1515, -0.1515);
    compareBinaryString<double>(-0.1515, 0.1515);
    compareBinaryString<double>(-0.1516, 0.1515);
    compareBinaryString<double>(-0.5151515152, -0.5151515151);
    compareBinaryStringFail<double>(-0.5151515151, -0.5151515152);

    // 0
    compareBinaryString<double>(0.0, 0.1);
    compareBinaryString<double>(-0.0, 0.0);
    compareBinaryString<double>(-0.1, -0.0);
    compareBinaryString<double>(0.5151515151, 100000.5151515152);
    compareBinaryString<double>(-100000.5151515152, -0.5151515151);
}

TEST_F(BinaryStringUtilTest, testMultiTypes)
{
    compareBinaryString<float>(-0.1516, 0.1515);
    compareBinaryString<float>(-0.1516, -0.1515);
    compareBinaryString<float>(0.1515, 0.1516);
}

TEST_F(BinaryStringUtilTest, testNullField)
{
    // negative number
    {
        std::string result1 = BinaryStringUtil::toString<int32_t>(-102, false);
        std::string result2 = BinaryStringUtil::toString<int32_t>(-100, false);
        ASSERT_TRUE(result1 < result2);

        result2 = BinaryStringUtil::toString<int32_t>(-100, true);
        ASSERT_TRUE(result1 > result2);
        result1 = BinaryStringUtil::toString<int32_t>(-102, true);
        ASSERT_TRUE(result1 < result2);
        ASSERT_EQ(5, result1.size());
        ASSERT_EQ(5, result2.size());
    }
    // positive
    {
        std::string result1 = BinaryStringUtil::toString<int32_t>(-10, false);
        std::string result2 = BinaryStringUtil::toString<int32_t>(10, false);
        ASSERT_TRUE(result1 < result2);
        result2 = BinaryStringUtil::toString<int32_t>(10, true);
        ASSERT_TRUE(result1 > result2);
        ASSERT_EQ(5, result1.size());
        ASSERT_EQ(5, result2.size());
    }
    // double
    {
        std::string result1 = BinaryStringUtil::toString<double>(-10.1, false);
        std::string result2 = BinaryStringUtil::toString<double>(10.1, false);
        ASSERT_TRUE(result1 < result2);
        result2 = BinaryStringUtil::toString<double>(10.1, true);
        ASSERT_TRUE(result1 > result2);
        ASSERT_EQ(9, result1.size());
        ASSERT_EQ(9, result2.size());
    }
    // float
    {
        std::string result1 = BinaryStringUtil::toString<float>(-10.1, false);
        std::string result2 = BinaryStringUtil::toString<float>(10.1, false);
        ASSERT_TRUE(result1 < result2);
        result2 = BinaryStringUtil::toString<float>(10.1, true);
        ASSERT_TRUE(result1 > result2);
        ASSERT_EQ(5, result1.size());
        ASSERT_EQ(5, result2.size());
    }
    // float
    {
        std::string result1 = BinaryStringUtil::toInvertString<float>(-10.1, false);
        std::string result2 = BinaryStringUtil::toInvertString<float>(10.1, false);
        ASSERT_TRUE(result1 > result2);
        result1 = BinaryStringUtil::toInvertString<float>(100.1, true);
        ASSERT_TRUE(result1 > result2);
        ASSERT_EQ(5, result1.size());
        ASSERT_EQ(5, result2.size());
    }

    {
        std::string result1 = BinaryStringUtil::toInvertString<int>(10, false);
        std::string result2 = BinaryStringUtil::toInvertString<int>(11, true);
        ASSERT_TRUE(result2 > result1);

        result1 = BinaryStringUtil::toInvertString<double>(10.1, false);
        result2 = BinaryStringUtil::toInvertString<double>(11.1, true);
        ASSERT_TRUE(result2 > result1);

        result1 = BinaryStringUtil::toInvertString<float>(-10.1, false);
        result2 = BinaryStringUtil::toInvertString<float>(11.1, true);
        ASSERT_TRUE(result2 > result1);
    }
}

} // namespace indexlibv2::base
