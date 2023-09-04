#include "indexlib/index/common/field_format/attribute/StringAttributeConvertor.h"

#include "autil/StringUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class StringAttributeConvertorTest : public TESTBASE
{
public:
    StringAttributeConvertorTest() = default;
    ~StringAttributeConvertorTest() = default;
    void setUp() override {}
    void tearDown() override {}

private:
    void TestEncode(const std::string& str, bool needHash);
    void TestEncode(const std::string& str, bool needHash, int32_t fixedValueCount);
};

void StringAttributeConvertorTest::TestEncode(const std::string& str, bool needHash)
{
    StringAttributeConvertor convertor(needHash);
    std::string resultStr = convertor.Encode(str);
    uint64_t expectedKey = (uint64_t)-1;
    if (needHash) {
        expectedKey = indexlib::util::HashString::Hash(str.data(), str.size());
    }

    const char* ptr = resultStr.c_str();
    uint64_t hashKey = *(uint64_t*)ptr;
    ASSERT_EQ(expectedKey, hashKey);

    ptr += sizeof(hashKey);

    size_t expectedCount = str.length();
    size_t encodeLen = 0;
    bool isNull = false;
    uint32_t count = MultiValueAttributeFormatter::DecodeCount(ptr, encodeLen, isNull);
    ASSERT_EQ((uint32_t)expectedCount, count);
    ptr += encodeLen;

    std::string value = std::string(ptr, (resultStr.c_str() + resultStr.size() - ptr));
    ASSERT_EQ(str.substr(0, expectedCount), value);
}

void StringAttributeConvertorTest::TestEncode(const std::string& str, bool needHash, int32_t fixedValueCount)
{
    StringAttributeConvertor convertor(needHash, "", fixedValueCount);
    std::string resultStr = convertor.Encode(str);
    uint64_t expectedKey = (uint64_t)-1;
    if (needHash) {
        expectedKey = indexlib::util::HashString::Hash(str.data(), str.size());
    }

    const char* ptr = resultStr.c_str();
    uint64_t hashKey = *(uint64_t*)ptr;
    ASSERT_EQ(expectedKey, hashKey);

    ptr += sizeof(hashKey);

    size_t expectedCount = fixedValueCount;
    size_t actualCount = resultStr.size() - sizeof(hashKey);
    ASSERT_EQ((uint32_t)expectedCount, actualCount);

    std::string expectedValue;
    if (expectedCount <= str.size()) {
        expectedValue = str.substr(0, expectedCount);
    } else {
        expectedValue = std::string(str);
        for (size_t i = expectedCount - str.size(); i > 0; i--) {
            expectedValue += ' ';
        }
    }
    ASSERT_EQ(expectedValue, std::string(ptr, actualCount));
}

TEST_F(StringAttributeConvertorTest, TestCaseForSimple)
{
    std::string str;
    for (size_t i = 0; i < 10; ++i) {
        str += "1234567";
    }
    TestEncode(str, false);
    TestEncode(str, true);
    str.clear();
    for (size_t i = 0; i < 10000; ++i) {
        str += "1234567";
    }
    TestEncode(str, false);
    TestEncode(str, true);
}

TEST_F(StringAttributeConvertorTest, TestCaseForFixedString)
{
    std::string str = "abcdefghijk";
    int32_t count = 20;
    TestEncode(str, false, count);
    TestEncode(str, true, count);
    count = 10;
    TestEncode(str, false, count);
    TestEncode(str, true, count);
}

} // namespace indexlibv2::index
