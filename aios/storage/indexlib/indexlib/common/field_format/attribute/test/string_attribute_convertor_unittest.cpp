#include "autil/StringUtil.h"
#include "indexlib/common/field_format/attribute/string_attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"

using namespace std;

namespace indexlib { namespace common {

class StringAttributeConvertorTest : public INDEXLIB_TESTBASE
{
public:
    void TestCaseForSimple()
    {
        string str;
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

    void TestCaseForFixedString()
    {
        string str = "abcdefghijk";
        int32_t count = 20;
        TestEncode(str, false, count);
        TestEncode(str, true, count);
        count = 10;
        TestEncode(str, false, count);
        TestEncode(str, true, count);
    }

private:
    void TestEncode(const std::string& str, bool needHash)
    {
        StringAttributeConvertor convertor(needHash);
        string resultStr = convertor.Encode(str);
        uint64_t expectedKey = (uint64_t)-1;
        if (needHash) {
            expectedKey = util::HashString::Hash(str.data(), str.size());
        }

        const char* ptr = resultStr.c_str();
        uint64_t hashKey = *(uint64_t*)ptr;
        INDEXLIB_TEST_EQUAL(expectedKey, hashKey);

        ptr += sizeof(hashKey);

        size_t expectedCount = str.length();
        size_t encodeLen = 0;
        bool isNull = false;
        uint32_t count = VarNumAttributeFormatter::DecodeCount(ptr, encodeLen, isNull);
        INDEXLIB_TEST_EQUAL((uint32_t)expectedCount, count);
        ptr += encodeLen;

        string value = string(ptr, (resultStr.c_str() + resultStr.size() - ptr));
        INDEXLIB_TEST_EQUAL(str.substr(0, expectedCount), value);
    }

    void TestEncode(const std::string& str, bool needHash, int32_t fixedValueCount)
    {
        StringAttributeConvertor convertor(needHash, "", fixedValueCount);
        string resultStr = convertor.Encode(str);
        uint64_t expectedKey = (uint64_t)-1;
        if (needHash) {
            expectedKey = util::HashString::Hash(str.data(), str.size());
        }

        const char* ptr = resultStr.c_str();
        uint64_t hashKey = *(uint64_t*)ptr;
        INDEXLIB_TEST_EQUAL(expectedKey, hashKey);

        ptr += sizeof(hashKey);

        size_t expectedCount = fixedValueCount;
        size_t actualCount = resultStr.size() - sizeof(hashKey);
        INDEXLIB_TEST_EQUAL((uint32_t)expectedCount, actualCount);

        string expectedValue;
        if (expectedCount <= str.size()) {
            expectedValue = str.substr(0, expectedCount);
        } else {
            expectedValue = string(str);
            for (size_t i = expectedCount - str.size(); i > 0; i--) {
                expectedValue += ' ';
            }
        }
        INDEXLIB_TEST_EQUAL(expectedValue, string(ptr, actualCount));
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(common, StringAttributeConvertorTest);

INDEXLIB_UNIT_TEST_CASE(StringAttributeConvertorTest, TestCaseForSimple);
INDEXLIB_UNIT_TEST_CASE(StringAttributeConvertorTest, TestCaseForFixedString);
}} // namespace indexlib::common
