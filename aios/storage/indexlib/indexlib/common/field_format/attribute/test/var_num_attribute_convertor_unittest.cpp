#include "autil/StringUtil.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace common {

class VarNumAttributeConvertorTest : public INDEXLIB_TESTBASE
{
public:
    void TestCaseForSimple()
    {
        int32_t dataArray[] = {1, 2, 3, 4, 5, 6, 7, -1, 0, 2};
        vector<int32_t> data(dataArray, dataArray + sizeof(dataArray) / sizeof(int32_t));
        TestEncode<int32_t>(data, true, -1);
        TestEncode<int32_t>(data, false, -1);
        TestEncode<int32_t>(data, true, -1, ",");
        TestEncode<int32_t>(data, false, -1, ",");
        TestDecode<int32_t>(data);
        TestEncodeWithError({5, 8, 10, 1000});
        TestEncodeNull<int32_t>();
        TestEncodeNull<float>();
    }

    void TestCaseForCountedMultiValueType()
    {
        int32_t dataArray[] = {1, 2, 3, 4, -1, -2};
        vector<int32_t> data(dataArray, dataArray + sizeof(dataArray) / sizeof(int32_t));
        TestEncode<int32_t>(data, true, data.size());
    }

private:
    template <typename T>
    void TestEncode(const vector<T>& data, bool needHash, int32_t fixedValueCount,
                    const std::string& seperator = INDEXLIB_MULTI_VALUE_SEPARATOR_STR)
    {
        string input;
        for (size_t i = 0; i < data.size(); ++i) {
            string value = autil::StringUtil::toString<T>(data[i]);
            input += value;
            input += seperator;
        }
        VarNumAttributeConvertor<T> convertor(needHash, "", fixedValueCount, seperator);
        string resultStr = convertor.Encode(input);

        // test EncodeFromAttrValueMeta
        StringView encodedStr1(resultStr.data(), resultStr.size());
        AttrValueMeta attrValueMeta = convertor.Decode(encodedStr1);
        StringView encodedStr2 = convertor.EncodeFromAttrValueMeta(attrValueMeta, &mPool);
        ASSERT_EQ(encodedStr1, encodedStr2);

        const char* ptr = resultStr.c_str();
        uint64_t key = *(uint64_t*)ptr;
        uint64_t expectedKey = (uint64_t)-1;
        if (needHash) {
            expectedKey = util::HashString::Hash(input.data(), input.size());
        }

        INDEXLIB_TEST_EQUAL(expectedKey, key);
        ptr += sizeof(key);

        if (fixedValueCount == -1) {
            size_t encodeLen = 0;
            bool isNull = false;
            uint32_t tokenSize = VarNumAttributeFormatter::DecodeCount(ptr, encodeLen, isNull);
            uint32_t expectedTokenSize = data.size();
            INDEXLIB_TEST_EQUAL(expectedTokenSize, tokenSize);
            ptr += encodeLen;
        }
        for (size_t i = 0; i < data.size(); ++i) {
            T value = *(T*)ptr;
            INDEXLIB_TEST_EQUAL(data[i], value);
            ptr += sizeof(T);
        }
        INDEXLIB_TEST_EQUAL(size_t(ptr - resultStr.c_str()), resultStr.length());
    }

    void TestEncodeWithError(const vector<int>& data)
    {
        string input;
        for (size_t i = 0; i < data.size(); ++i) {
            string value = autil::StringUtil::toString<int>(data[i]);
            input += value;
            input += MULTI_VALUE_SEPARATOR;
        }

        bool hasFormatError = false;
        autil::mem_pool::Pool pool;
        autil::StringView str(input);

        VarNumAttributeConvertor<int8_t> convertor(false);
        autil::StringView resultStr = convertor.Encode(str, &pool, hasFormatError);
        EXPECT_TRUE(hasFormatError);
        const char* ptr = resultStr.data();
        uint64_t key = *(uint64_t*)ptr;
        uint64_t expectedKey = (uint64_t)-1;
        INDEXLIB_TEST_EQUAL(expectedKey, key);
        ptr += sizeof(key);

        size_t encodeLen = 0;
        bool isNull = false;
        uint32_t tokenSize = VarNumAttributeFormatter::DecodeCount(ptr, encodeLen, isNull);
        uint32_t expectedTokenSize = data.size();
        INDEXLIB_TEST_EQUAL(expectedTokenSize, tokenSize);
        ptr += encodeLen;

        for (size_t i = 0; i < data.size() - 1; ++i) {
            int8_t value = *(int8_t*)ptr;
            INDEXLIB_TEST_EQUAL(data[i], value);
            ptr += sizeof(int8_t);
        }

        INDEXLIB_TEST_EQUAL(0, *(int8_t*)ptr);
        ptr += sizeof(int8_t);
        INDEXLIB_TEST_EQUAL(size_t(ptr - resultStr.data()), resultStr.length());
    }

    template <typename T>
    void TestEncodeNull()
    {
        VarNumAttributeConvertor<T> convertor = VarNumAttributeConvertor<T>();
        string result = convertor.EncodeNullValue();
        StringView encodedStr1(result);
        const char* ptr = result.data() + sizeof(uint64_t);
        AttrValueMeta attrValueMeta = convertor.Decode(encodedStr1);
        StringView encodedStr2 = convertor.EncodeFromAttrValueMeta(attrValueMeta, &mPool);
        ASSERT_EQ(encodedStr1, encodedStr2);

        size_t encodeLen = 0;
        bool isNull = false;
        VarNumAttributeFormatter::DecodeCount(ptr, encodeLen, isNull);
        INDEXLIB_TEST_EQUAL(true, isNull);
    }

    template <typename T>
    void TestDecode(std::vector<T> data)
    {
        string input;
        uint64_t expectedHashKey = 0x123;
        input.append((const char*)&expectedHashKey, sizeof(expectedHashKey));
        for (size_t i = 0; i < data.size(); ++i) {
            input.append((const char*)&data, sizeof(T));
        }
        VarNumAttributeConvertor<T> convertor;
        AttrValueMeta meta = convertor.Decode(autil::StringView(input));
        INDEXLIB_TEST_EQUAL(expectedHashKey, meta.hashKey);
        INDEXLIB_TEST_EQUAL(input.size() - sizeof(expectedHashKey), meta.data.size());
        autil::StringView str(input.c_str() + sizeof(expectedHashKey), input.size() - sizeof(expectedHashKey));
        INDEXLIB_TEST_EQUAL(str, meta.data);
    }

private:
    autil::mem_pool::Pool mPool;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(common, VarNumAttributeConvertorTest);

INDEXLIB_UNIT_TEST_CASE(VarNumAttributeConvertorTest, TestCaseForSimple);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeConvertorTest, TestCaseForCountedMultiValueType);
}} // namespace indexlib::common
