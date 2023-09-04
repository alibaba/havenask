#include "indexlib/index/common/field_format/attribute/MultiValueAttributeConvertor.h"

#include "autil/StringUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class MultiValueAttributeConvertorTest : public TESTBASE
{
public:
    MultiValueAttributeConvertorTest() = default;
    ~MultiValueAttributeConvertorTest() = default;
    void setUp() override {}
    void tearDown() override {}

private:
    template <typename T>
    void TestEncode(const std::vector<T>& data, bool needHash, int32_t fixedValueCount,
                    const std::string& seperator = INDEXLIB_MULTI_VALUE_SEPARATOR_STR);

    void TestEncodeWithError(const std::vector<int>& data);

    template <typename T>
    void TestEncodeNull();

    template <typename T>
    void TestDecode(std::vector<T> data);

private:
    autil::mem_pool::Pool _pool;
};
TEST_F(MultiValueAttributeConvertorTest, TestCaseForSimple)
{
    int32_t dataArray[] = {1, 2, 3, 4, 5, 6, 7, -1, 0, 2};
    std::vector<int32_t> data(dataArray, dataArray + sizeof(dataArray) / sizeof(int32_t));
    TestEncode<int32_t>(data, true, -1);
    TestEncode<int32_t>(data, false, -1);
    TestEncode<int32_t>(data, true, -1, ",");
    TestEncode<int32_t>(data, false, -1, ",");
    TestDecode<int32_t>(data);
    TestEncodeWithError({5, 8, 10, 1000});
    TestEncodeNull<int32_t>();
    TestEncodeNull<float>();
}

TEST_F(MultiValueAttributeConvertorTest, TestCaseForCountedMultiValueType)
{
    int32_t dataArray[] = {1, 2, 3, 4, -1, -2};
    std::vector<int32_t> data(dataArray, dataArray + sizeof(dataArray) / sizeof(int32_t));
    TestEncode<int32_t>(data, true, data.size());
}

template <typename T>
void MultiValueAttributeConvertorTest::TestEncode(const std::vector<T>& data, bool needHash, int32_t fixedValueCount,
                                                  const std::string& seperator)
{
    std::string input;
    for (size_t i = 0; i < data.size(); ++i) {
        std::string value = autil::StringUtil::toString<T>(data[i]);
        input += value;
        input += seperator;
    }
    MultiValueAttributeConvertor<T> convertor(needHash, "", fixedValueCount, seperator);
    std::string resultStr = convertor.Encode(input);

    // test EncodeFromAttrValueMeta
    autil::StringView encodedStr1(resultStr.data(), resultStr.size());
    AttrValueMeta attrValueMeta = convertor.Decode(encodedStr1);
    autil::StringView encodedStr2 = convertor.EncodeFromAttrValueMeta(attrValueMeta, &_pool);
    ASSERT_EQ(encodedStr1, encodedStr2);

    const char* ptr = resultStr.c_str();
    uint64_t key = *(uint64_t*)ptr;
    uint64_t expectedKey = (uint64_t)-1;
    if (needHash) {
        expectedKey = indexlib::util::HashString::Hash(input.data(), input.size());
    }

    ASSERT_EQ(expectedKey, key);
    ptr += sizeof(key);

    if (fixedValueCount == -1) {
        size_t encodeLen = 0;
        bool isNull = false;
        uint32_t tokenSize = MultiValueAttributeFormatter::DecodeCount(ptr, encodeLen, isNull);
        uint32_t expectedTokenSize = data.size();
        ASSERT_EQ(expectedTokenSize, tokenSize);
        ptr += encodeLen;
    }
    for (size_t i = 0; i < data.size(); ++i) {
        T value = *(T*)ptr;
        ASSERT_EQ(data[i], value);
        ptr += sizeof(T);
    }
    ASSERT_EQ(size_t(ptr - resultStr.c_str()), resultStr.length());
}

void MultiValueAttributeConvertorTest::TestEncodeWithError(const std::vector<int>& data)
{
    std::string input;
    for (size_t i = 0; i < data.size(); ++i) {
        std::string value = autil::StringUtil::toString<int>(data[i]);
        input += value;
        input += autil::MULTI_VALUE_DELIMITER;
    }

    bool hasFormatError = false;
    autil::mem_pool::Pool pool;
    autil::StringView str(input);

    MultiValueAttributeConvertor<int8_t> convertor;
    autil::StringView resultStr = convertor.Encode(str, &pool, hasFormatError);
    EXPECT_TRUE(hasFormatError);
    const char* ptr = resultStr.data();
    uint64_t key = *(uint64_t*)ptr;
    uint64_t expectedKey = (uint64_t)-1;
    ASSERT_EQ(expectedKey, key);
    ptr += sizeof(key);

    size_t encodeLen = 0;
    bool isNull = false;
    uint32_t tokenSize = MultiValueAttributeFormatter::DecodeCount(ptr, encodeLen, isNull);
    uint32_t expectedTokenSize = data.size();
    ASSERT_EQ(expectedTokenSize, tokenSize);
    ptr += encodeLen;

    for (size_t i = 0; i < data.size() - 1; ++i) {
        int8_t value = *(int8_t*)ptr;
        ASSERT_EQ(data[i], value);
        ptr += sizeof(int8_t);
    }

    ASSERT_EQ(0, *(int8_t*)ptr);
    ptr += sizeof(int8_t);
    ASSERT_EQ(size_t(ptr - resultStr.data()), resultStr.length());
}

template <typename T>
void MultiValueAttributeConvertorTest::TestEncodeNull()
{
    MultiValueAttributeConvertor<T> convertor = MultiValueAttributeConvertor<T>();
    auto [st, result] = convertor.EncodeNullValue();
    ASSERT_TRUE(st.IsOK());
    autil::StringView encodedStr1(result);
    const char* ptr = result.data() + sizeof(uint64_t);
    AttrValueMeta attrValueMeta = convertor.Decode(encodedStr1);
    autil::StringView encodedStr2 = convertor.EncodeFromAttrValueMeta(attrValueMeta, &_pool);
    ASSERT_EQ(encodedStr1, encodedStr2);

    size_t encodeLen = 0;
    bool isNull = false;
    MultiValueAttributeFormatter::DecodeCount(ptr, encodeLen, isNull);
    ASSERT_EQ(true, isNull);
}

template <typename T>
void MultiValueAttributeConvertorTest::TestDecode(std::vector<T> data)
{
    std::string input;
    uint64_t expectedHashKey = 0x123;
    input.append((const char*)&expectedHashKey, sizeof(expectedHashKey));
    for (size_t i = 0; i < data.size(); ++i) {
        input.append((const char*)&data, sizeof(T));
    }
    MultiValueAttributeConvertor<T> convertor;
    AttrValueMeta meta = convertor.Decode(autil::StringView(input));
    ASSERT_EQ(expectedHashKey, meta.hashKey);
    ASSERT_EQ(input.size() - sizeof(expectedHashKey), meta.data.size());
    autil::StringView str(input.c_str() + sizeof(expectedHashKey), input.size() - sizeof(expectedHashKey));
    ASSERT_EQ(str, meta.data);
}
} // namespace indexlibv2::index
