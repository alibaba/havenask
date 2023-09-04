#include "indexlib/index/common/field_format/attribute/MultiStringAttributeConvertor.h"

#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Constant.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class MultiStringAttributeConvertorTest : public TESTBASE
{
public:
    MultiStringAttributeConvertorTest() = default;
    ~MultiStringAttributeConvertorTest() = default;
    void setUp() override {}
    void tearDown() override {}

private:
    void TestEncode(std::vector<std::string>& strVec, bool needHash, bool isBinary);
    void AppendCount(std::string& str, uint32_t count);
    void TestEncodeForNull();

private:
    autil::mem_pool::Pool _pool;
};

void MultiStringAttributeConvertorTest::TestEncode(std::vector<std::string>& strVec, bool needHash, bool isBinary)
{
    std::string input;
    std::string expectedData;
    uint32_t expectedCount = 0;
    uint32_t offset = 0;
    std::vector<uint32_t> offsetVec;

    for (size_t i = 0; i < strVec.size(); ++i) {
        input += strVec[i];
        if (i < strVec.size() - 1) {
            input += MULTI_VALUE_SEPARATOR;
        }

        size_t encodeCountLen = MultiValueAttributeFormatter::GetEncodedCountLength(strVec[i].size());
        offsetVec.push_back(offset);
        offset += (strVec[i].size() + encodeCountLen);
        AppendCount(expectedData, strVec[i].size());
        expectedData += strVec[i];
        ++expectedCount;
    }

    if (isBinary) {
        autil::DataBuffer dataBuffer;
        dataBuffer.write(strVec);
        input = std::string(dataBuffer.getData(), dataBuffer.getDataLen());
    }
    MultiStringAttributeConvertor convertor(needHash, "", isBinary, INDEXLIB_MULTI_VALUE_SEPARATOR_STR);
    std::string result = convertor.Encode(input);
    // test EncodeFromAttrValueMeta
    autil::StringView encodedStr1(result.data(), result.size());
    AttrValueMeta attrValueMeta = convertor.Decode(encodedStr1);
    autil::StringView encodedStr2 = convertor.EncodeFromAttrValueMeta(attrValueMeta, &_pool);
    ASSERT_EQ(encodedStr1, encodedStr2);

    uint64_t expectedHashKey = (uint64_t)-1;
    if (needHash) {
        expectedHashKey = indexlib::util::HashString::Hash(input.data(), input.size());
    }

    // check hashKey
    const char* ptr = result.c_str();
    uint64_t hashKey = *(uint64_t*)ptr;
    ASSERT_EQ(expectedHashKey, hashKey);
    ptr += sizeof(uint64_t);

    // check count
    size_t encodeCountLen = 0;
    bool isNull = false;
    uint32_t count = MultiValueAttributeFormatter::DecodeCount(ptr, encodeCountLen, isNull);
    ASSERT_EQ(expectedCount, count);
    ptr += encodeCountLen;

    uint8_t expectOffsetLen = MultiValueAttributeFormatter::GetOffsetItemLength(*offsetVec.rbegin());
    ASSERT_EQ(expectOffsetLen, *(uint8_t*)ptr);
    ptr++;

    // check offset
    for (uint16_t i = 0; i < count; ++i) {
        uint32_t offset = MultiValueAttributeFormatter::GetOffset(ptr, expectOffsetLen, i);
        ASSERT_EQ(offsetVec[i], offset);
    }
    ptr += (expectOffsetLen * count);

    const char* endPtr = result.c_str() + result.size();
    // check string data
    ASSERT_EQ(expectedData, std::string(ptr, endPtr - ptr));
}

void MultiStringAttributeConvertorTest::AppendCount(std::string& str, uint32_t count)
{
    char buffer[4];
    size_t encodeLen = MultiValueAttributeFormatter::EncodeCount(count, buffer, 4);
    str.append(buffer, encodeLen);
}

void MultiStringAttributeConvertorTest::TestEncodeForNull()
{
    MultiStringAttributeConvertor convertor(true, "", false, INDEXLIB_MULTI_VALUE_SEPARATOR_STR);
    auto [st, result] = convertor.EncodeNullValue();
    ASSERT_TRUE(st.IsOK());
    autil::StringView encodedStr1(result);
    AttrValueMeta attrValueMeta = convertor.Decode(encodedStr1);
    autil::StringView encodedStr2 = convertor.EncodeFromAttrValueMeta(attrValueMeta, &_pool);
    ASSERT_EQ(encodedStr1, encodedStr2);

    const char* ptr = result.c_str() + sizeof(uint64_t);
    bool isNull = false;
    uint64_t encodeCountLen = 0;
    MultiValueAttributeFormatter::DecodeCount(ptr, encodeCountLen, isNull);
    ASSERT_EQ(true, isNull);
}

TEST_F(MultiStringAttributeConvertorTest, TestCaseForSimple)
{
    std::vector<std::string> strVec;
    for (size_t i = 0; i < 10000; ++i) {
        std::string value = "attribute";
        value += autil::StringUtil::toString(i);
        strVec.push_back(value);
    }
    TestEncode(strVec, false, false);
    TestEncode(strVec, true, false);
    TestEncode(strVec, false, true);
    TestEncode(strVec, true, true);
    TestEncodeForNull();
}
} // namespace indexlibv2::index
