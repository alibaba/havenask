#include "indexlib/index/common/field_format/attribute/SingleValueAttributeConvertor.h"

#include "autil/ConstString.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "unittest/unittest.h"

using namespace indexlib;
namespace indexlibv2::index {

class SingleValueAttributeConvertorTest : public TESTBASE
{
public:
    SingleValueAttributeConvertorTest() = default;
    ~SingleValueAttributeConvertorTest() = default;

    void setUp() override {}
    void tearDown() override {}

private:
    template <typename T>
    void TestEncode(T data);

    template <typename T>
    void TestExceptionEncode(std::string& input);

    template <typename T>
    void TestDecode(T data);

    template <typename T>
    void TestEncodeEmptyValue();

private:
    autil::mem_pool::Pool _pool;
};

template <typename T>
void SingleValueAttributeConvertorTest::TestEncode(T data)
{
    std::string input = autil::StringUtil::toString<T>(data);
    SingleValueAttributeConvertor<T> convertor;
    std::string resultStr = convertor.Encode(input);
    T result = *(T*)(resultStr.c_str());
    ASSERT_EQ(data, result);

    autil::StringView encodedStr1(resultStr.data(), resultStr.size());
    AttrValueMeta attrValueMeta = convertor.Decode(encodedStr1);
    autil::StringView encodedStr2 = convertor.EncodeFromAttrValueMeta(attrValueMeta, &_pool);
    EXPECT_EQ(encodedStr1, encodedStr2);
}

template <typename T>
void SingleValueAttributeConvertorTest::TestExceptionEncode(std::string& input)
{
    SingleValueAttributeConvertor<T> convertor;
    bool hasFormatError = false;
    autil::mem_pool::Pool pool;
    autil::StringView str(input);
    autil::StringView resultStr = convertor.Encode(str, &pool, hasFormatError);
    EXPECT_TRUE(hasFormatError);
    T result = *(T*)(resultStr.data());
    ASSERT_EQ((T)0, result);

    // empty string
    hasFormatError = false;
    str = autil::StringView::empty_instance();
    resultStr = convertor.Encode(str, &pool, hasFormatError);
    EXPECT_FALSE(hasFormatError);

    convertor.SetEncodeEmpty(true);
    resultStr = convertor.Encode(str, &pool, hasFormatError);
    EXPECT_TRUE(hasFormatError);
}

template <typename T>
void SingleValueAttributeConvertorTest::TestDecode(T data)
{
    autil::StringView input((const char*)&data, sizeof(T));
    SingleValueAttributeConvertor<T> convertor;
    AttrValueMeta meta = convertor.Decode(input);
    ASSERT_EQ(uint64_t(-1), meta.hashKey);
    T result = *(T*)(meta.data.data());
    ASSERT_EQ(data, result);
}

template <typename T>
void SingleValueAttributeConvertorTest::TestEncodeEmptyValue()
{
    SingleValueAttributeConvertor<T> convertor;
    std::string encodeValue = convertor.Encode("");
    ASSERT_EQ("", encodeValue);
}

TEST_F(SingleValueAttributeConvertorTest, TestCaseForSimple)
{
    TestEncode<int32_t>(12);
    TestDecode<int32_t>(12);
    TestEncode<float>(66.6);
    TestDecode<double>(77.7);
    std::string input = autil::StringUtil::toString<int32_t>(1000);
    TestExceptionEncode<uint8_t>(input);
}

TEST_F(SingleValueAttributeConvertorTest, TestCaseForEmptyValue)
{
    TestEncodeEmptyValue<int32_t>();
    TestEncodeEmptyValue<uint32_t>();
    TestEncodeEmptyValue<int64_t>();
    TestEncodeEmptyValue<float>();
}

TEST_F(SingleValueAttributeConvertorTest, TestCaseForConvertFactory)
{
    auto fieldConfig = std::make_shared<indexlibv2::config::FieldConfig>("int32_field", ft_int32, false);
    auto attrConfig = std::make_shared<indexlibv2::index::AttributeConfig>();
    ASSERT_TRUE(attrConfig->Init(fieldConfig).IsOK());

    std::shared_ptr<AttributeConvertor> convertor(
        AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));

    // tt_index, will not encode empty string
    bool hasFormatError = false;
    autil::mem_pool::Pool pool;
    autil::StringView resultStr = convertor->Encode(autil::StringView::empty_instance(), &pool, hasFormatError);
    EXPECT_FALSE(hasFormatError);

    // tt_kv or tt_kkv, will not encode empty string
    hasFormatError = false;
    convertor.reset(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    convertor->SetEncodeEmpty(true);
    resultStr = convertor->Encode(autil::StringView::empty_instance(), &pool, hasFormatError);
    EXPECT_TRUE(hasFormatError);
}
} // namespace indexlibv2::index
