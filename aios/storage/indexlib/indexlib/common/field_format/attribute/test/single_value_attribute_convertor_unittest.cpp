#include "autil/ConstString.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common/field_format/attribute/single_value_attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace common {

class SingleValueAttributeConvertorTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForSimple()
    {
        TestEncode<int32_t>(12);
        TestDecode<int32_t>(12);
        TestEncode<float>(66.6);
        TestDecode<double>(77.7);
        string input = autil::StringUtil::toString<int32_t>(1000);
        TestExceptionEncode<uint8_t>(input);
    }

    void TestCaseForEmptyValue()
    {
        TestEncodeEmptyValue<int32_t>();
        TestEncodeEmptyValue<uint32_t>();
        TestEncodeEmptyValue<int64_t>();
        TestEncodeEmptyValue<float>();
    }

    void TestCaseForConvertFactory()
    {
        config::FieldConfigPtr fieldConfig(new config::FieldConfig("int32_field", ft_int32, false));
        config::AttributeConfigPtr attrConfig(new config::AttributeConfig());
        attrConfig->Init(fieldConfig);
        AttributeConvertorPtr convertor(
            AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig, tt_index));

        // tt_index, will not encode empty string
        bool hasFormatError = false;
        autil::mem_pool::Pool pool;
        autil::StringView resultStr = convertor->Encode(autil::StringView::empty_instance(), &pool, hasFormatError);
        EXPECT_FALSE(hasFormatError);

        // tt_kv or tt_kkv, will not encode empty string
        hasFormatError = false;
        convertor.reset(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig, tt_kkv));
        resultStr = convertor->Encode(autil::StringView::empty_instance(), &pool, hasFormatError);
        EXPECT_TRUE(hasFormatError);

        hasFormatError = false;
        convertor.reset(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig, tt_kv));
        resultStr = convertor->Encode(autil::StringView::empty_instance(), &pool, hasFormatError);
        EXPECT_TRUE(hasFormatError);
    }

private:
    template <typename T>
    void TestEncode(T data)
    {
        string input = autil::StringUtil::toString<T>(data);
        SingleValueAttributeConvertor<T> convertor;
        string resultStr = convertor.Encode(input);
        T result = *(T*)(resultStr.c_str());
        INDEXLIB_TEST_EQUAL(data, result);

        StringView encodedStr1(resultStr.data(), resultStr.size());
        AttrValueMeta attrValueMeta = convertor.Decode(encodedStr1);
        StringView encodedStr2 = convertor.EncodeFromAttrValueMeta(attrValueMeta, &mPool);
        EXPECT_EQ(encodedStr1, encodedStr2);
    }

    template <typename T>
    void TestExceptionEncode(string& input)
    {
        SingleValueAttributeConvertor<T> convertor;
        bool hasFormatError = false;
        autil::mem_pool::Pool pool;
        autil::StringView str(input);
        autil::StringView resultStr = convertor.Encode(str, &pool, hasFormatError);
        EXPECT_TRUE(hasFormatError);
        T result = *(T*)(resultStr.data());
        INDEXLIB_TEST_EQUAL((T)0, result);

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
    void TestDecode(T data)
    {
        autil::StringView input((const char*)&data, sizeof(T));
        SingleValueAttributeConvertor<T> convertor;
        AttrValueMeta meta = convertor.Decode(input);
        INDEXLIB_TEST_EQUAL(uint64_t(-1), meta.hashKey);
        T result = *(T*)(meta.data.data());
        INDEXLIB_TEST_EQUAL(data, result);
    }

    template <typename T>
    void TestEncodeEmptyValue()
    {
        SingleValueAttributeConvertor<T> convertor;
        string encodeValue = convertor.Encode("");
        INDEXLIB_TEST_EQUAL("", encodeValue);
    }

private:
    autil::mem_pool::Pool mPool;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(common, SingleValueAttributeConvertorTest);

INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeConvertorTest, TestCaseForSimple);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeConvertorTest, TestCaseForEmptyValue);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeConvertorTest, TestCaseForConvertFactory);
}} // namespace indexlib::common
