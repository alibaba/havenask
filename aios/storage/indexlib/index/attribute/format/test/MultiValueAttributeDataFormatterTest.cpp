#include "indexlib/index/attribute/format/MultiValueAttributeDataFormatter.h"

#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/MultiStringAttributeConvertor.h"
#include "unittest/unittest.h"

using namespace indexlibv2::config;
namespace indexlibv2::index {

class MultiValueAttributeDataFormatterTest : public TESTBASE
{
public:
    MultiValueAttributeDataFormatterTest() = default;
    ~MultiValueAttributeDataFormatterTest() = default;

    void setUp() override {}
    void tearDown() override {}

private:
    std::shared_ptr<AttributeConfig> CreateAttrConfig(FieldType fieldType, bool isMultiValue, bool isUpdatable);
    void CheckFormatterInit(FieldType fieldType, bool isMultiValue, bool isUpdatable, uint32_t fieldSize);
};
std::shared_ptr<AttributeConfig> MultiValueAttributeDataFormatterTest::CreateAttrConfig(FieldType fieldType,
                                                                                        bool isMultiValue,
                                                                                        bool isUpdatableMultiValue)
{
    auto fieldConfig = std::make_shared<FieldConfig>("test", fieldType, isMultiValue);
    auto attrConfig = std::make_shared<AttributeConfig>();
    auto status = attrConfig->Init(fieldConfig);
    if (!status.IsOK()) {
        assert(false);
        return nullptr;
    }
    if (isMultiValue || fieldType == ft_string) {
        attrConfig->SetUpdatable(isUpdatableMultiValue);
    }
    return attrConfig;
}

void MultiValueAttributeDataFormatterTest::CheckFormatterInit(FieldType fieldType, bool isMultiValue, bool isUpdatable,
                                                              uint32_t fieldSizeShift)
{
    auto attrConfig = CreateAttrConfig(fieldType, isMultiValue, isUpdatable);

    MultiValueAttributeDataFormatter formatter;
    formatter.Init(attrConfig);
    ASSERT_EQ(fieldType == ft_string && isMultiValue, formatter._isMultiString);
    ASSERT_EQ(fieldSizeShift, formatter._fieldSizeShift);
}

TEST_F(MultiValueAttributeDataFormatterTest, TestInit)
{
    CheckFormatterInit(ft_integer, true, true, 2);
    CheckFormatterInit(ft_float, true, true, 2);
    CheckFormatterInit(ft_double, true, true, 3);
    CheckFormatterInit(ft_long, true, true, 3);
    CheckFormatterInit(ft_uint32, true, true, 2);
    CheckFormatterInit(ft_uint64, true, true, 3);
    CheckFormatterInit(ft_uint8, true, true, 0);
    CheckFormatterInit(ft_int8, true, true, 0);
    CheckFormatterInit(ft_int16, true, true, 1);
    CheckFormatterInit(ft_uint16, true, true, 1);
    CheckFormatterInit(ft_string, true, true, 0);
    CheckFormatterInit(ft_string, false, true, 0);
}
TEST_F(MultiValueAttributeDataFormatterTest, TestGetNormalAttrDataLength)
{
    uint8_t value[10];
    uint8_t nullValue[10];
    MultiValueAttributeFormatter::EncodeCount(2, (char*)value, 10);
    MultiValueAttributeFormatter::EncodeCount(MultiValueAttributeFormatter::MULTI_VALUE_NULL_FIELD_VALUE_COUNT,
                                              (char*)nullValue, 10);
    {
        // test string
        auto attrConfig = CreateAttrConfig(ft_string, false, true);
        MultiValueAttributeDataFormatter formatter;
        formatter.Init(attrConfig);
        bool isNull = false;
        ASSERT_EQ((uint32_t)3, formatter.GetNormalAttrDataLength(value, isNull));
        ASSERT_FALSE(isNull);

        ASSERT_EQ(4, formatter.GetNormalAttrDataLength(nullValue, isNull));
        ASSERT_TRUE(isNull);
    }
    {
        // test int
        auto attrConfig = CreateAttrConfig(ft_uint32, true, true);
        MultiValueAttributeDataFormatter formatter;
        formatter.Init(attrConfig);
        bool isNull = false;
        ASSERT_EQ((uint32_t)9, formatter.GetNormalAttrDataLength(value, isNull));
        ASSERT_FALSE(isNull);

        ASSERT_EQ(4, formatter.GetNormalAttrDataLength(nullValue, isNull));
        ASSERT_TRUE(isNull);
    }
}
TEST_F(MultiValueAttributeDataFormatterTest, TestGetNormalEncodeFloatAttrDataLength)
{
    {
        // fixed length, no compress
        auto attrConfig = CreateAttrConfig(ft_float, true, true);
        attrConfig->GetFieldConfig()->SetFixedMultiValueCount(4);
        MultiValueAttributeDataFormatter formatter;
        formatter.Init(attrConfig);
        bool isNull = false;
        ASSERT_EQ((uint32_t)16, formatter.GetNormalAttrDataLength(NULL, isNull));
        ASSERT_FALSE(isNull);
    }
    {
        // fixed length, fp16 compress
        auto attrConfig = CreateAttrConfig(ft_float, true, true);
        attrConfig->GetFieldConfig()->SetFixedMultiValueCount(4);
        ASSERT_TRUE(attrConfig->SetCompressType("fp16").IsOK());

        MultiValueAttributeDataFormatter formatter;
        formatter.Init(attrConfig);
        bool isNull = false;
        ASSERT_EQ((uint32_t)8, formatter.GetNormalAttrDataLength(NULL, isNull));
        ASSERT_FALSE(isNull);
    }
    {
        // fixed length, block_fp compress
        auto attrConfig = CreateAttrConfig(ft_float, true, true);
        attrConfig->GetFieldConfig()->SetFixedMultiValueCount(4);
        ASSERT_TRUE(attrConfig->SetCompressType("block_fp").IsOK());

        MultiValueAttributeDataFormatter formatter;
        formatter.Init(attrConfig);
        bool isNull = false;
        ASSERT_EQ((uint32_t)9, formatter.GetNormalAttrDataLength(NULL, isNull));
        ASSERT_FALSE(isNull);
    }
}
TEST_F(MultiValueAttributeDataFormatterTest, TestGetMultiStringAttrDataLength)
{
    auto attrConfig = CreateAttrConfig(ft_string, true, true);
    MultiValueAttributeDataFormatter formatter;
    formatter.Init(attrConfig);

    MultiStringAttributeConvertor convertor;
    std::string value = convertor.Encode("abcefgefgh");
    AttrValueMeta meta = convertor.Decode(autil::StringView(value));
    uint32_t expectLength = MultiValueAttributeFormatter::GetEncodedCountLength(3) + sizeof(uint8_t) +
                            sizeof(uint8_t) * 3 + MultiValueAttributeFormatter::GetEncodedCountLength(3) + 3 +
                            MultiValueAttributeFormatter::GetEncodedCountLength(3) + 3 +
                            MultiValueAttributeFormatter::GetEncodedCountLength(4) + 4;
    bool isNull = false;
    ASSERT_EQ(expectLength, formatter.GetMultiStringAttrDataLength((const uint8_t*)meta.data.data(), isNull));
    ASSERT_FALSE(isNull);

    auto [st, v] = convertor.EncodeNullValue();
    ASSERT_TRUE(st.IsOK());
    meta = convertor.Decode(autil::StringView(v));
    ASSERT_EQ(4, formatter.GetMultiStringAttrDataLength((const uint8_t*)meta.data.data(), isNull));
    ASSERT_TRUE(isNull);
}

} // namespace indexlibv2::index
