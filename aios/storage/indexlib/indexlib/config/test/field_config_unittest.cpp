#include "indexlib/config/test/field_config_unittest.h"

#include "autil/legacy/jsonizable.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/KeyValueMap.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

using namespace indexlib::util;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, FieldConfigTest);

FieldConfigTest::FieldConfigTest() {}

FieldConfigTest::~FieldConfigTest() {}

void FieldConfigTest::TestJsonize()
{
    string fieldConfig = "{\n                                        \
   \"field_name\":\"int1\",\n               \
   \"field_type\":\"INTEGER\",\n            \
   \"multi_value\":true,\n                  \
   \"uniq_encode\":true,\n                  \
   \"u32offset_threshold\":10,\n            \
   \"compress_type\":\"uniq | equal\",\n    \
   \"default_value\":\"100\",\n             \
   \"defrag_slice_percent\":60,\n           \
   \"fixed_multi_value_count\":10\n         \
}";

    FieldConfig config;
    Any any = ParseJson(fieldConfig);
    FromJson(config, any);

    ASSERT_EQ(string("int1"), config.GetFieldName());
    ASSERT_EQ(ft_integer, config.GetFieldType());
    ASSERT_TRUE(config.IsMultiValue());
    ASSERT_EQ((uint64_t)10, config.GetU32OffsetThreshold());
    ASSERT_EQ(string("100"), config.GetDefaultValue());
    ASSERT_EQ(60, config.GetDefragSlicePercent());
    ASSERT_EQ((int)10, config.GetFixedMultiValueCount());

    CompressTypeOption type;
    ASSERT_TRUE(type.Init(string("uniq | equal")).IsOK());
    ASSERT_TRUE(config.GetCompressType().AssertEqual(type).IsOK());

    // test to json
    Any toAny = ToJson(config);
    string jsonString = ToString(toAny);
    Any comparedAny = ParseJson(jsonString);
    FieldConfig comparedConfig;
    FromJson(comparedConfig, toAny);
    ASSERT_NO_THROW(config.AssertEqual(comparedConfig));

    {
        string tsFieldConfig = "{\n                                          \
   \"field_name\":\"timestamp\",\n            \
   \"field_type\":\"TIMESTAMP\",\n            \
   \"default_time_zone\":\"+0800\"\n          \
}";

        FieldConfig tsConfig;
        Any any = ParseJson(tsFieldConfig);
        FromJson(tsConfig, any);
        ASSERT_EQ((int)-8 * 3600000, tsConfig.GetDefaultTimeZoneDelta());

        // test to json
        Any toAny = ToJson(tsConfig);
        FieldConfig newTsConfig;
        FromJson(newTsConfig, toAny);
        ASSERT_EQ((int)-8 * 3600000, tsConfig.GetDefaultTimeZoneDelta());
    }
}

void FieldConfigTest::TestJsonizeU32OffsetThreshold()
{
    string fieldConfig = "{\n                                       \
   \"field_name\":\"int1\",\n              \
   \"field_type\":\"INTEGER\",\n           \
   \"u32offset_threshold\":10\n            \
}";
    {
        FieldConfig config;
        Any any = ParseJson(fieldConfig);
        FromJson(config, any);
        ASSERT_EQ(config.GetU32OffsetThreshold(), (uint64_t)10);

        // test to json
        Any toAny = ToJson(config);
        string jsonString = ToString(toAny);
        Any comparedAny = ParseJson(jsonString);
        FieldConfig comparedConfig;
        FromJson(comparedConfig, toAny);
        ASSERT_NO_THROW(config.AssertEqual(comparedConfig));
    }

    fieldConfig = "{\n                                       \
   \"field_name\":\"int1\",\n              \
   \"field_type\":\"INTEGER\"\n            \
}";
    {
        FieldConfig config;
        Any any = ParseJson(fieldConfig);
        FromJson(config, any);
        ASSERT_EQ(config.GetU32OffsetThreshold(), (uint64_t)index::ATTRIBUTE_U32OFFSET_THRESHOLD_MAX);
        // test to json
        Any toAny = ToJson(config);
        string jsonString = ToString(toAny);
        Any comparedAny = ParseJson(jsonString);
        FieldConfig comparedConfig;
        FromJson(comparedConfig, toAny);
        ASSERT_NO_THROW(config.AssertEqual(comparedConfig));
    }

    fieldConfig = "{\n                                       \
   \"field_name\":\"int1\",\n              \
   \"field_type\":\"INTEGER\",\n           \
   \"u32offset_threshold\":9999999999\n    \
}";
    {
        FieldConfig config;
        Any any = ParseJson(fieldConfig);
        FromJson(config, any);
        ASSERT_EQ(config.GetU32OffsetThreshold(), (uint64_t)index::ATTRIBUTE_U32OFFSET_THRESHOLD_MAX);

        // test to json
        Any toAny = ToJson(config);
        string jsonString = ToString(toAny);
        Any comparedAny = ParseJson(jsonString);
        FieldConfig comparedConfig;
        FromJson(comparedConfig, toAny);
        ASSERT_NO_THROW(config.AssertEqual(comparedConfig));
    }
}
void FieldConfigTest::TestAssertEqual()
{
    // only test new function for assert equal
    FieldConfig config;
    config.SetCompressType("equal|uniq");

    {
        FieldConfig newConfig;
        newConfig.SetCompressType("equal|uniq");
        ASSERT_NO_THROW(config.AssertEqual(newConfig));
    }

    {
        FieldConfig newConfig;
        newConfig.SetCompressType("uniq");
        ASSERT_ANY_THROW(config.AssertEqual(newConfig));
    }

    {
        FieldConfig newConfig;
        newConfig.SetCompressType("");
        ASSERT_ANY_THROW(config.AssertEqual(newConfig));
    }

    {
        FieldConfig newConfig;
        newConfig.SetCompressType("equal");
        ASSERT_ANY_THROW(config.AssertEqual(newConfig));
    }

    // fixed multi value
    config.SetFixedMultiValueCount(10);
    {
        FieldConfig newConfig;
        newConfig.SetCompressType("equal|uniq");
        newConfig.SetFixedMultiValueCount(10);
        ASSERT_NO_THROW(config.AssertEqual(newConfig));
    }
}

void FieldConfigTest::TestCheckUniqEncode()
{
    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetCompressType("uniq");
        ASSERT_NO_THROW(config.CheckUniqEncode());
    }

    {
        FieldConfig config;
        config.SetFieldType(ft_string);
        config.SetCompressType("uniq");
        ASSERT_NO_THROW(config.CheckUniqEncode());
    }

    {
        FieldConfig config;
        config.SetFieldType(ft_integer);
        config.SetCompressType("uniq");
        ASSERT_ANY_THROW(config.CheckUniqEncode());
    }
}

void FieldConfigTest::TestCheckBlockFpEncode()
{
    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetCompressType("uniq");
        ASSERT_NO_THROW(config.CheckBlockFpEncode());
    }

    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFixedMultiValueCount(4);
        config.SetCompressType("block_fp");
        config.SetFieldType(ft_float);
        ASSERT_NO_THROW(config.CheckBlockFpEncode());
    }

    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFixedMultiValueCount(4);
        config.SetCompressType("block_fp");
        config.SetFieldType(ft_integer);
        ASSERT_ANY_THROW(config.CheckBlockFpEncode());

        config.SetFieldType(ft_float);
        config.SetFixedMultiValueCount(-1);
        ASSERT_ANY_THROW(config.CheckBlockFpEncode());

        config.SetMultiValue(false);
        config.SetFixedMultiValueCount(4);
        ASSERT_ANY_THROW(config.CheckBlockFpEncode());
    }
}

void FieldConfigTest::TestCheckFp16Encode()
{
    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetCompressType("uniq");
        ASSERT_NO_THROW(config.CheckFp16Encode());
    }

    {
        // single float, can't use fp16 & equal
        FieldConfig config;
        config.SetMultiValue(false);
        config.SetCompressType("fp16|equal");
        config.SetFieldType(ft_float);
        ASSERT_ANY_THROW(config.CheckFp16Encode());
        // multi float
        FieldConfig config2;
        config2.SetMultiValue(true);
        config2.SetCompressType("fp16|equal");
        config2.SetFieldType(ft_float);
        ASSERT_NO_THROW(config2.CheckFp16Encode());
    }

    {
        FieldConfig config;
        config.SetMultiValue(false);
        config.SetCompressType("fp16");
        config.SetFieldType(ft_float);
        ASSERT_NO_THROW(config.CheckFp16Encode());
    }

    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFixedMultiValueCount(4);
        config.SetCompressType("fp16");
        config.SetFieldType(ft_float);
        ASSERT_NO_THROW(config.CheckFp16Encode());
    }

    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFixedMultiValueCount(4);
        config.SetCompressType("fp16");
        config.SetFieldType(ft_integer);
        ASSERT_ANY_THROW(config.CheckFp16Encode());

        config.SetFieldType(ft_float);
        config.SetFixedMultiValueCount(-1);
        ASSERT_NO_THROW(config.CheckFp16Encode());

        config.SetMultiValue(false);
        config.SetFixedMultiValueCount(4);
        ASSERT_NO_THROW(config.CheckFp16Encode());
    }
}

void FieldConfigTest::TestCheckFloatInt8Encode()
{
    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetCompressType("uniq");
        ASSERT_NO_THROW(config.CheckFloatInt8Encode());
    }

    {
        // single float, can't use int8compress & equal
        FieldConfig config;
        config.SetMultiValue(false);
        config.SetCompressType("int8#0.3|equal");
        config.SetFieldType(ft_float);
        ASSERT_ANY_THROW(config.CheckFloatInt8Encode());
        // multi float
        FieldConfig config2;
        config2.SetMultiValue(true);
        config2.SetCompressType("int8#0.3|equal");
        config2.SetFieldType(ft_float);
        ASSERT_NO_THROW(config2.CheckFloatInt8Encode());
    }

    {
        FieldConfig config;
        config.SetMultiValue(false);
        config.SetCompressType("int8#0.3");
        config.SetFieldType(ft_float);
        ASSERT_NO_THROW(config.CheckFloatInt8Encode());
    }

    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFixedMultiValueCount(4);
        config.SetCompressType("int8#0.3");
        config.SetFieldType(ft_float);
        ASSERT_NO_THROW(config.CheckFloatInt8Encode());
    }

    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFixedMultiValueCount(4);
        config.SetCompressType("int8#0.3");
        config.SetFieldType(ft_integer);
        ASSERT_ANY_THROW(config.CheckFloatInt8Encode());

        config.SetFieldType(ft_float);
        config.SetFixedMultiValueCount(-1);
        ASSERT_NO_THROW(config.CheckFloatInt8Encode());

        config.SetMultiValue(false);
        config.SetFixedMultiValueCount(4);
        ASSERT_NO_THROW(config.CheckFloatInt8Encode());
    }
}

void FieldConfigTest::TestCheckFixedLengthMultiValue()
{
    // multi value int 32
    {
        FieldConfig config;
        config.SetFieldType(ft_int32);
        config.SetMultiValue(true);
        // fixed size
        config.SetFixedMultiValueCount(100);
        ASSERT_NO_THROW(config.CheckFixedLengthMultiValue());
        // var size
        config.SetFixedMultiValueCount(-1);
        ASSERT_NO_THROW(config.CheckFixedLengthMultiValue());
        // zero size
        config.SetFixedMultiValueCount(0);
        ASSERT_THROW(config.CheckFixedLengthMultiValue(), SchemaException);

        // fixed size with enable null
        config.SetFixedMultiValueCount(100);
        config.SetEnableNullField(true);
        ASSERT_THROW(config.CheckFixedLengthMultiValue(), SchemaException);
    }
    // multi value string
    {
        FieldConfig config;
        config.SetFieldType(ft_string);
        config.SetMultiValue(true);
        // fixed size
        config.SetFixedMultiValueCount(100);
        ASSERT_THROW(config.CheckFixedLengthMultiValue(), SchemaException);
        // var size
        config.SetFixedMultiValueCount(-1);
        ASSERT_NO_THROW(config.CheckFixedLengthMultiValue());
        // zero size
        config.SetFixedMultiValueCount(0);
        ASSERT_THROW(config.CheckFixedLengthMultiValue(), SchemaException);
    }

    // single value int16
    {
        FieldConfig config;
        config.SetFieldType(ft_int16);
        config.SetMultiValue(false);
        // fixed size
        config.SetFixedMultiValueCount(100);
        ASSERT_THROW(config.CheckFixedLengthMultiValue(), SchemaException);
        // var size
        config.SetFixedMultiValueCount(-1);
        ASSERT_NO_THROW(config.CheckFixedLengthMultiValue());
        // zero size
        config.SetFixedMultiValueCount(0);
        ASSERT_THROW(config.CheckFixedLengthMultiValue(), SchemaException);
    }
    // single value string
    {
        FieldConfig config;
        config.SetFieldType(ft_string);
        config.SetMultiValue(false);
        // fixed size
        config.SetFixedMultiValueCount(100);
        ASSERT_NO_THROW(config.CheckFixedLengthMultiValue());
        // zero size
        config.SetFixedMultiValueCount(0);
        ASSERT_THROW(config.CheckFixedLengthMultiValue(), SchemaException);
        // var size
        config.SetFixedMultiValueCount(-1);
        ASSERT_NO_THROW(config.CheckFixedLengthMultiValue());
    }

    {
        FieldConfig config;
        config.SetFieldType(ft_text);
        config.SetMultiValue(true);
        config.SetFixedMultiValueCount(100);
        ASSERT_THROW(config.CheckFixedLengthMultiValue(), SchemaException);
    }
}

void FieldConfigTest::TestCheck()
{
    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetCompressType("equal");
        ASSERT_NO_THROW(config.Check());
    }

    {
        FieldConfig config;
        config.SetFieldType(ft_integer);
        config.SetCompressType("equal");
        ASSERT_NO_THROW(config.Check());
    }

    {
        FieldConfig config;
        config.SetFieldType(ft_float);
        config.SetCompressType("equal");
        ASSERT_NO_THROW(config.Check());
    }

    {
        FieldConfig config;
        config.SetFieldType(ft_double);
        config.SetCompressType("equal");
        ASSERT_NO_THROW(config.Check());
    }

    {
        FieldConfig config;
        config.SetFieldType(ft_integer);
        config.SetUpdatableMultiValue(true);
        config.SetCompressType("equal");
        ASSERT_NO_THROW(config.Check());
    }

    {
        FieldConfig config;
        config.SetFieldType(ft_text);
        config.SetUpdatableMultiValue(true);
        config.SetCompressType("equal");
        ASSERT_ANY_THROW(config.Check());
    }

    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFixedMultiValueCount(4);
        config.SetCompressType("block_fp");
        config.SetFieldType(ft_float);
        ASSERT_NO_THROW(config.Check());
    }

    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFixedMultiValueCount(4);
        config.SetCompressType("block_fp");
        config.SetFieldType(ft_integer);
        ASSERT_ANY_THROW(config.Check());
    }

    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFixedMultiValueCount(4);
        config.SetCompressType("fp16");
        config.SetFieldType(ft_float);
        ASSERT_NO_THROW(config.Check());
    }

    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFixedMultiValueCount(4);
        config.SetCompressType("fp16");
        config.SetFieldType(ft_integer);
        ASSERT_ANY_THROW(config.Check());
    }

    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFieldType(ft_timestamp);
        ASSERT_ANY_THROW(config.Check());
    }

    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFieldType(ft_time);
        ASSERT_ANY_THROW(config.Check());
    }

    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFieldType(ft_date);
        ASSERT_ANY_THROW(config.Check());
    }
}

void FieldConfigTest::TestSupportSort()
{
    FieldType fieldTypes[] = {ft_integer, ft_uint32, ft_long,  ft_uint64, ft_int8, ft_uint8,    ft_int16,
                              ft_uint16,  ft_double, ft_float, ft_date,   ft_time, ft_timestamp};
    for (size_t i = 0; i < sizeof(fieldTypes) / sizeof(fieldTypes[0]); i++) {
#define CHECK_SUPPORT_SORT(isMultiValue)                                                                               \
    do {                                                                                                               \
        FieldConfig config;                                                                                            \
        config.SetFieldType(fieldTypes[i]);                                                                            \
        config.SetMultiValue(isMultiValue);                                                                            \
        config.SetFieldName("fieldName");                                                                              \
        EXPECT_EQ(!isMultiValue, config.SupportSort());                                                                \
    } while (0)

        CHECK_SUPPORT_SORT(false);
        CHECK_SUPPORT_SORT(true);
    }
    FieldConfig config;
    config.SetFieldType(ft_string);
    config.SetFieldName("fieldName");
    config.SetMultiValue(false);
    EXPECT_FALSE(config.SupportSort());
}

void FieldConfigTest::TestUserDefinedParam()
{
    string fieldConfig = "{\n                                                    \
   \"field_name\":\"int1\",\n                           \
   \"field_type\":\"INTEGER\",\n                        \
   \"multi_value\":true,\n                              \
   \"uniq_encode\":true,\n                              \
   \"user_defined_param\":{\"key\":\"hello\"}\n         \
}";

    FieldConfig config;
    Any any = ParseJson(fieldConfig);
    FromJson(config, any);
    ASSERT_EQ(string("int1"), config.GetFieldName());
    ASSERT_EQ(ft_integer, config.GetFieldType());
    ASSERT_TRUE(config.IsMultiValue());

    ASSERT_EQ("hello", *GetTypeValueFromJsonMap<std::string>(config.GetUserDefinedParam(), "key", ""));

    // test to json
    Any toAny = ToJson(config);
    string jsonString = ToString(toAny);
    Any comparedAny = ParseJson(jsonString);
    FieldConfig comparedConfig;
    FromJson(comparedConfig, toAny);
    ASSERT_NO_THROW(config.AssertEqual(comparedConfig));
}
void FieldConfigTest::TestUserDefinedParamV2()
{
    std::string fieldConfig = R"({
    "field_name":"int1",
    "field_type":"INTEGER",
    "multi_value":true,
    "uniq_encode":true,
    "user_defined_param":{
         "source_field":{
             "field_name" : "attributes",
             "kv_pair_separator" : ";",
             "kv_separator" : ":"
          }
    }
    })";

    FieldConfig config;
    Any any = ParseJson(fieldConfig);
    FromJson(config, any);
    ASSERT_EQ(string("int1"), config.GetFieldName());
    ASSERT_EQ(ft_integer, config.GetFieldType());
    ASSERT_TRUE(config.IsMultiValue());

    const auto& kvMap = config.GetUserDefinedParam();
    auto it = kvMap.find("source_field");
    ASSERT_TRUE(it != kvMap.end());

    auto kvPair = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(it->second);
    auto iter = kvPair.find("field_name");
    ASSERT_TRUE(iter != kvPair.end());
    ASSERT_EQ("attributes", autil::legacy::AnyCast<std::string>(iter->second));

    iter = kvPair.find("kv_pair_separator");
    ASSERT_TRUE(iter != kvPair.end());
    ASSERT_EQ(";", autil::legacy::AnyCast<std::string>(iter->second));

    iter = kvPair.find("kv_separator");
    ASSERT_TRUE(iter != kvPair.end());
    ASSERT_EQ(":", autil::legacy::AnyCast<std::string>(iter->second));

    // test to json
    Any toAny = ToJson(config);
    string jsonString = ToString(toAny);
    Any comparedAny = ParseJson(jsonString);
    FieldConfig comparedConfig;
    FromJson(comparedConfig, toAny);
    ASSERT_NO_THROW(config.AssertEqual(comparedConfig));
}

void FieldConfigTest::TestRewriteFieldType()
{
    FieldConfig config;
    config.SetFieldName("testField");
    // invalid field type
    config.SetFieldType(ft_double);
    config.RewriteFieldType();
    ASSERT_EQ(ft_double, config.GetFieldType());

    config.SetFieldType(ft_int8);
    config.RewriteFieldType();
    ASSERT_EQ(ft_int8, config.GetFieldType());

    // no compresstype
    config.SetFieldType(ft_float);
    config.RewriteFieldType();
    ASSERT_EQ(ft_float, config.GetFieldType());

    // fp16
    config.SetCompressType("fp16");
    FieldConfig config2 = config;
    config.RewriteFieldType();
    ASSERT_NO_THROW(config.AssertEqual(config2));
    ASSERT_EQ(ft_fp16, config.GetFieldType());
    // int8
    config.ClearCompressType();
    config.SetFieldType(ft_float);
    config.SetCompressType("int8#1.0");
    config.RewriteFieldType();
    ASSERT_EQ(ft_fp8, config.GetFieldType());
    // multi value
    config.SetMultiValue(true);
    config.SetFieldType(ft_float);
    config.RewriteFieldType();
    ASSERT_EQ(ft_float, config.GetFieldType());
}

void FieldConfigTest::TestCheckFieldType()
{
    {
        // multi value
        FieldConfig config("testField", ft_fp16, true);
        config.SetCompressType("fp16");
        config.SetMultiValue(true);

        FieldConfig config2("testField", ft_float, true);
        config2.SetMultiValue(true);
        config2.SetCompressType("fp16");

        ASSERT_ANY_THROW(config.CheckFieldTypeEqual(config2));
        ASSERT_ANY_THROW(config2.CheckFieldTypeEqual(config));
    }
    {
        // single value, fp16
        FieldConfig config("testField", ft_fp16, true);
        config.SetCompressType("fp16");
        config.SetMultiValue(false);

        FieldConfig config2("testField", ft_float, true);
        config2.SetCompressType("fp16");
        config2.SetMultiValue(false);

        ASSERT_NO_THROW(config.CheckFieldTypeEqual(config2));
        ASSERT_NO_THROW(config2.CheckFieldTypeEqual(config));

        FieldConfig config3("testField", ft_int16, true);
        config2.SetMultiValue(false);
        ASSERT_ANY_THROW(config.CheckFieldTypeEqual(config3));
        ASSERT_ANY_THROW(config3.CheckFieldTypeEqual(config));
    }
    {
        // single value, int8
        FieldConfig config("testField", ft_fp8, true);
        config.SetCompressType("int8#1");
        config.SetMultiValue(false);

        FieldConfig config2("testField", ft_float, true);
        config2.SetCompressType("int8#1");
        config2.SetMultiValue(false);

        ASSERT_NO_THROW(config.CheckFieldTypeEqual(config2));
        ASSERT_NO_THROW(config2.CheckFieldTypeEqual(config));

        FieldConfig config3("testField", ft_string, true);
        config2.SetMultiValue(false);
        ASSERT_ANY_THROW(config.CheckFieldTypeEqual(config3));
        ASSERT_ANY_THROW(config3.CheckFieldTypeEqual(config));
    }
}

void FieldConfigTest::TestCheckSupportNull()
{
    // raw field
    FieldConfig rawConfig("testField", ft_raw, false);
    rawConfig.SetEnableNullField(true);
    EXPECT_ANY_THROW(rawConfig.Check());

    // single value
    FieldConfig config("testField", ft_float, false);
    config.SetEnableNullField(true);
    config.SetUserDefineAttributeNullValue("0.0");
    EXPECT_NO_THROW(config.Check());

    FieldConfig config2("testField", ft_int8, false);
    config2.SetEnableNullField(true);
    EXPECT_NO_THROW(config2.Check());

    FieldConfig config3("testField", ft_float, false);
    config3.SetEnableNullField(true);
    config3.SetCompressType("fp16");
    EXPECT_NO_THROW(config3.Check());

    FieldConfig config4 = config3;
    config4.SetUserDefineAttributeNullValue("1.2345");
    EXPECT_NO_THROW(config4.Check());

    FieldConfig config5 = config3;
    config5.SetUserDefineAttributeNullValue("abcd");
    EXPECT_ANY_THROW(config5.Check());

    // multiple value
    FieldConfig config6("testField", ft_int16, true);
    config6.SetEnableNullField(true);
    config6.SetMultiValue(true);
    config6.SetFixedMultiValueCount(4);
    EXPECT_ANY_THROW(config6.Check());

    FieldConfig config8("testField", ft_string, true);
    config8.SetEnableNullField(true);
    config8.SetMultiValue(true);
    EXPECT_NO_THROW(config8.Check());

    FieldConfig config9 = config8;
    config9.SetMultiValue(true);
    config9.SetUserDefineAttributeNullValue("120");
    EXPECT_NO_THROW(config9.Check());

    FieldConfig config10("testField", ft_int16, true);
    config10.SetMultiValue(true);
    config10.SetFixedMultiValueCount(4);
    config10.SetEnableNullField(true);
    config10.SetCompressType("equal");
    EXPECT_ANY_THROW(config10.Check());

    FieldConfig config11("testField", ft_int16, true);
    config11.SetMultiValue(true);
    config11.SetEnableNullField(true);
    config11.SetCompressType("equal");
    EXPECT_NO_THROW(config11.Check());
}

void FieldConfigTest::TestSeparator()
{
    FieldConfig fieldConfig("f1", ft_int16, true);
    EXPECT_EQ(INDEXLIB_MULTI_VALUE_SEPARATOR_STR, fieldConfig.GetSeparator());

    fieldConfig.SetSeparator(",");
    EXPECT_EQ(",", fieldConfig.GetSeparator());

    auto str = ToJsonString(fieldConfig);
    FieldConfig fieldConfig2;
    FromJsonString(fieldConfig2, str);
    EXPECT_EQ(",", fieldConfig2.GetSeparator());
    ASSERT_NO_THROW(fieldConfig.AssertEqual(fieldConfig2));

    fieldConfig2.SetFieldType(ft_text);
    EXPECT_ANY_THROW(fieldConfig2.Check());
}
}} // namespace indexlib::config
