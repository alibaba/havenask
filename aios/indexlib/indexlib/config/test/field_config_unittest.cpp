#include "indexlib/config/test/field_config_unittest.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/config/configurator_define.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace std;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, FieldConfigTest);

FieldConfigTest::FieldConfigTest()
{
}

FieldConfigTest::~FieldConfigTest()
{
}

void FieldConfigTest::SetUp()
{
}

void FieldConfigTest::TearDown()
{
}

void FieldConfigTest::TestJsonize()
{
    string fieldConfig =
"{\n                                        \
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

    const FieldConfigImplPtr& impl = config.GetImpl();
    ASSERT_EQ(string("int1"), impl->mFieldName);
    ASSERT_EQ(ft_integer, impl->mFieldType);
    ASSERT_EQ(true, impl->mMultiValue);
    ASSERT_EQ((uint64_t)10, impl->mU32OffsetThreshold);
    ASSERT_EQ(string("100"), impl->mDefaultStrValue);
    ASSERT_EQ((float)0.6, impl->GetDefragSlicePercent());
    ASSERT_EQ((int)10, impl->GetFixedMultiValueCount());    
    
    CompressTypeOption type;
    type.Init(string("uniq | equal"));
    ASSERT_NO_THROW(impl->mCompressType.AssertEqual(type));

    //test to json
    Any toAny = ToJson(config);
    string jsonString = ToString(toAny);
    Any comparedAny = ParseJson(jsonString);
    FieldConfig comparedConfig;
    FromJson(comparedConfig, toAny);
    ASSERT_NO_THROW(config.AssertEqual(comparedConfig));
}

void FieldConfigTest::TestJsonizeU32OffsetThreshold()
{
    string fieldConfig =
"{\n                                       \
   \"field_name\":\"int1\",\n              \
   \"field_type\":\"INTEGER\",\n           \
   \"u32offset_threshold\":10\n            \
}";
    {
        FieldConfig config;
        Any any = ParseJson(fieldConfig);
        FromJson(config, any);
        const FieldConfigImplPtr& impl = config.GetImpl();
        ASSERT_EQ(impl->mU32OffsetThreshold, (uint64_t)10);

        //test to json
        Any toAny = ToJson(config);
        string jsonString = ToString(toAny);
        Any comparedAny = ParseJson(jsonString);
        FieldConfig comparedConfig;
        FromJson(comparedConfig, toAny);
        ASSERT_NO_THROW(config.AssertEqual(comparedConfig));
    }

    fieldConfig =
"{\n                                       \
   \"field_name\":\"int1\",\n              \
   \"field_type\":\"INTEGER\"\n            \
}";
    {
        FieldConfig config;
        Any any = ParseJson(fieldConfig);
        FromJson(config, any);
        const FieldConfigImplPtr& impl = config.GetImpl();
        ASSERT_EQ(impl->mU32OffsetThreshold, (uint64_t)FIELD_U32OFFSET_THRESHOLD_MAX);

        //test to json
        Any toAny = ToJson(config);
        string jsonString = ToString(toAny);
        Any comparedAny = ParseJson(jsonString);
        FieldConfig comparedConfig;
        FromJson(comparedConfig, toAny);
        ASSERT_NO_THROW(config.AssertEqual(comparedConfig));
    }

    fieldConfig =
"{\n                                       \
   \"field_name\":\"int1\",\n              \
   \"field_type\":\"INTEGER\",\n           \
   \"u32offset_threshold\":9999999999\n    \
}";
    {
        FieldConfig config;
        Any any = ParseJson(fieldConfig);
        FromJson(config, any);
        const FieldConfigImplPtr& impl = config.GetImpl();
        ASSERT_EQ(impl->mU32OffsetThreshold, (uint64_t)FIELD_U32OFFSET_THRESHOLD_MAX);

        //test to json
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
    //only test new function for assert equal
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

void FieldConfigTest::TestIsAttributeUpdatable()
{
    {
        FieldConfig config;
        config.SetMultiValue(true);
        ASSERT_FALSE(config.IsAttributeUpdatable());
    }

    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetUpdatableMultiValue(true);
        config.SetFieldType(ft_integer);
        ASSERT_TRUE(config.IsAttributeUpdatable());
    }

    {
        FieldConfig config;
        config.SetMultiValue(false);
        config.SetCompressType("equal");
        ASSERT_FALSE(config.IsAttributeUpdatable());
    }

    {
        FieldConfig config;
        config.SetMultiValue(false);
        config.SetFieldType(ft_integer);
        ASSERT_TRUE(config.IsAttributeUpdatable());
    }

    {
        FieldConfig config;
        config.SetMultiValue(false);
        config.SetFieldType(ft_string);

        ASSERT_FALSE(config.IsAttributeUpdatable());
        config.SetUpdatableMultiValue(true);
        ASSERT_TRUE(config.IsAttributeUpdatable());
    }
}

void FieldConfigTest::TestCheckUniqEncode()
{
    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetCompressType("uniq");
        ASSERT_NO_THROW(config.GetImpl()->CheckUniqEncode());
    }

    {
        FieldConfig config;
        config.SetFieldType(ft_string);
        config.SetCompressType("uniq");
        ASSERT_NO_THROW(config.GetImpl()->CheckUniqEncode());
    }

    {
        FieldConfig config;
        config.SetFieldType(ft_integer);
        config.SetCompressType("uniq");
        ASSERT_ANY_THROW(config.GetImpl()->CheckUniqEncode());
    }
}

void FieldConfigTest::TestCheckBlockFpEncode()
{
    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetCompressType("uniq");
        ASSERT_NO_THROW(config.GetImpl()->CheckBlockFpEncode());
    }

    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFixedMultiValueCount(4);
        config.SetCompressType("block_fp");
        config.SetFieldType(ft_float);
        ASSERT_NO_THROW(config.GetImpl()->CheckBlockFpEncode());
    }

    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFixedMultiValueCount(4);
        config.SetCompressType("block_fp");
        config.SetFieldType(ft_integer);
        ASSERT_ANY_THROW(config.GetImpl()->CheckBlockFpEncode());

        config.SetFieldType(ft_float);
        config.SetFixedMultiValueCount(-1);
        ASSERT_ANY_THROW(config.GetImpl()->CheckBlockFpEncode());

        config.SetMultiValue(false);
        config.SetFixedMultiValueCount(4);
        ASSERT_ANY_THROW(config.GetImpl()->CheckBlockFpEncode());
    }
}

void FieldConfigTest::TestCheckFp16Encode()
{
    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetCompressType("uniq");
        ASSERT_NO_THROW(config.GetImpl()->CheckFp16Encode());
    }

    {
        // single float, can't use fp16 & equal
        FieldConfig config;
        config.SetMultiValue(false);
        config.SetCompressType("fp16|equal");
        config.SetFieldType(ft_float);
        ASSERT_ANY_THROW(config.GetImpl()->CheckFp16Encode());
        // multi float
        FieldConfig config2;
        config2.SetMultiValue(true);
        config2.SetCompressType("fp16|equal");
        config2.SetFieldType(ft_float);
        ASSERT_NO_THROW(config2.GetImpl()->CheckFp16Encode());
    }

    {
        FieldConfig config;
        config.SetMultiValue(false);
        config.SetCompressType("fp16");
        config.SetFieldType(ft_float);
        ASSERT_NO_THROW(config.GetImpl()->CheckFp16Encode());
    }
    
    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFixedMultiValueCount(4);
        config.SetCompressType("fp16");
        config.SetFieldType(ft_float);
        ASSERT_NO_THROW(config.GetImpl()->CheckFp16Encode());
    }

    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFixedMultiValueCount(4);
        config.SetCompressType("fp16");
        config.SetFieldType(ft_integer);
        ASSERT_ANY_THROW(config.GetImpl()->CheckFp16Encode());

        config.SetFieldType(ft_float);
        config.SetFixedMultiValueCount(-1);
        ASSERT_NO_THROW(config.GetImpl()->CheckFp16Encode());

        config.SetMultiValue(false);
        config.SetFixedMultiValueCount(4);
        ASSERT_NO_THROW(config.GetImpl()->CheckFp16Encode());
    }
}

void FieldConfigTest::TestCheckFloatInt8Encode()
{
    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetCompressType("uniq");
        ASSERT_NO_THROW(config.GetImpl()->CheckFloatInt8Encode());
    }
    
    {
        //single float, can't use int8compress & equal 
        FieldConfig config;
        config.SetMultiValue(false);
        config.SetCompressType("int8#0.3|equal");
        config.SetFieldType(ft_float);
        ASSERT_ANY_THROW(config.GetImpl()->CheckFloatInt8Encode());
        //multi float
        FieldConfig config2;
        config2.SetMultiValue(true);
        config2.SetCompressType("int8#0.3|equal");
        config2.SetFieldType(ft_float);
        ASSERT_NO_THROW(config2.GetImpl()->CheckFloatInt8Encode());
    }

    {
        FieldConfig config;
        config.SetMultiValue(false);
        config.SetCompressType("int8#0.3");
        config.SetFieldType(ft_float);
        ASSERT_NO_THROW(config.GetImpl()->CheckFloatInt8Encode());
    }
    
    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFixedMultiValueCount(4);
        config.SetCompressType("int8#0.3");
        config.SetFieldType(ft_float);
        ASSERT_NO_THROW(config.GetImpl()->CheckFloatInt8Encode());
    }

    {
        FieldConfig config;
        config.SetMultiValue(true);
        config.SetFixedMultiValueCount(4);
        config.SetCompressType("int8#0.3");
        config.SetFieldType(ft_integer);
        ASSERT_ANY_THROW(config.GetImpl()->CheckFloatInt8Encode());

        config.SetFieldType(ft_float);
        config.SetFixedMultiValueCount(-1);
        ASSERT_NO_THROW(config.GetImpl()->CheckFloatInt8Encode());

        config.SetMultiValue(false);
        config.SetFixedMultiValueCount(4);
        ASSERT_NO_THROW(config.GetImpl()->CheckFloatInt8Encode());
    }
}

void FieldConfigTest::TestCheckDefaultAttributeValue()
{
    {
        FieldConfig config;
        config.SetFieldType(ft_int32);
        config.SetMultiValue(true);
        config.SetDefaultValue("100");
        ASSERT_ANY_THROW(config.GetImpl()->CheckDefaultAttributeValue());
    }

    {
        FieldConfig config;
        config.SetFieldType(ft_string);
        config.SetMultiValue(false);
        config.SetDefaultValue("100");
        ASSERT_NO_THROW(config.GetImpl()->CheckDefaultAttributeValue());
    }

    {
        FieldConfig config;
        config.SetFieldType(ft_text);
        config.SetMultiValue(false);
        config.SetDefaultValue("100");
        ASSERT_ANY_THROW(config.GetImpl()->CheckDefaultAttributeValue());
    }

    {
        FieldConfig config;
        config.SetFieldType(ft_int32);
        config.SetMultiValue(false);
        config.SetDefaultValue("100");
        ASSERT_NO_THROW(config.GetImpl()->CheckDefaultAttributeValue());
    }

    {
        FieldConfig config;
        config.SetFieldType(ft_float);
        config.SetMultiValue(false);
        config.SetDefaultValue("100");
        ASSERT_NO_THROW(config.GetImpl()->CheckDefaultAttributeValue());
    }

    {
        FieldConfig config;
        config.SetFieldType(ft_double);
        config.SetMultiValue(false);
        config.SetDefaultValue("100");
        ASSERT_NO_THROW(config.GetImpl()->CheckDefaultAttributeValue());
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
        ASSERT_NO_THROW(config.GetImpl()->CheckFixedLengthMultiValue());
        // var size
        config.SetFixedMultiValueCount(-1);
        ASSERT_NO_THROW(config.GetImpl()->CheckFixedLengthMultiValue());
        // zero size
        config.SetFixedMultiValueCount(0);
        ASSERT_THROW(config.GetImpl()->CheckFixedLengthMultiValue(), SchemaException);
    }
    // multi value string
    {
        FieldConfig config;
        config.SetFieldType(ft_string);
        config.SetMultiValue(true);
        // fixed size
        config.SetFixedMultiValueCount(100);
        ASSERT_THROW(config.GetImpl()->CheckFixedLengthMultiValue(), SchemaException);
        // var size
        config.SetFixedMultiValueCount(-1);
        ASSERT_NO_THROW(config.GetImpl()->CheckFixedLengthMultiValue());
        // zero size
        config.SetFixedMultiValueCount(0);
        ASSERT_THROW(config.GetImpl()->CheckFixedLengthMultiValue(), SchemaException);
    }

    // single value int16
    {
        FieldConfig config;
        config.SetFieldType(ft_int16);
        config.SetMultiValue(false);
        // fixed size
        config.SetFixedMultiValueCount(100);
        ASSERT_THROW(config.GetImpl()->CheckFixedLengthMultiValue(), SchemaException);
        // var size
        config.SetFixedMultiValueCount(-1);
        ASSERT_NO_THROW(config.GetImpl()->CheckFixedLengthMultiValue());
        // zero size
        config.SetFixedMultiValueCount(0);
        ASSERT_THROW(config.GetImpl()->CheckFixedLengthMultiValue(), SchemaException);
    }
    // single value string
    {
        FieldConfig config;
        config.SetFieldType(ft_string);
        config.SetMultiValue(false);
        // fixed size
        config.SetFixedMultiValueCount(100);
        ASSERT_NO_THROW(config.GetImpl()->CheckFixedLengthMultiValue());
        // zero size
        config.SetFixedMultiValueCount(0);
        ASSERT_THROW(config.GetImpl()->CheckFixedLengthMultiValue(), SchemaException);
        // var size
        config.SetFixedMultiValueCount(-1);
        ASSERT_NO_THROW(config.GetImpl()->CheckFixedLengthMultiValue());
    }
    
    
    {
        FieldConfig config;
        config.SetFieldType(ft_text);
        config.SetMultiValue(true);
        config.SetFixedMultiValueCount(100);
        ASSERT_THROW(config.GetImpl()->CheckFixedLengthMultiValue(), SchemaException);
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
}

void FieldConfigTest::TestSupportSort()
{
    FieldType fieldTypes[] = {
        ft_integer, ft_uint32, ft_long, ft_uint64, ft_int8, 
        ft_uint8, ft_int16, ft_uint16, ft_double, ft_float
    };
    for (size_t i = 0; i < sizeof(fieldTypes)/sizeof(fieldTypes[0]); i++)
    {
#define CHECK_SUPPORT_SORT(isMultiValue) do {   \
            FieldConfig config;                 \
            config.SetFieldType(fieldTypes[i]); \
            config.SetMultiValue(isMultiValue);   \
            config.SetFieldName("fieldName");                   \
            EXPECT_EQ(!isMultiValue, config.SupportSort());     \
        } while(0)
        
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
    string fieldConfig =
"{\n                                                    \
   \"field_name\":\"int1\",\n                           \
   \"field_type\":\"INTEGER\",\n                        \
   \"multi_value\":true,\n                              \
   \"uniq_encode\":true,\n                              \
   \"user_defined_param\":{\"key\":\"hello\"}\n         \
}";

    FieldConfig config;
    Any any = ParseJson(fieldConfig);
    FromJson(config, any);
    const FieldConfigImplPtr& impl = config.GetImpl();
    ASSERT_EQ(string("int1"), impl->mFieldName);
    ASSERT_EQ(ft_integer, impl->mFieldType);
    ASSERT_EQ(true, impl->mMultiValue);

    const KeyValueMap& kvMap = config.GetUserDefinedParam();
    ASSERT_EQ("hello", GetValueFromKeyValueMap(kvMap, "key"));
    
    //test to json
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
    //invalid field type
    config.SetFieldType(ft_double);
    config.RewriteFieldType();
    ASSERT_EQ(ft_double, config.GetFieldType());
    
    config.SetFieldType(ft_int8);
    config.RewriteFieldType();
    ASSERT_EQ(ft_int8, config.GetFieldType());

    //no compresstype
    config.SetFieldType(ft_float);
    config.RewriteFieldType();
    ASSERT_EQ(ft_float, config.GetFieldType());

    //fp16
    config.SetCompressType("fp16");
    config.RewriteFieldType();
    ASSERT_EQ(ft_fp16, config.GetFieldType());
    //int8
    config.ClearCompressType();
    config.SetFieldType(ft_float);
    config.SetCompressType("int8#1.0");
    config.RewriteFieldType();
    ASSERT_EQ(ft_fp8, config.GetFieldType());
    //multi value
    config.SetMultiValue(true);
    config.SetFieldType(ft_float);
    config.RewriteFieldType();
    ASSERT_EQ(ft_float, config.GetFieldType());
}

void FieldConfigTest::TestCheckFieldType()
{
    {
        // multi value
        FieldConfigImpl config("testField", ft_fp16, true);
        config.SetCompressType("fp16");
        config.SetMultiValue(true);
        
        FieldConfigImpl config2("testField", ft_float, true);
        config2.SetMultiValue(true);
        config2.SetCompressType("fp16");

        ASSERT_ANY_THROW(config.CheckFieldTypeEqual(config2));
        ASSERT_ANY_THROW(config2.CheckFieldTypeEqual(config));
    }
    {
        // single value, fp16
        FieldConfigImpl config("testField", ft_fp16, true);
        config.SetCompressType("fp16");
        config.SetMultiValue(false);
        
        FieldConfigImpl config2("testField", ft_float, true);
        config2.SetCompressType("fp16");
        config2.SetMultiValue(false);

        ASSERT_NO_THROW(config.CheckFieldTypeEqual(config2));
        ASSERT_NO_THROW(config2.CheckFieldTypeEqual(config));

        FieldConfigImpl config3("testField", ft_int16, true);
        config2.SetMultiValue(false);
        ASSERT_ANY_THROW(config.CheckFieldTypeEqual(config3));
        ASSERT_ANY_THROW(config3.CheckFieldTypeEqual(config));
    }
    {
        // single value, int8
        FieldConfigImpl config("testField", ft_fp8, true);
        config.SetCompressType("int8#1");
        config.SetMultiValue(false);
        
        FieldConfigImpl config2("testField", ft_float, true);
        config2.SetCompressType("int8#1");
        config2.SetMultiValue(false);

        ASSERT_NO_THROW(config.CheckFieldTypeEqual(config2));
        ASSERT_NO_THROW(config2.CheckFieldTypeEqual(config));

        FieldConfigImpl config3("testField", ft_string, true);
        config2.SetMultiValue(false);
        ASSERT_ANY_THROW(config.CheckFieldTypeEqual(config3));
        ASSERT_ANY_THROW(config3.CheckFieldTypeEqual(config));
    }
}

IE_NAMESPACE_END(config);

