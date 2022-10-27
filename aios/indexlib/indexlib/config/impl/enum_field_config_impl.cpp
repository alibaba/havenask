#include "indexlib/config/impl/enum_field_config_impl.h"
#include "autil/legacy/any.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/json.h"
#include "indexlib/config/configurator_define.h"

IE_NAMESPACE_BEGIN(config);

IE_LOG_SETUP(config, EnumFieldConfigImpl);
using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

void EnumFieldConfigImpl::AssertEqual(const EnumFieldConfigImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mFieldId, other.GetFieldId(), "FieldId not equal");
    IE_CONFIG_ASSERT_EQUAL(mMultiValue, other.IsMultiValue(), "MultiValue not equal");
    IE_CONFIG_ASSERT_EQUAL(mFieldName, other.GetFieldName(), "FieldName not equal");
    IE_CONFIG_ASSERT_EQUAL(mFieldType, other.GetFieldType(), "FieldType not equal");
    //const EnumFieldConfigImpl& other2 = (const EnumFieldConfigImpl&)other;
    IE_CONFIG_ASSERT_EQUAL(mValidValues, other.mValidValues, "ValieValues not equal");
}

void EnumFieldConfigImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) 
{
    if (json.GetMode() == TO_JSON) 
    {
        if (mMultiValue)
        {
            json.Jsonize(FIELD_MULTI_VALUE, mMultiValue);
        }
        json.Jsonize(FIELD_NAME, mFieldName);
        if (mFieldType == ft_enum && !mValidValues.empty())
        {
            json.Jsonize(FIELD_VALID_VALUES, mValidValues);
        }
        const char* types[] = {
            "TEXT", "STRING", "ENUM", "INTEGER", 
            "FLOAT", "LONG", "TIME", "LOCATION", 
            "CHAR", "ONLINE", "PROPERTY", "UNKNOWN"};
        assert((size_t)mFieldType < sizeof(types) / sizeof(char*));
        string typeStr = types[mFieldType];
        json.Jsonize(string(FIELD_TYPE), typeStr);
    }
}

IE_NAMESPACE_END(config);

