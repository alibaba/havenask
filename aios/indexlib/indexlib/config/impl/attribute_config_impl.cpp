#include "indexlib/config/attribute_config.h"
#include "indexlib/config/impl/attribute_config_impl.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/config_define.h"
#include "indexlib/config/pack_attribute_config.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, AttributeConfigImpl);

AttributeConfigImpl::AttributeConfigImpl(AttributeConfig::ConfigType type)
    : mAttrId(0)
    , mConfigType(type)
    , mPackAttrConfig(NULL)
    , mStatus(is_normal)
    , mOwnerOpId(INVALID_SCHEMA_OP_ID)
{
}

void AttributeConfigImpl::SetFieldConfig(const FieldConfigPtr& fieldConfig) 
{
    assert(fieldConfig.get() != NULL);
    FieldType fieldType = fieldConfig->GetFieldType();
    bool isMultiValue = fieldConfig->IsMultiValue();

    if (fieldType == ft_integer || fieldType == ft_float
        || fieldType == ft_long
        || fieldType == ft_fp8 || fieldType == ft_fp16
        || fieldType == ft_uint32 || fieldType == ft_uint64
        || fieldType == ft_int8 || fieldType == ft_uint8
        || fieldType == ft_int16 || fieldType == ft_uint16
        || fieldType == ft_double
        || (fieldType == ft_hash_64 && !isMultiValue)
        || (fieldType == ft_hash_128 && !isMultiValue)
        || fieldType == ft_string || fieldType == ft_location
        || fieldType == ft_line || fieldType == ft_polygon
        || fieldType == ft_raw)
    {
        mFieldConfig = fieldConfig;
        return;
    }

    INDEXLIB_FATAL_ERROR(Schema,
                   "Unsupport Attribute: fieldName = %s, fieldType = %s, isMultiValue = %s",
                   fieldConfig->GetFieldName().c_str(),
                   FieldConfig::FieldTypeToStr(fieldType),
                   (isMultiValue ? "true" : "false"));
}

void AttributeConfigImpl::SetAttrValueInitializerCreator(
        const AttributeValueInitializerCreatorPtr& creator)
{
    mValueInitializerCreator = creator;
}

void AttributeConfigImpl::AssertEqual(const AttributeConfigImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mAttrId, other.mAttrId, "mAttrId not equal");
    IE_CONFIG_ASSERT_EQUAL(mConfigType, other.mConfigType, "mConfigType not equal");
    IE_CONFIG_ASSERT_EQUAL(mOwnerOpId, other.mOwnerOpId, "mOwnerOpId not equal");
    
    if (mFieldConfig.get() != NULL && other.mFieldConfig.get() != NULL)
    {
        mFieldConfig->AssertEqual(*(other.mFieldConfig));
    }
    else if (mFieldConfig.get() != NULL || other.mFieldConfig.get() != NULL)
    {
        INDEXLIB_FATAL_ERROR(AssertEqual, "mFieldConfig is not equal");
    }
    
    if (mPackAttrConfig && other.mPackAttrConfig)
    {
        mPackAttrConfig->AssertEqual(*other.mPackAttrConfig);
    }
    else if (mPackAttrConfig || other.mPackAttrConfig)
    {
        INDEXLIB_FATAL_ERROR(AssertEqual, "mPackAttrConfig is not equal");
    }

    for (size_t i = 0; i < mCustomizedConfigs.size(); i++)
    {
        mCustomizedConfigs[i]->AssertEqual(*other.mCustomizedConfigs[i]);
    }
}

bool AttributeConfigImpl::IsLoadPatchExpand()
{
    if (IsMultiValue() && IsAttributeUpdatable())
    {
        return true;
    }
    if (GetFieldType() == ft_string && IsAttributeUpdatable())
    {
        return true;
    }
    if (mPackAttrConfig)
    {
        return true;
    }
    return false;
}

size_t AttributeConfigImpl::EstimateAttributeExpandSize(size_t docCount)
{
    FieldType fieldType = GetFieldType();
    if (IsMultiValue() || fieldType == ft_string || fieldType == ft_raw)
    {
        INDEXLIB_FATAL_ERROR(UnSupported,
                       "can not estimate the size of multivalue attribute or raw attribute");
    }
    return SizeOfFieldType(fieldType) * docCount;
}

void AttributeConfigImpl::Delete()
{
    if (GetPackAttributeConfig() != NULL)
    {
        INDEXLIB_FATAL_ERROR(UnSupported,
                             "delete attribute [%s] fail, which is in pack attribute",
                             GetAttrName().c_str());
    }
    mStatus = is_deleted; 
}

IE_NAMESPACE_END(config);

