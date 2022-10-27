#include "indexlib/config/attribute_config.h"
#include "indexlib/config/impl/attribute_config_impl.h"

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, AttributeConfig);

IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(common);

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

AttributeConfig::AttributeConfig(ConfigType type)
{
    mImpl.reset(new AttributeConfigImpl(type));
}

void AttributeConfig::Init(const FieldConfigPtr& fieldConfig, 
                           const AttributeValueInitializerCreatorPtr& creator,
                           const CustomizedConfigVector& customizedConfigs)
{
    mImpl->Init(fieldConfig, creator, customizedConfigs);
}

AttributeConfig::ConfigType AttributeConfig::GetConfigType() const
{
    return mImpl->GetConfigType();
}
attrid_t AttributeConfig::GetAttrId() const
{
    return mImpl->GetAttrId();
}
void AttributeConfig::SetAttrId(attrid_t id)
{
    mImpl->SetAttrId(id);
}

const string& AttributeConfig::GetAttrName() const 
{
    return mImpl->GetAttrName();
}
fieldid_t AttributeConfig::GetFieldId() const
{
    return mImpl->GetFieldId();
}    
size_t AttributeConfig::EstimateAttributeExpandSize(size_t docCount)
{
    return mImpl->EstimateAttributeExpandSize(docCount);
}
    
FieldType AttributeConfig::GetFieldType() const 
{
    return mImpl->GetFieldType();
}
bool AttributeConfig::IsMultiValue() const
{
    return mImpl->IsMultiValue();
}
bool AttributeConfig::IsLengthFixed() const
{
    return mImpl->IsLengthFixed();
}
void AttributeConfig::SetUniqEncode(bool encode)
{
    mImpl->SetUniqEncode(encode);
}
bool AttributeConfig::IsUniqEncode() const
{
    return mImpl->IsUniqEncode();
}
bool AttributeConfig::IsMultiString() const 
{
    return mImpl->IsMultiString();
}

CompressTypeOption AttributeConfig::GetCompressType() const
{
    return mImpl->GetCompressType();
}

bool AttributeConfig::IsAttributeUpdatable() const
{
    return mImpl->IsAttributeUpdatable();
}
    
float AttributeConfig::GetDefragSlicePercent() 
{
    return mImpl->GetDefragSlicePercent();
}

void AttributeConfig::SetUpdatableMultiValue(bool IsUpdatableMultiValue)
{
    mImpl->SetUpdatableMultiValue(IsUpdatableMultiValue);
}

const FieldConfigPtr& AttributeConfig::GetFieldConfig() const
{
    return mImpl->GetFieldConfig();
}

common::AttributeValueInitializerCreatorPtr AttributeConfig::GetAttrValueInitializerCreator() const
{
    return mImpl->GetAttrValueInitializerCreator();
}

void AttributeConfig::AssertEqual(const AttributeConfig& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

void AttributeConfig::SetPackAttributeConfig(PackAttributeConfig* packAttrConfig)
{
    mImpl->SetPackAttributeConfig(packAttrConfig);
}

PackAttributeConfig* AttributeConfig::GetPackAttributeConfig() const
{
    return mImpl->GetPackAttributeConfig();
}

bool AttributeConfig::IsLoadPatchExpand()
{
    return mImpl->IsLoadPatchExpand();
}

void AttributeConfig::SetCustomizedConfig(const CustomizedConfigVector& customizedConfigs)
{
    mImpl->SetCustomizedConfig(customizedConfigs);
}

const CustomizedConfigVector& AttributeConfig::GetCustomizedConfigs() const
{
    return mImpl->GetCustomizedConfigs();
}

void AttributeConfig::Disable()
{
    assert(ct_normal == GetConfigType() ||
           ct_index_accompany == GetConfigType());
    mImpl->Disable();
}

bool AttributeConfig::IsDisable() const
{
    return mImpl->IsDisable();
}

void AttributeConfig::Delete()
{
    mImpl->Delete();
}

bool AttributeConfig::IsDeleted() const
{
    return mImpl->IsDeleted();
}

void AttributeConfig::SetConfigType(ConfigType type)
{
    mImpl->SetConfigType(type);
}

bool AttributeConfig::IsNormal() const
{
    return mImpl->IsNormal();
}

IndexStatus AttributeConfig::GetStatus() const
{
    return mImpl->GetStatus();
}

void AttributeConfig::SetOwnerModifyOperationId(schema_opid_t opId)
{
    mImpl->SetOwnerModifyOperationId(opId);
}

schema_opid_t AttributeConfig::GetOwnerModifyOperationId() const
{
    return mImpl->GetOwnerModifyOperationId();
}

IE_NAMESPACE_END(config);

