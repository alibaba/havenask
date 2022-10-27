#include "indexlib/config/enum_field_config.h"
#include "indexlib/config/impl/enum_field_config_impl.h"

IE_NAMESPACE_BEGIN(config);

IE_LOG_SETUP(config, EnumFieldConfig);
using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

EnumFieldConfig::EnumFieldConfig()
    : mImpl(new EnumFieldConfigImpl)
{
    mImpl->SetFieldType(ft_enum);
    //reduce mem use
    ResetImpl(mImpl);
}

EnumFieldConfig::EnumFieldConfig(const string& fieldName, bool multiValue)
    : mImpl(new EnumFieldConfigImpl(fieldName, multiValue))
{
    ResetImpl(mImpl);
}

void EnumFieldConfig::AddValidValue(const string& validValue)
{
    mImpl->AddValidValue(validValue);
}

void EnumFieldConfig::AddValidValue(const ValueVector& validValues)
{
    mImpl->AddValidValue(validValues);
}    

const EnumFieldConfig::ValueVector& EnumFieldConfig::GetValidValues() const 
{
    return mImpl->GetValidValues();
}

void EnumFieldConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

void EnumFieldConfig::AssertEqual(const FieldConfig& other) const
{
    const EnumFieldConfig& other2 = (const EnumFieldConfig&)other;
    mImpl->AssertEqual(*(other2.mImpl));
}

IE_NAMESPACE_END(config);

