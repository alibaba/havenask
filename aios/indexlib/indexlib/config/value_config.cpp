#include "indexlib/config/value_config.h"
#include "indexlib/config/impl/value_config_impl.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, ValueConfig);

ValueConfig::ValueConfig()
    : mImpl(new ValueConfigImpl)
{}

ValueConfig::ValueConfig(const ValueConfig& other)
    : mImpl(new ValueConfigImpl(*other.mImpl))
{}

ValueConfig::~ValueConfig() {}

void ValueConfig::Init(const vector<AttributeConfigPtr>& attrConfigs)
{
    mImpl->Init(attrConfigs);
}
size_t ValueConfig::GetAttributeCount() const
{
    return mImpl->GetAttributeCount();
}
const AttributeConfigPtr& ValueConfig::GetAttributeConfig(size_t idx) const
{
    return mImpl->GetAttributeConfig(idx);
}

config::PackAttributeConfigPtr ValueConfig::CreatePackAttributeConfig() const
{
    return mImpl->CreatePackAttributeConfig();
}

void ValueConfig::EnableCompactFormat(bool toSetCompactFormat)
{
    mImpl->EnableCompactFormat(toSetCompactFormat);
}

bool ValueConfig::IsCompactFormatEnabled() const
{
    return mImpl->IsCompactFormatEnabled();
}

int32_t ValueConfig::GetFixedLength() const
{
    return mImpl->GetFixedLength();
}

FieldType ValueConfig::GetFixedLengthValueType() const 
{
    return mImpl->GetFixedLengthValueType();
}

FieldType ValueConfig::GetActualFieldType() const 
{
    return mImpl->GetActualFieldType();
}

IE_NAMESPACE_END(config);
