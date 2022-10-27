#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/config/impl/pack_attribute_config_impl.h"
#include "indexlib/config/field_config.h"
#include "indexlib/util/block_fp_encoder.h"
#include "indexlib/util/fp16_encoder.h"
#include "indexlib/util/float_int8_encoder.h"
#include "indexlib/config/compress_type_option.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, PackAttributeConfig);

PackAttributeConfig::PackAttributeConfig(const string& attrName,
                                         const CompressTypeOption& compressType,
                                         uint64_t defragSlicePercent)
    : mImpl(new PackAttributeConfigImpl(attrName, compressType, defragSlicePercent))
{
    assert(attrName.size() > 0);
}

PackAttributeConfig::~PackAttributeConfig() {}

void PackAttributeConfig::Jsonize(JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

const string& PackAttributeConfig::GetAttrName() const
{
    return mImpl->GetAttrName();
}

CompressTypeOption PackAttributeConfig::GetCompressType() const
{
    return mImpl->GetCompressType();
}

packattrid_t PackAttributeConfig::GetPackAttrId() const
{
    return mImpl->GetPackAttrId();
}
void PackAttributeConfig::SetPackAttrId(packattrid_t packId)
{
    mImpl->SetPackAttrId(packId);
}

void PackAttributeConfig::AddAttributeConfig(
        const AttributeConfigPtr& attributeConfig)
{
    mImpl->AddAttributeConfig(attributeConfig);
    attributeConfig->SetPackAttributeConfig(this);
}

const vector<AttributeConfigPtr>& 
PackAttributeConfig::GetAttributeConfigVec() const
{
    return mImpl->GetAttributeConfigVec();
}

void PackAttributeConfig::GetSubAttributeNames(
        vector<string>& fieldNames) const
{
    mImpl->GetSubAttributeNames(fieldNames);
}

void PackAttributeConfig::AssertEqual(const PackAttributeConfig& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

AttributeConfigPtr PackAttributeConfig::CreateAttributeConfig() const
{
    return mImpl->CreateAttributeConfig();
}

bool PackAttributeConfig::IsUpdatable() const
{
    return mImpl->IsUpdatable();
}

uint32_t PackAttributeConfig::GetFixLenFieldSize(const config::FieldConfigPtr& fieldConfig)
{
    if (fieldConfig->GetFieldType() == ft_string)
    {
        assert(!fieldConfig->IsMultiValue());
        return fieldConfig->GetFixedMultiValueCount() * sizeof(char);
    }

    if (!fieldConfig->IsMultiValue())
    {
        //single value float
        config::CompressTypeOption compressType = fieldConfig->GetCompressType();
        if (fieldConfig->GetFieldType() == FieldType::ft_float) {
            return CompressTypeOption::GetSingleValueCompressLen(compressType);
        }
        return config::SizeOfFieldType(fieldConfig->GetFieldType());
    }
    //multi value float
    if (fieldConfig->GetFieldType() == FieldType::ft_float) {
        config::CompressTypeOption compressType = fieldConfig->GetCompressType();
        return CompressTypeOption::GetMultiValueCompressLen(
            compressType, fieldConfig->GetFixedMultiValueCount());
    }
    return config::SizeOfFieldType(fieldConfig->GetFieldType()) * fieldConfig->GetFixedMultiValueCount();
}

void PackAttributeConfig::Disable()
{
    mImpl->Disable();
}

bool PackAttributeConfig::IsDisable() const
{
    return mImpl->IsDisable();
}

IE_NAMESPACE_END(config);

