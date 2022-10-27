#include "indexlib/config/impl/value_config_impl.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, ValueConfigImpl);

void ValueConfigImpl::Init(const vector<AttributeConfigPtr>& attrConfigs)
{
    if (attrConfigs.size() == 0)
    {
        INDEXLIB_FATAL_ERROR(Schema, "cannot create value config "
                             "when no attribute config is defined");
    }
    mAttrConfigs = attrConfigs;
    if (attrConfigs.size() == 1)
    {
        if (attrConfigs[0]->GetFieldType() == FieldType::ft_float)
        {
            const FieldConfigPtr& fieldConfig = attrConfigs[0]->GetFieldConfig();
            assert(fieldConfig);
            CompressTypeOption compressType = fieldConfig->GetCompressType();
            // convert field_type for reader, not for builder/merger
            if (compressType.HasFp16EncodeCompress()) {
                mActualFieldType = ft_int16;
            } else if (compressType.HasInt8EncodeCompress()) {
                mActualFieldType = ft_int8;
            } else {
                mActualFieldType = ft_float;
            }
        }
        else
        {
            mActualFieldType = mAttrConfigs[0]->GetFieldType();
        }
    }
    else
    {
        // all field is packed
        mActualFieldType = ft_string;
    }
    mFixedValueLen = 0;
    for (const auto& attrConfig : attrConfigs)
    {
        if (!attrConfig->IsLengthFixed())
        {
            mFixedValueLen = -1;
            break;
        }
        mFixedValueLen += PackAttributeConfig::GetFixLenFieldSize(attrConfig->GetFieldConfig());
    }
}

config::PackAttributeConfigPtr ValueConfigImpl::CreatePackAttributeConfig() const
{
    CompressTypeOption compressOption;
    compressOption.Init("");
    PackAttributeConfigPtr packAttrConfig(new PackAttributeConfig("pack_values",
                    compressOption, FIELD_DEFAULT_DEFRAG_SLICE_PERCENT));

    for (size_t i = 0; i < mAttrConfigs.size(); ++i)
    {
        packAttrConfig->AddAttributeConfig(mAttrConfigs[i]);
    }
    return packAttrConfig;    
}

FieldType ValueConfigImpl::GetFixedLengthValueType() const
{
    int32_t valueSize = GetFixedLength();
    if (valueSize < 0 || valueSize > 8) {
        assert(false);
        return ft_unknown;
    }
    
    if (GetAttributeCount() == 1) {
        AttributeConfigPtr tempConf = mAttrConfigs[0];
        if (!tempConf->IsMultiValue() && tempConf->GetFieldType() != ft_string) {
            return mActualFieldType;
        }
    }
    return CalculateFixLenValueType(valueSize);
}

FieldType ValueConfigImpl::CalculateFixLenValueType(int32_t size) const
{
    switch(size) {
    case 1:
        return ft_byte1;
    case 2:
        return ft_byte2;
    case 3:
        return ft_byte3;
    case 4:
        return ft_byte4;
    case 5:
        return ft_byte5;
    case 6:
        return ft_byte6;
    case 7:
        return ft_byte7;
    case 8:
        return ft_byte8;
    default:
        break;
    }
    return ft_unknown;
}

FieldType ValueConfigImpl::GetActualFieldType() const
{
    return mActualFieldType;
}

IE_NAMESPACE_END(config);
