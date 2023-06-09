/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/config/impl/value_config_impl.h"

#include "indexlib/config/configurator_define.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;
using namespace indexlib::common;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, ValueConfigImpl);

void ValueConfigImpl::Init(const vector<AttributeConfigPtr>& attrConfigs)
{
    if (attrConfigs.size() == 0) {
        INDEXLIB_FATAL_ERROR(Schema, "cannot create value config "
                                     "when no attribute config is defined");
    }
    mAttrConfigs = attrConfigs;
    if (attrConfigs.size() == 1) {
        if (attrConfigs[0]->GetFieldType() == FieldType::ft_float) {
            CompressTypeOption compressType = attrConfigs[0]->GetCompressType();
            // convert field_type for reader, not for builder/merger
            if (compressType.HasFp16EncodeCompress()) {
                mActualFieldType = ft_int16;
            } else if (compressType.HasInt8EncodeCompress()) {
                mActualFieldType = ft_int8;
            } else {
                mActualFieldType = ft_float;
            }
        } else {
            mActualFieldType = mAttrConfigs[0]->GetFieldType();
        }
    } else {
        // all field is packed
        mActualFieldType = ft_string;
    }
    mFixedValueLen = 0;
    for (const auto& attrConfig : attrConfigs) {
        if (!attrConfig->IsLengthFixed()) {
            mFixedValueLen = -1;
            break;
        }
        mFixedValueLen += attrConfig->GetFixLenFieldSize();
    }
    CheckSupportNullFields();
}

void ValueConfigImpl::CheckSupportNullFields() const
{
    for (auto attr : mAttrConfigs) {
        if (attr && attr->SupportNull()) {
            INDEXLIB_FATAL_ERROR(Schema, "not support init with enable null field attribute [%s]",
                                 attr->GetAttrName().c_str());
        }
    }
}

config::PackAttributeConfigPtr ValueConfigImpl::CreatePackAttributeConfig() const
{
    CompressTypeOption compressOption;
    THROW_IF_STATUS_ERROR(compressOption.Init(""));
    PackAttributeConfigPtr packAttrConfig(new PackAttributeConfig("pack_values", compressOption,
                                                                  index::ATTRIBUTE_DEFAULT_DEFRAG_SLICE_PERCENT,
                                                                  std::shared_ptr<FileCompressConfig>()));

    for (size_t i = 0; i < mAttrConfigs.size(); ++i) {
        auto status = packAttrConfig->AddAttributeConfig(mAttrConfigs[i]);
        THROW_IF_STATUS_ERROR(status);
    }

    if (mValueImpact) {
        packAttrConfig->EnableImpact();
    }
    if (mPlainFormat) {
        packAttrConfig->EnablePlainFormat();
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
    switch (size) {
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

FieldType ValueConfigImpl::GetActualFieldType() const { return mActualFieldType; }

bool ValueConfigImpl::IsSimpleValue() const
{
    return !mDisableSimpleValue && GetAttributeCount() == 1 && !GetAttributeConfig(0)->IsMultiValue() &&
           (GetAttributeConfig(0)->GetFieldType() != ft_string);
}

void ValueConfigImpl::DisableSimpleValue()
{
    mDisableSimpleValue = true;
    mFixedValueLen = -1;
}

}} // namespace indexlib::config
