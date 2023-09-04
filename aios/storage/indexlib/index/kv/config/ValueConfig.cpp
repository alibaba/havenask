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
#include "indexlib/index/kv/config/ValueConfig.h"

#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"

using namespace std;

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.index, ValueConfig);

struct ValueConfig::Impl {
    vector<shared_ptr<index::AttributeConfig>> attrConfigs;
    int32_t fixedValueLen = -1;
    FieldType actualFieldType = FieldType::ft_unknown;
    bool enableCompactPackAttribute = false;
    bool valueImpact = false;
    bool plainFormat = false;
    bool disableSimpleValue = false;
};

ValueConfig::ValueConfig() : _impl(make_unique<Impl>()) {}

ValueConfig::ValueConfig(const ValueConfig& other) : _impl(make_unique<Impl>(*other._impl)) {}

ValueConfig::~ValueConfig() {}

Status ValueConfig::Init(const vector<shared_ptr<index::AttributeConfig>>& attrConfigs)
{
    if (attrConfigs.size() == 0) {
        return Status::ConfigError("attribute configs is empty, init failed");
    }
    RETURN_IF_STATUS_ERROR(CheckIfAttributesSupportNull(attrConfigs), "check attribute config failed");

    SetFixedValueLen(attrConfigs);
    SetActualFieldType(attrConfigs);
    _impl->attrConfigs = attrConfigs;

    return Status::OK();
}

Status
ValueConfig::CheckIfAttributesSupportNull(const std::vector<std::shared_ptr<index::AttributeConfig>>& attrConfigs) const
{
    for (auto attr : attrConfigs) {
        if (attr && attr->SupportNull()) {
            return Status::ConfigError("attribute %s which support null is not allowed in kv/kkv index",
                                       attr->GetAttrName().c_str());
        }
    }
    return Status::OK();
}

void ValueConfig::SetActualFieldType(const std::vector<std::shared_ptr<index::AttributeConfig>>& attrConfigs)
{
    if (attrConfigs.size() > 1) {
        // all field is packed
        _impl->actualFieldType = ft_string;
        return;
    }

    auto attrConfig = attrConfigs[0];
    if (attrConfig->GetFieldType() == FieldType::ft_float) {
        auto compressType = attrConfig->GetCompressType();
        // convert field_type for reader, not for builder/merger
        if (compressType.HasFp16EncodeCompress()) {
            _impl->actualFieldType = ft_int16;
        } else if (compressType.HasInt8EncodeCompress()) {
            _impl->actualFieldType = ft_int8;
        } else {
            _impl->actualFieldType = ft_float;
        }
    } else {
        _impl->actualFieldType = attrConfig->GetFieldType();
    }
}

void ValueConfig::SetFixedValueLen(const std::vector<std::shared_ptr<index::AttributeConfig>>& attrConfigs)
{
    _impl->fixedValueLen = 0;
    for (const auto& attrConfig : attrConfigs) {
        if (!attrConfig->IsLengthFixed()) {
            _impl->fixedValueLen = -1;
            break;
        }
        _impl->fixedValueLen += attrConfig->GetFixLenFieldSize();
    }
}

size_t ValueConfig::GetAttributeCount() const { return _impl->attrConfigs.size(); }
const shared_ptr<index::AttributeConfig>& ValueConfig::GetAttributeConfig(size_t idx) const
{
    assert(idx < GetAttributeCount());
    return _impl->attrConfigs[idx];
}

const std::vector<std::shared_ptr<index::AttributeConfig>>& ValueConfig::GetAttributeConfigs() const
{
    return _impl->attrConfigs;
}

pair<Status, shared_ptr<index::PackAttributeConfig>> ValueConfig::CreatePackAttributeConfig() const
{
    auto packAttrConfig = std::make_shared<index::PackAttributeConfig>();
    for (auto attrConfig : _impl->attrConfigs) {
        auto status = packAttrConfig->AddAttributeConfig(attrConfig);
        RETURN2_IF_STATUS_ERROR(status, nullptr, "pack attribute config add attribute config[%s] fail",
                                attrConfig->GetAttrName().c_str());
    }

    if (_impl->valueImpact) {
        packAttrConfig->EnableImpact();
    }
    if (_impl->plainFormat) {
        packAttrConfig->EnablePlainFormat();
    }
    return {Status::OK(), packAttrConfig};
}

void ValueConfig::EnableCompactFormat(bool toSetCompactFormat)
{
    _impl->enableCompactPackAttribute = toSetCompactFormat;
}

bool ValueConfig::IsCompactFormatEnabled() const { return _impl->enableCompactPackAttribute; }

int32_t ValueConfig::GetFixedLength() const { return _impl->enableCompactPackAttribute ? _impl->fixedValueLen : -1; }

FieldType ValueConfig::GetFixedLengthValueType() const
{
    int32_t fixedLen = GetFixedLength();
    if (fixedLen < 0 || fixedLen > 8) {
        assert(false);
        return ft_unknown;
    }

    if (GetAttributeCount() == 1) {
        auto config = _impl->attrConfigs[0];
        if (!config->IsMultiValue() && config->GetFieldType() != ft_string) {
            return _impl->actualFieldType;
        }
    }
    return FixedLenToFieldType(fixedLen);
}

FieldType ValueConfig::GetActualFieldType() const { return _impl->actualFieldType; }

bool ValueConfig::IsSimpleValue() const
{
    return !_impl->disableSimpleValue && GetAttributeCount() == 1 && !GetAttributeConfig(0)->IsMultiValue() &&
           (GetAttributeConfig(0)->GetFieldType() != ft_string);
}

void ValueConfig::DisableSimpleValue()
{
    _impl->disableSimpleValue = true;
    _impl->fixedValueLen = -1;
}

bool ValueConfig::IsValueImpact() const { return _impl->valueImpact; }

void ValueConfig::EnableValueImpact(bool flag)
{
    _impl->valueImpact = flag;
    _impl->plainFormat = _impl->plainFormat && (!_impl->valueImpact);
}
bool ValueConfig::IsPlainFormat() const { return _impl->plainFormat; }
void ValueConfig::EnablePlainFormat(bool flag)
{
    _impl->plainFormat = flag;
    _impl->valueImpact = _impl->valueImpact && (!_impl->plainFormat);
}

FieldType ValueConfig::FixedLenToFieldType(int32_t size) const
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

Status ValueConfig::AppendField(const std::shared_ptr<FieldConfig>& fieldConfig)
{
    auto attrConfig = std::make_shared<index::AttributeConfig>();
    auto status = attrConfig->Init(fieldConfig);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "append field failed, status %s", status.ToString().c_str());
        assert(false);
        return status;
    }
    if (attrConfig->SupportNull()) {
        return Status::ConfigError("attribute %s which support null is not allowed in kv/kkv index",
                                   attrConfig->GetAttrName().c_str());
    }
    if (_impl->attrConfigs.empty()) {
        std::vector<std::shared_ptr<index::AttributeConfig>> attrConfigs;
        attrConfigs.emplace_back(std::move(attrConfig));
        return Init(attrConfigs);
    } else {
        // update fixedValueLen
        if (!attrConfig->IsLengthFixed()) {
            _impl->fixedValueLen = -1;
        } else if (_impl->fixedValueLen > 0) {
            _impl->fixedValueLen += attrConfig->GetFixLenFieldSize();
        }
        // update field type
        _impl->actualFieldType = ft_string;
        _impl->attrConfigs.push_back(attrConfig);
        return Status::OK();
    }
}

} // namespace indexlibv2::config
