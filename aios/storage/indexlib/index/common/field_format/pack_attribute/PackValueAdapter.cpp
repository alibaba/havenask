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
#include "indexlib/index/common/field_format/pack_attribute/PackValueAdapter.h"

#include "indexlib/index/attribute/config/PackAttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PackValueAdapter);

bool PackValueAdapter::ValueState::Init(const std::shared_ptr<indexlibv2::config::PackAttributeConfig>& packConfig)
{
    assert(packConfig);
    if (packConfig->HasEnableImpact()) {
        AUTIL_LOG(ERROR, "not support impact value format");
        return false;
    }

    _packFormatter.reset(new PackAttributeFormatter);
    if (!_packFormatter->Init(packConfig)) {
        return false;
    }
    _plainFormatEncoder.reset(_packFormatter->CreatePlainFormatEncoder());
    auto attrReferenceVec = _packFormatter->GetAttributeReferences();
    _defaultValues.reserve(attrReferenceVec.size());
    for (size_t i = 0; i < attrReferenceVec.size(); i++) {
        auto attrName = attrReferenceVec[i]->GetAttrName();
        auto attrConfig = _packFormatter->GetAttributeConfig(attrName);
        if (!attrConfig) {
            return false;
        }

        std::shared_ptr<AttributeConvertor> convertor(
            AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
        convertor->SetEncodeEmpty(true);
        auto encodeData = convertor->Encode(autil::StringView(attrConfig->GetFieldConfig()->GetDefaultValue()), &_pool);
        auto meta = convertor->Decode(encodeData);
        _defaultValues.emplace_back(std::make_pair(meta.data, PackAttributeFormatter::UNKNOWN_VALUE_COUNT));
    }
    return true;
}

void PackValueAdapter::ValueState::GetAttributesWithStoreOrder(std::vector<std::string>& attributes) const
{
    if (!_packFormatter) {
        return;
    }
    _packFormatter->GetAttributesWithStoreOrder(attributes);
}

bool PackValueAdapter::ValueState::GetStoreOrder(const std::string& attribute, size_t& idx) const
{
    if (!_packFormatter) {
        return false;
    }
    return _packFormatter->GetAttributeStoreIndex(attribute, idx);
}

PackAttributeFormatter::AttributeFieldData PackValueAdapter::ValueState::GetDefaultValue(size_t idx) const
{
    if (idx < _defaultValues.size()) {
        return _defaultValues[idx];
    }
    return std::make_pair(autil::StringView::empty_instance(), PackAttributeFormatter::UNKNOWN_VALUE_COUNT);
}

bool PackValueAdapter::ValueState::UnpackValues(const autil::StringView& packValue,
                                                std::vector<PackAttributeFormatter::AttributeFieldData>& values) const
{
    if (_plainFormatEncoder) {
        return _plainFormatEncoder->DecodeDataValues(packValue, values);
    }

    if (_packFormatter) {
        const std::vector<std::shared_ptr<AttributeReference>>& references = _packFormatter->GetAttributeReferences();
        values.reserve(references.size());
        for (auto& ref : references) {
            auto dataValue = ref->GetDataValue(packValue.data());
            if (dataValue.hasCountInValueStr || dataValue.isFixedValueLen) {
                values.emplace_back(std::make_pair(dataValue.valueStr, PackAttributeFormatter::UNKNOWN_VALUE_COUNT));
            } else {
                values.emplace_back(std::make_pair(dataValue.valueStr, dataValue.valueCount));
            }
        }
        return true;
    }
    return false;
}

autil::StringView
PackValueAdapter::ValueState::FormatValues(const std::vector<PackAttributeFormatter::AttributeFieldData>& values,
                                           autil::mem_pool::Pool* pool) const
{
    if (_plainFormatEncoder) {
        return _plainFormatEncoder->EncodeDataValues(values, pool);
    }

    size_t offsetUnitSize = 0;
    autil::StringView mergeBuffer = _packFormatter->ReserveBuffer(values, pool, offsetUnitSize);
    assert(offsetUnitSize == sizeof(uint64_t));
    if (!_packFormatter->MergePackAttributeFields(values, offsetUnitSize, mergeBuffer)) {
        return autil::StringView::empty_instance();
    }
    return mergeBuffer;
}

void PackValueAdapter::ValueState::DisablePlainFormat() { _plainFormatEncoder.reset(); }

PackValueAdapter::PackValueAdapter() : _needConvert(false) {}

PackValueAdapter::~PackValueAdapter() {}

bool PackValueAdapter::Init(const std::shared_ptr<indexlibv2::config::PackAttributeConfig>& current,
                            const std::shared_ptr<indexlibv2::config::PackAttributeConfig>& target,
                            const std::vector<std::string>& ignoreFieldsInCurrent)
{
    if (!current || !target) {
        return false;
    }

    bool isEqual = CheckPackAttributeEqual(current, target, ignoreFieldsInCurrent);
    if (isEqual) {
        AUTIL_LOG(DEBUG, "pack attribute format is equal between current and target, no need convert.");
        _needConvert = false;
        return true;
    }

    if (!_currentState.Init(current)) {
        AUTIL_LOG(ERROR, "init pack value state for current fail.");
        return false;
    }

    if (!_targetState.Init(target)) {
        AUTIL_LOG(ERROR, "init pack value state for target fail.");
        return false;
    }

    _needConvert = true;
    std::vector<std::string> targetAttributes;
    _targetState.GetAttributesWithStoreOrder(targetAttributes);
    _convOrderMap.resize(targetAttributes.size(), (int32_t)-1);
    for (size_t i = 0; i < targetAttributes.size(); i++) {
        if (_ignoreFieldsInCurrent.find(targetAttributes[i]) != _ignoreFieldsInCurrent.end()) {
            continue;
        }
        size_t idx = 0;
        if (_currentState.GetStoreOrder(targetAttributes[i], idx)) {
            _convOrderMap[i] = idx;
        }
    }
    return true;
}

autil::StringView PackValueAdapter::ConvertIndexPackValue(const autil::StringView& packValue,
                                                          autil::mem_pool::Pool* pool)
{
    if (!_needConvert) {
        return packValue;
    }

    std::vector<PackAttributeFormatter::AttributeFieldData> values;
    _currentState.UnpackValues(packValue, values);

    std::vector<PackAttributeFormatter::AttributeFieldData> targetValues;
    targetValues.reserve(_convOrderMap.size());
    for (size_t i = 0; i < _convOrderMap.size(); i++) {
        int32_t idx = _convOrderMap[i];
        if (idx >= 0) {
            targetValues.emplace_back(values[idx]);
        } else {
            targetValues.emplace_back(_targetState.GetDefaultValue(i));
        }
    }
    return _targetState.FormatValues(targetValues, pool);
}

bool PackValueAdapter::CheckPackAttributeEqual(const std::shared_ptr<indexlibv2::config::PackAttributeConfig>& current,
                                               const std::shared_ptr<indexlibv2::config::PackAttributeConfig>& target,
                                               const std::vector<std::string>& ignoreFieldsInCurrent)
{
    auto currentAttributes = current->GetAttributeConfigVec();
    auto targetAttributes = target->GetAttributeConfigVec();
    for (auto& targetAttr : targetAttributes) {
        for (auto& currentAttr : currentAttributes) {
            if (targetAttr->GetAttrName() != currentAttr->GetAttrName()) {
                continue;
            }

            if (std::find(ignoreFieldsInCurrent.begin(), ignoreFieldsInCurrent.end(), targetAttr->GetAttrName()) !=
                ignoreFieldsInCurrent.end()) {
                _ignoreFieldsInCurrent.insert(targetAttr->GetAttrName());
                AUTIL_LOG(INFO, "attribute [%s] in ignored field list.", targetAttr->GetAttrName().c_str());
                continue;
            }

            if (IsAttrEqual(currentAttr, targetAttr)) {
                break;
            }
            // same name but not equal, check fail
            AUTIL_LOG(WARN, "attribute [%s] not equal between current and target, will add to ignored field list.",
                      targetAttr->GetAttrName().c_str());
            _ignoreFieldsInCurrent.insert(targetAttr->GetAttrName());
        }
    }

    if (currentAttributes.size() != targetAttributes.size()) {
        return false;
    }

    if (!_ignoreFieldsInCurrent.empty()) {
        return false;
    }
    for (size_t i = 0; i < currentAttributes.size(); i++) {
        auto attrInCurrent = currentAttributes[i];
        auto attrInTarget = targetAttributes[i];
        if (!IsAttrEqual(attrInCurrent, attrInTarget)) {
            return false;
        }
    }
    return current->HasEnablePlainFormat() == target->HasEnablePlainFormat();
}

bool PackValueAdapter::IsAttrEqual(const std::shared_ptr<indexlibv2::config::AttributeConfig>& current,
                                   const std::shared_ptr<indexlibv2::config::AttributeConfig>& target) const
{
    assert(current && target);
    return current->GetAttrName() == target->GetAttrName() && current->GetFieldType() == target->GetFieldType() &&
           current->IsMultiValue() == target->IsMultiValue() && current->IsLengthFixed() == target->IsLengthFixed() &&
           current->GetCompressType() == target->GetCompressType();
}

void PackValueAdapter::DisablePlainFormat()
{
    _currentState.DisablePlainFormat();
    _targetState.DisablePlainFormat();
}

} // namespace indexlibv2::index
