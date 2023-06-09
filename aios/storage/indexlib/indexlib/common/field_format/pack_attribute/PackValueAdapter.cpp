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
#include "indexlib/common/field_format/pack_attribute/PackValueAdapter.h"

#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/pack_attribute_config.h"

namespace indexlib::common {
AUTIL_LOG_SETUP(indexlib.common, PackValueAdapter);

bool PackValueAdapter::ValueState::Init(const config::PackAttributeConfigPtr& packConfig)
{
    assert(packConfig);
    if (packConfig->HasEnableImpact()) {
        AUTIL_LOG(ERROR, "not support impact value format");
        return false;
    }

    mPackFormatter.reset(new PackAttributeFormatter);
    if (!mPackFormatter->Init(packConfig)) {
        return false;
    }
    mPlainFormatEncoder.reset(mPackFormatter->CreatePlainFormatEncoder());
    auto attrReferenceVec = mPackFormatter->GetAttributeReferences();
    mDefaultValues.reserve(attrReferenceVec.size());
    for (size_t i = 0; i < attrReferenceVec.size(); i++) {
        auto attrName = attrReferenceVec[i]->GetAttrName();
        auto attrConfig = mPackFormatter->GetAttributeConfig(attrName);
        if (!attrConfig) {
            return false;
        }

        AttributeConvertorPtr convertor(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
        convertor->SetEncodeEmpty(true);
        auto encodeData = convertor->Encode(autil::StringView(attrConfig->GetFieldConfig()->GetDefaultValue()), &mPool);
        auto meta = convertor->Decode(encodeData);
        mDefaultValues.emplace_back(std::make_pair(meta.data, PackAttributeFormatter::UNKNOWN_VALUE_COUNT));
    }
    return true;
}

void PackValueAdapter::ValueState::GetAttributesWithStoreOrder(std::vector<std::string>& attributes) const
{
    if (!mPackFormatter) {
        return;
    }
    mPackFormatter->GetAttributesWithStoreOrder(attributes);
}

bool PackValueAdapter::ValueState::GetStoreOrder(const std::string& attribute, size_t& idx) const
{
    if (!mPackFormatter) {
        return false;
    }
    return mPackFormatter->GetAttributeStoreIndex(attribute, idx);
}

PackAttributeFormatter::AttributeFieldData PackValueAdapter::ValueState::GetDefaultValue(size_t idx) const
{
    if (idx < mDefaultValues.size()) {
        return mDefaultValues[idx];
    }
    return std::make_pair(autil::StringView::empty_instance(), PackAttributeFormatter::UNKNOWN_VALUE_COUNT);
}

bool PackValueAdapter::ValueState::UnpackValues(const autil::StringView& packValue,
                                                std::vector<PackAttributeFormatter::AttributeFieldData>& values) const
{
    if (mPlainFormatEncoder) {
        return mPlainFormatEncoder->DecodeDataValues(packValue, values);
    }

    if (mPackFormatter) {
        const std::vector<AttributeReferencePtr>& references = mPackFormatter->GetAttributeReferences();
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
    if (mPlainFormatEncoder) {
        return mPlainFormatEncoder->EncodeDataValues(values, pool);
    }

    size_t offsetUnitSize = 0;
    autil::StringView mergeBuffer = mPackFormatter->ReserveBuffer(values, pool, offsetUnitSize);
    assert(offsetUnitSize == sizeof(uint64_t));
    if (!mPackFormatter->MergePackAttributeFields(values, offsetUnitSize, mergeBuffer)) {
        return autil::StringView::empty_instance();
    }
    return mergeBuffer;
}

void PackValueAdapter::ValueState::DisablePlainFormat() { mPlainFormatEncoder.reset(); }

PackValueAdapter::PackValueAdapter() : mNeedConvert(false) {}

PackValueAdapter::~PackValueAdapter() {}

bool PackValueAdapter::Init(const config::PackAttributeConfigPtr& current, const config::PackAttributeConfigPtr& target,
                            const std::vector<std::string>& ignoreFieldsInCurrent)
{
    if (!current || !target) {
        return false;
    }

    bool isEqual = CheckPackAttributeEqual(current, target, ignoreFieldsInCurrent);
    if (isEqual) {
        AUTIL_LOG(INFO, "pack attribute format is equal between current and target, no need convert.");
        mNeedConvert = false;
        return true;
    }

    if (!mCurrentState.Init(current)) {
        AUTIL_LOG(ERROR, "init pack value state for current fail.");
        return false;
    }

    if (!mTargetState.Init(target)) {
        AUTIL_LOG(ERROR, "init pack value state for target fail.");
        return false;
    }

    mNeedConvert = true;
    std::vector<std::string> targetAttributes;
    mTargetState.GetAttributesWithStoreOrder(targetAttributes);
    mConvOrderMap.resize(targetAttributes.size(), (int32_t)-1);
    for (size_t i = 0; i < targetAttributes.size(); i++) {
        if (mIgnoreFieldsInCurrent.find(targetAttributes[i]) != mIgnoreFieldsInCurrent.end()) {
            continue;
        }
        size_t idx = 0;
        if (mCurrentState.GetStoreOrder(targetAttributes[i], idx)) {
            mConvOrderMap[i] = idx;
        }
    }
    return true;
}

autil::StringView PackValueAdapter::ConvertIndexPackValue(const autil::StringView& packValue,
                                                          autil::mem_pool::Pool* pool)
{
    if (!mNeedConvert) {
        return packValue;
    }

    std::vector<PackAttributeFormatter::AttributeFieldData> values;
    mCurrentState.UnpackValues(packValue, values);

    std::vector<PackAttributeFormatter::AttributeFieldData> targetValues;
    targetValues.reserve(mConvOrderMap.size());
    for (size_t i = 0; i < mConvOrderMap.size(); i++) {
        int32_t idx = mConvOrderMap[i];
        if (idx >= 0) {
            targetValues.emplace_back(values[idx]);
        } else {
            targetValues.emplace_back(mTargetState.GetDefaultValue(i));
        }
    }
    return mTargetState.FormatValues(targetValues, pool);
}

bool PackValueAdapter::CheckPackAttributeEqual(const config::PackAttributeConfigPtr& current,
                                               const config::PackAttributeConfigPtr& target,
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
                mIgnoreFieldsInCurrent.insert(targetAttr->GetAttrName());
                AUTIL_LOG(INFO, "attribute [%s] in ignored field list.", targetAttr->GetAttrName().c_str());
                continue;
            }

            if (IsAttrEqual(currentAttr, targetAttr)) {
                break;
            }
            // same name but not equal, check fail
            AUTIL_LOG(WARN, "attribute [%s] not equal between current and target, will add to ignored field list.",
                      targetAttr->GetAttrName().c_str());
            mIgnoreFieldsInCurrent.insert(targetAttr->GetAttrName());
        }
    }

    if (currentAttributes.size() != targetAttributes.size()) {
        return false;
    }

    if (!mIgnoreFieldsInCurrent.empty()) {
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

bool PackValueAdapter::IsAttrEqual(const config::AttributeConfigPtr& current,
                                   const config::AttributeConfigPtr& target) const
{
    assert(current && target);
    return current->GetAttrName() == target->GetAttrName() && current->GetFieldType() == target->GetFieldType() &&
           current->IsMultiValue() == target->IsMultiValue() && current->IsLengthFixed() == target->IsLengthFixed() &&
           current->GetCompressType() == target->GetCompressType();
}

void PackValueAdapter::DisablePlainFormat()
{
    mCurrentState.DisablePlainFormat();
    mTargetState.DisablePlainFormat();
}

} // namespace indexlib::common
