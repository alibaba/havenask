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
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"

#include "autil/Log.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/index/common/field_format/attribute/DefaultAttributeValueInitializer.h"
#include "indexlib/index/common/field_format/attribute/DefaultAttributeValueInitializerCreator.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/slice_array/DefragSliceArray.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PackAttributeFormatter);

bool PackAttributeFormatter::Init(const std::shared_ptr<indexlibv2::index::PackAttributeConfig>& packAttrConfig)
{
    assert(packAttrConfig);
    _packAttrConfig = packAttrConfig;
    _enableImpact = _packAttrConfig->HasEnableImpact();
    _enablePlainFormat = _packAttrConfig->HasEnablePlainFormat();

    auto subConfs = packAttrConfig->GetAttributeConfigVec();
    std::vector<std::shared_ptr<index::AttributeConfig>> subAttrConfs;
    subAttrConfs.reserve(subConfs.size());
    for (auto conf : subConfs) {
        subAttrConfs.emplace_back(conf);
    }
    ClassifySubAttributeConfigs(subAttrConfs);

    if (!InitAttributeConvertors()) {
        AUTIL_LOG(ERROR, "init attribute convertors fail!");
        ERROR_COLLECTOR_LOG(ERROR, "init attribute convertors fail!");
        return false;
    }

    if (!InitAttributeReferences()) {
        AUTIL_LOG(ERROR, "init attribute references fail!");
        ERROR_COLLECTOR_LOG(ERROR, "init attribute references fail!");
        return false;
    }

    if (!InitSingleValueAttributeInitializers()) {
        AUTIL_LOG(ERROR, "init attribute value initializer fail!");
        ERROR_COLLECTOR_LOG(ERROR, "init attribute value initializer fail!");
        return false;
    }
    return true;
}

bool PackAttributeFormatter::AddImpactAttributeReference(const std::shared_ptr<index::AttributeConfig>& attrConfig,
                                                         size_t varIdx)
{
    assert(attrConfig->IsMultiValue() || attrConfig->GetFieldType() == ft_string);
    const std::string& attrName = attrConfig->GetAttrName();
    if (IsAttributeReferenceExist(attrName)) {
        AUTIL_LOG(ERROR, "Init pack attribute formatter failed, attribute [%s] duplicated", attrName.c_str());
        return false;
    }

    indexlib::config::CompressTypeOption compressType = attrConfig->GetCompressType();
    FieldType fieldType = attrConfig->GetFieldType();
    _attrName2Idx[attrName] = _attrRefs.size();
    AddItemToAttrIdMap(attrConfig->GetAttrId(), _attrRefs.size());
    switch (fieldType) {
#define CASE_MACRO(ft)                                                                                                 \
    case ft: {                                                                                                         \
        typedef indexlib::index::FieldTypeTraits<ft>::AttrItemType T;                                                  \
        bool isLastVar = ((varIdx + 1) == _varLenAttributes.size());                                                   \
        ReferenceOffset offset =                                                                                       \
            ReferenceOffset::impactOffset(_fixAttrSize, varIdx, _varLenAttributes.size(), isLastVar);                  \
        auto ref = std::make_shared<AttributeReferenceTyped<autil::MultiValueType<T>>>(                                \
            offset, attrConfig->GetAttrName(), compressType, -1);                                                      \
        _attrRefs.push_back(ref);                                                                                      \
        break;                                                                                                         \
    }

        NUMBER_FIELD_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO

    case ft_string: {
        if (attrConfig->IsMultiValue()) {
            auto ref = std::make_shared<AttributeReferenceTyped<autil::MultiValueType<autil::MultiChar>>>(
                ReferenceOffset::impactOffset(_fixAttrSize, varIdx, _varLenAttributes.size(), true),
                attrConfig->GetAttrName(), compressType, -1);
            _attrRefs.push_back(ref);
        } else {
            bool isLastVar = ((varIdx + 1) == _varLenAttributes.size());
            auto ref = std::make_shared<AttributeReferenceTyped<autil::MultiChar>>(
                ReferenceOffset::impactOffset(_fixAttrSize, varIdx, _varLenAttributes.size(), isLastVar),
                attrConfig->GetAttrName(), compressType, -1);
            _attrRefs.push_back(ref);
        }
        break;
    }
    default:
        assert(false);
        break;
    }
    return true;
}

bool PackAttributeFormatter::AddNormalAttributeReference(const std::shared_ptr<index::AttributeConfig>& attrConfig,
                                                         size_t& offset)
{
    const std::string& attrName = attrConfig->GetAttrName();
    if (IsAttributeReferenceExist(attrName)) {
        AUTIL_LOG(ERROR, "Init pack attribute formatter failed, attribute [%s] duplicated", attrName.c_str());
        return false;
    }
    FieldType fieldType = attrConfig->GetFieldType();
    size_t fieldSlotLen = 0;
    if (attrConfig->IsLengthFixed()) {
        fieldSlotLen = attrConfig->GetFixLenFieldSize();
    } else {
        fieldSlotLen = sizeof(MultiValueOffsetType);
    }

    _attrName2Idx[attrName] = _attrRefs.size();
    AddItemToAttrIdMap(attrConfig->GetAttrId(), _attrRefs.size());

    switch (fieldType) {
#define CASE_MACRO(ft)                                                                                                 \
    case ft: {                                                                                                         \
        typedef indexlib::index::FieldTypeTraits<ft>::AttrItemType T;                                                  \
        _attrRefs.push_back(CreateAttributeReference<T>(offset, attrConfig));                                          \
        break;                                                                                                         \
    }
        NUMBER_FIELD_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO

    case ft_string:
        _attrRefs.push_back(CreateStringAttributeReference(offset, attrConfig));
        break;

    default:
        assert(false);
        break;
    }
    offset += fieldSlotLen;
    return true;
}

bool PackAttributeFormatter::IsAttributeReferenceExist(const std::string& attrName) const
{
    return _attrName2Idx.find(attrName) != _attrName2Idx.end();
}

std::shared_ptr<AttributeReference>
PackAttributeFormatter::CreateStringAttributeReference(size_t offset,
                                                       const std::shared_ptr<index::AttributeConfig>& attrConfig)
{
    indexlib::config::CompressTypeOption compressType = attrConfig->GetCompressType();
    if (attrConfig->IsMultiValue()) {
        return std::make_shared<AttributeReferenceTyped<autil::MultiString>>(
            ReferenceOffset::normalOffset(offset, true), attrConfig->GetAttrName(), compressType, UNKNOWN_VALUE_COUNT);
    }

    if (attrConfig->IsLengthFixed()) {
        return std::make_shared<AttributeReferenceTyped<autil::MultiChar>>(
            ReferenceOffset::normalOffset(offset, false), attrConfig->GetAttrName(), compressType,
            attrConfig->GetFieldConfig()->GetFixedMultiValueCount());
    }
    return std::make_shared<AttributeReferenceTyped<autil::MultiChar>>(
        ReferenceOffset::normalOffset(offset, true), attrConfig->GetAttrName(), compressType, UNKNOWN_VALUE_COUNT);
}

autil::StringView PackAttributeFormatter::Format(const std::vector<autil::StringView>& fields,
                                                 autil::mem_pool::Pool* pool)
{
    // convertor convert buf
    return _dataConvertor->Encode(FormatAttrField(fields, pool), pool);
}

autil::StringView PackAttributeFormatter::FormatAttrField(const std::vector<autil::StringView>& fields,
                                                          autil::mem_pool::Pool* pool)
{
    assert(_packAttrConfig);
    assert(pool);

    assert(fields.size() == _attrRefs.size());
    size_t cursor = 0;
    std::vector<AttributeFieldData> attrFieldData;
    attrFieldData.reserve(fields.size());

    // fixLenAttributes lays before varLenAttributes in both _attrRefs[] and attrFieldData[]
    // and in the exactly same order
    for (size_t i = 0; i < _fixLenAttributes.size(); ++i) {
        auto convertor = _fixLenAttrConvertors[i];
        assert(convertor);
        const auto& attrConfig = _fixLenAttributes[i];
        const autil::StringView& field = fields[cursor++];
        if (field.empty()) {
            if (attrConfig->IsMultiValue()) {
                AUTIL_LOG(ERROR, "Fixed MultiValue field[%s] should not be empty", attrConfig->GetAttrName().c_str());
                return autil::StringView::empty_instance();
            }
            assert(_fixLenAttrDefaultValueInitializer[i]);
            autil::StringView defaultValue = _fixLenAttrDefaultValueInitializer[i]->GetDefaultValue(pool);
            attrFieldData.push_back(std::make_pair(convertor->Decode(defaultValue).data, UNKNOWN_VALUE_COUNT));
        } else {
            attrFieldData.push_back(std::make_pair(convertor->Decode(field).data, UNKNOWN_VALUE_COUNT));
        }
    }

    for (size_t i = 0; i < _varLenAttributes.size(); ++i) {
        const autil::StringView& field = fields[cursor++];
        auto convertor = _varLenAttributeConvertors[i];
        assert(convertor);
        attrFieldData.push_back(std::make_pair(convertor->Decode(field).data, UNKNOWN_VALUE_COUNT));
    }

    size_t offsetUnitSize = 0;
    autil::StringView buf = ReserveBuffer(attrFieldData, pool, offsetUnitSize);
    if (!MergePackAttributeFields(attrFieldData, offsetUnitSize, buf)) {
        return autil::StringView::empty_instance();
    }
    return buf;
}

std::string PackAttributeFormatter::GetFieldStringValueFromPackedValue(const autil::StringView& packValue,
                                                                       attrid_t attrId, autil::mem_pool::Pool* pool)
{
    AttributeReference* attrRef = GetAttributeReference(attrId);
    if (packValue.empty() || attrRef == NULL) {
        return std::string("");
    }

    AttrValueMeta attrMeta = _dataConvertor->Decode(packValue);
    autil::MultiChar value;
    value.init(attrMeta.data.data());
    std::string ret;
    if (attrRef->GetStrValue(value.data(), ret, pool)) {
        return ret;
    }
    return std::string("");
}

AttributeReference::DataValue PackAttributeFormatter::GetFieldValueFromPackedValue(const autil::StringView& packValue,
                                                                                   attrid_t attrId)
{
    AttributeReference* attrRef = GetAttributeReference(attrId);
    if (packValue.empty() || attrRef == NULL) {
        return AttributeReference::DataValue();
    }

    AttrValueMeta attrMeta = _dataConvertor->Decode(packValue);
    autil::MultiChar value;
    value.init(attrMeta.data.data());
    return attrRef->GetDataValue(value.data());
}

autil::StringView PackAttributeFormatter::MergeAndFormatUpdateFields(const char* baseAddr,
                                                                     const PackAttributeFields& packAttrFields,
                                                                     bool hasHashKeyInAttrFields,
                                                                     indexlib::util::MemBuffer& buffer)
{
    std::vector<AttributeFieldData> attrFieldData;
    if (!GetMergedAttributeFieldData(baseAddr, packAttrFields, hasHashKeyInAttrFields, attrFieldData)) {
        return autil::StringView::empty_instance();
    }

    size_t offsetUnitSize = 0;
    size_t dataLen = CalculatePackDataSize(attrFieldData, offsetUnitSize);
    size_t encodeCountLen = MultiValueAttributeFormatter::GetEncodedCountLength(dataLen);
    if (indexlib::util::DefragSliceArray::IsOverLength(dataLen + encodeCountLen, index::ATTRIBUTE_DEFAULT_SLICE_LEN)) {
        AUTIL_LOG(WARN, "updated pack attribute data is overlength, ignore");
        ERROR_COLLECTOR_LOG(WARN, "updated pack attribute data is overlength, ignore");
        return autil::StringView::empty_instance();
    }

    // another dataLen + hashKey + count for DataConvertor memory use
    size_t buffSize = dataLen * 2 + sizeof(uint64_t) + encodeCountLen;
    buffer.Reserve(buffSize);

    autil::StringView mergeBuffer(buffer.GetBuffer(), dataLen);
    if (!MergePackAttributeFields(attrFieldData, offsetUnitSize, mergeBuffer)) {
        return autil::StringView::empty_instance();
    }
    // convertor convert buf
    return _dataConvertor->Encode(mergeBuffer, buffer.GetBuffer() + dataLen);
}

const std::shared_ptr<index::AttributeConfig>&
PackAttributeFormatter::GetAttributeConfig(const std::string& attrName) const
{
    AttrNameToIdx::const_iterator iter = _attrName2Idx.find(attrName);
    if (iter == _attrName2Idx.end() || (iter->second >= _fixLenAttributes.size() + _varLenAttributes.size())) {
        static std::shared_ptr<index::AttributeConfig> retNullPtr;
        return retNullPtr;
    }
    size_t idx = iter->second;
    assert(idx < _fixLenAttributes.size() + _varLenAttributes.size());
    if (idx < _fixLenAttributes.size()) {
        return _fixLenAttributes[idx];
    }
    return _varLenAttributes[idx - _fixLenAttributes.size()];
}

bool PackAttributeFormatter::MergePackAttributeFields(const std::vector<AttributeFieldData>& attrFieldData,
                                                      size_t offsetUnitSize, autil::StringView& mergeBuffer)
{
    if (offsetUnitSize != sizeof(uint64_t) && _varLenAttributes.size() > 1) {
        char* baseAddr = (char*)mergeBuffer.data();
        assert(_fixAttrSize < mergeBuffer.size());
        baseAddr[_fixAttrSize] = (char)offsetUnitSize;
    }
    size_t dataCursor = 0;
    for (size_t i = 0; i < _attrRefs.size(); ++i) {
        if (i == _fixLenAttributes.size()) {
            // all fixLen attributes have been processed, dataCursor skip the MultiValueType Array
            if (offsetUnitSize == sizeof(uint64_t)) {
                dataCursor += sizeof(MultiValueOffsetType) * _varLenAttributes.size();
            } else if (_varLenAttributes.size() > 1) {
                dataCursor += sizeof(char);
                dataCursor += (_varLenAttributes.size() - 1) * offsetUnitSize;
            }
        }
        if (attrFieldData[i].second == UNKNOWN_VALUE_COUNT) {
            dataCursor += _attrRefs[i]->SetValue((char*)mergeBuffer.data(), dataCursor, attrFieldData[i].first);
        } else {
            dataCursor += _attrRefs[i]->SetDataValue((char*)mergeBuffer.data(), dataCursor, attrFieldData[i].first,
                                                     attrFieldData[i].second);
        }
    }

    if (dataCursor != mergeBuffer.size()) {
        AUTIL_LOG(ERROR, "format pack attributes failed due to unexpected data length.");
        ERROR_COLLECTOR_LOG(ERROR, "format pack attributes failed due to unexpected data length.");
        return false;
    }
    return true;
}

bool PackAttributeFormatter::GetMergedAttributeFieldData(const char* baseAddr,
                                                         const PackAttributeFields& packAttrFields,
                                                         bool hasHashKeyInAttrFields,
                                                         std::vector<AttributeFieldData>& attrFieldData)
{
    assert(baseAddr);
    assert(!packAttrFields.empty());
    assert(attrFieldData.empty());
    attrFieldData.resize(_attrRefs.size(), std::make_pair(autil::StringView::empty_instance(), UNKNOWN_VALUE_COUNT));
    for (size_t i = 0; i < packAttrFields.size(); ++i) {
        assert(!packAttrFields[i].second.empty());
        int32_t idx = GetIdxByAttrId(packAttrFields[i].first);
        if (idx == UNKNOWN_VALUE_COUNT) {
            AUTIL_LOG(ERROR, "attribute [attrId:%u] not in pack attribute [%s]", packAttrFields[i].first,
                      _packAttrConfig->GetPackName().c_str());
            ERROR_COLLECTOR_LOG(ERROR, "attribute [attrId:%u] not in pack attribute [%s]", packAttrFields[i].first,
                                _packAttrConfig->GetPackName().c_str());
            return false;
        }
        if (hasHashKeyInAttrFields) {
            const std::shared_ptr<AttributeConvertor>& attrConvertor = GetAttributeConvertorByIdx(idx);
            assert(attrConvertor);
            attrFieldData[idx] =
                std::make_pair(attrConvertor->Decode(packAttrFields[i].second).data, UNKNOWN_VALUE_COUNT);
        } else {
            attrFieldData[idx] = std::make_pair(packAttrFields[i].second, UNKNOWN_VALUE_COUNT);
        }
    }

    for (size_t i = 0; i < attrFieldData.size(); ++i) {
        if (attrFieldData[i].first.empty()) {
            auto dataValue = _attrRefs[i]->GetDataValue(baseAddr);
            if (dataValue.isFixedValueLen || dataValue.hasCountInValueStr) {
                attrFieldData[i] = std::make_pair(dataValue.valueStr, UNKNOWN_VALUE_COUNT);
            } else {
                attrFieldData[i] = std::make_pair(dataValue.valueStr, dataValue.valueCount);
            }
        }
    }
    return true;
}

size_t PackAttributeFormatter::CalculatePackDataSize(const std::vector<AttributeFieldData>& attrFieldData,
                                                     size_t& offsetUnitSize)
{
    size_t bufLen = 0;
    size_t totalVarDataLen = 0;
    size_t lastVarDataLen = 0;
    for (size_t i = 0; i < attrFieldData.size(); ++i) {
        if (i < _fixLenAttributes.size() || !_enableImpact) {
            bufLen += attrFieldData[i].first.length();
            continue;
        }

        bool needEncodeCount = _attrRefs[i]->NeedVarLenHeader();
        if (!needEncodeCount && attrFieldData[i].second == UNKNOWN_VALUE_COUNT) {
            size_t encodeCountLen =
                MultiValueAttributeFormatter::GetEncodedCountFromFirstByte(attrFieldData[i].first.data()[0]);
            assert(attrFieldData[i].first.length() >= encodeCountLen);
            lastVarDataLen = attrFieldData[i].first.length() - encodeCountLen;
        } else {
            lastVarDataLen = attrFieldData[i].first.length();
        }
        bufLen += lastVarDataLen;
        totalVarDataLen += lastVarDataLen;
    }

    if (!_enableImpact) {
        // add MultiValueType array length for var-len attributes
        offsetUnitSize = sizeof(MultiValueOffsetType);
        bufLen += sizeof(MultiValueOffsetType) * _varLenAttributes.size();
    } else {
        size_t maxOffset = totalVarDataLen - lastVarDataLen;
        if (maxOffset <= (size_t)std::numeric_limits<uint8_t>::max()) {
            offsetUnitSize = sizeof(uint8_t);
        } else if (maxOffset <= (size_t)std::numeric_limits<uint16_t>::max()) {
            offsetUnitSize = sizeof(uint16_t);
        } else {
            offsetUnitSize = sizeof(uint32_t);
        }
        if (_varLenAttributes.size() > 1) {
            bufLen += sizeof(char); // store offsetUnitSize in 1 byte
            bufLen += offsetUnitSize * (_varLenAttributes.size() - 1);
        }
    }
    return bufLen;
}

autil::StringView PackAttributeFormatter::ReserveBuffer(const std::vector<AttributeFieldData>& attrFieldData,
                                                        autil::mem_pool::Pool* pool, size_t& offsetUnitSize)
{
    size_t dataLen = CalculatePackDataSize(attrFieldData, offsetUnitSize);
    char* bufAddr = (char*)pool->allocate(dataLen);
    return autil::StringView(bufAddr, dataLen);
}

AttributeReference* PackAttributeFormatter::GetAttributeReference(const std::string& subAttrName) const
{
    AttrNameToIdx::const_iterator it = _attrName2Idx.find(subAttrName);
    if (it == _attrName2Idx.end()) {
        return NULL;
    }

    assert(it->second < _attrRefs.size());
    return _attrRefs[it->second].get();
}

AttributeReference* PackAttributeFormatter::GetAttributeReference(attrid_t subAttrId) const
{
    int32_t idx = GetIdxByAttrId(subAttrId);
    if (idx < 0) {
        return NULL;
    }

    assert(idx < (int32_t)_attrRefs.size());
    return _attrRefs[idx].get();
}

void PackAttributeFormatter::ClassifySubAttributeConfigs(
    const std::vector<std::shared_ptr<index::AttributeConfig>>& subAttrConfs)
{
    _fixAttrSize = 0;
    for (size_t i = 0; i < subAttrConfs.size(); i++) {
        const std::shared_ptr<index::AttributeConfig>& attrConfig = subAttrConfs[i];
        if (attrConfig->IsLengthFixed()) {
            _fixLenAttributes.push_back(attrConfig);
            _fixAttrSize += attrConfig->GetFixLenFieldSize();
        } else {
            _varLenAttributes.push_back(attrConfig);
        }
    }
}

bool PackAttributeFormatter::InitAttributeConvertors()
{
    for (size_t i = 0; i < _fixLenAttributes.size(); i++) {
        std::shared_ptr<AttributeConvertor> convertor(
            AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(_fixLenAttributes[i]));
        if (!convertor) {
            AUTIL_LOG(ERROR, "fail to create AttributeConvertor for Attribute: %s",
                      _fixLenAttributes[i]->GetAttrName().c_str());
            ERROR_COLLECTOR_LOG(ERROR, "fail to create AttributeConvertor for Attribute: %s",
                                _fixLenAttributes[i]->GetAttrName().c_str());
            return false;
        }
        _fixLenAttrConvertors.push_back(convertor);
    }

    for (size_t i = 0; i < _varLenAttributes.size(); i++) {
        std::shared_ptr<AttributeConvertor> convertor(
            AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(_varLenAttributes[i]));
        if (!convertor) {
            AUTIL_LOG(ERROR, "fail to create AttributeConvertor for Attribute: %s",
                      _varLenAttributes[i]->GetAttrName().c_str());
            ERROR_COLLECTOR_LOG(ERROR, "fail to create AttributeConvertor for Attribute: %s",
                                _varLenAttributes[i]->GetAttrName().c_str());
            return false;
        }
        _varLenAttributeConvertors.push_back(convertor);
    }

    std::shared_ptr<index::AttributeConfig> packDataAttrConfig = _packAttrConfig->CreateAttributeConfig();
    if (packDataAttrConfig == nullptr) {
        AUTIL_LOG(ERROR, "fail to create attribute config for pack attribute %s",
                  _packAttrConfig->GetIndexName().c_str());
        ERROR_COLLECTOR_LOG(ERROR, "fail to create attribute config for pack attribute %s",
                            _packAttrConfig->GetIndexName().c_str());
        assert(false);
        return false;
    }
    _dataConvertor.reset(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(packDataAttrConfig));
    if (!_dataConvertor) {
        AUTIL_LOG(ERROR, "fail to create AttributeConvertor for pack attribute: %s",
                  packDataAttrConfig->GetAttrName().c_str());
        ERROR_COLLECTOR_LOG(ERROR, "fail to create AttributeConvertor for pack attribute: %s",
                            packDataAttrConfig->GetAttrName().c_str());
        return false;
    }
    return true;
}

bool PackAttributeFormatter::InitSingleValueAttributeInitializers()
{
    for (size_t i = 0; i < _fixLenAttributes.size(); i++) {
        if (_fixLenAttributes[i]->IsMultiValue()) {
            // fixed length multiValue does not support default initializer
            _fixLenAttrDefaultValueInitializer.push_back(nullptr);
            continue;
        }
        DefaultAttributeValueInitializerCreator initializerCreator(_fixLenAttributes[i]);

        std::shared_ptr<AttributeValueInitializer> initializer = initializerCreator.Create();
        if (!initializer) {
            AUTIL_LOG(ERROR, "fail to init AttrValueInitializer for Attribute: %s",
                      _fixLenAttributes[i]->GetAttrName().c_str());
            ERROR_COLLECTOR_LOG(ERROR, "fail to init AttrValueInitializer for Attribute: %s",
                                _fixLenAttributes[i]->GetAttrName().c_str());
            return false;
        }
        _fixLenAttrDefaultValueInitializer.push_back(
            std::dynamic_pointer_cast<DefaultAttributeValueInitializer>(initializer));
    }
    return true;
}

bool PackAttributeFormatter::InitAttributeReferences()
{
    size_t offset = 0;
    for (size_t i = 0; i < _fixLenAttributes.size(); i++) {
        _fieldIdVec.push_back(_fixLenAttributes[i]->GetFieldId());
        if (!AddNormalAttributeReference(_fixLenAttributes[i], offset)) {
            _fieldIdVec.clear();
            return false;
        }
    }
    for (size_t i = 0; i < _varLenAttributes.size(); i++) {
        _fieldIdVec.push_back(_varLenAttributes[i]->GetFieldId());
        bool ret = false;
        if (_enableImpact) {
            ret = AddImpactAttributeReference(_varLenAttributes[i], i);
        } else {
            ret = AddNormalAttributeReference(_varLenAttributes[i], offset);
        }
        if (!ret) {
            _fieldIdVec.clear();
            return false;
        }
    }
    return true;
}

size_t PackAttributeFormatter::GetEncodePatchValueLen(const PackAttributeFields& patchFields)
{
    if (patchFields.empty()) {
        return 0;
    }
    size_t len = sizeof(uint32_t); // count
    for (size_t i = 0; i < patchFields.size(); ++i) {
        size_t patchDataLen = patchFields[i].second.size();
        len += MultiValueAttributeFormatter::GetEncodedCountLength(patchDataLen);

        len += sizeof(attrid_t);
        len += patchDataLen;
    }
    return len;
}

size_t PackAttributeFormatter::EncodePatchValues(const PackAttributeFields& patchFields, uint8_t* buffer,
                                                 size_t bufferLen)
{
    if (!buffer || patchFields.empty()) {
        return 0;
    }
    uint8_t* end = buffer + bufferLen;
    uint8_t* cur = buffer;
    *(uint32_t*)cur = patchFields.size();
    cur += sizeof(uint32_t);

    for (size_t i = 0; i < patchFields.size(); ++i) {
        size_t patchDataLen = patchFields[i].second.size();
        size_t encodeLength = MultiValueAttributeFormatter::GetEncodedCountLength(patchDataLen);
        if (size_t(end - cur) < (sizeof(attrid_t) + encodeLength + patchDataLen)) {
            // buffer overflow
            return 0;
        }
        *(attrid_t*)cur = patchFields[i].first;
        cur += sizeof(attrid_t);

        MultiValueAttributeFormatter::EncodeCount(patchDataLen, (char*)cur, encodeLength);
        cur += encodeLength;
        memcpy(cur, patchFields[i].second.data(), patchDataLen);
        cur += patchDataLen;
    }
    return cur - buffer;
}

bool PackAttributeFormatter::DecodePatchValues(uint8_t* buffer, size_t bufferLen, PackAttributeFields& patchFields)
{
    assert(buffer);
    assert(patchFields.empty());

    if (bufferLen == 0) // empty patchFields
    {
        return true;
    }

    uint8_t* end = buffer + bufferLen;
    uint8_t* cur = buffer;

    uint32_t fieldCount = *(uint32_t*)cur;
    cur += sizeof(uint32_t);

    for (size_t i = 0; i < fieldCount; ++i) {
        // 1 byte for first byte of count
        if (size_t(end - cur) < sizeof(attrid_t) + sizeof(uint8_t)) {
            return false;
        }
        attrid_t attrId = *(attrid_t*)cur;
        cur += sizeof(attrid_t);
        size_t encodeCountLen = MultiValueAttributeFormatter::GetEncodedCountFromFirstByte(*cur);
        if (size_t(end - cur) < encodeCountLen) {
            return false;
        }

        bool isNull = false;
        uint32_t dataLen = MultiValueAttributeFormatter::DecodeCount((const char*)cur, encodeCountLen, isNull);
        assert(!isNull);

        cur += encodeCountLen;
        if (size_t(end - cur) < dataLen) {
            return false;
        }
        patchFields.push_back(std::make_pair(attrId, autil::StringView((const char*)cur, dataLen)));
        cur += dataLen;
    }
    return cur == end;
}

bool PackAttributeFormatter::CheckLength(const autil::StringView& packAttrField)
{
    AttrValueMeta attrMeta = _dataConvertor->Decode(packAttrField);

    autil::MultiChar value;
    value.init(attrMeta.data.data());
    size_t size = value.size();
    if (_varLenAttributes.empty()) {
        return _fixAttrSize == size;
    }

    size_t minLength = 0;
    if (_enableImpact) {
        minLength = _fixAttrSize;
        if (_varLenAttributes.size() > 1) {
            minLength += sizeof(char);
            minLength += (_varLenAttributes.size() - 1) * sizeof(char);
        }
    } else {
        minLength = _fixAttrSize + _varLenAttributes.size() * sizeof(MultiValueOffsetType);
    }
    // not use strict check logic : for build efficiency
    return size > minLength;
}

void PackAttributeFormatter::GetAttributesWithStoreOrder(std::vector<std::string>& attributes) const
{
    for (size_t i = 0; i < _fixLenAttributes.size(); ++i) {
        attributes.push_back(_fixLenAttributes[i]->GetAttrName());
    }
    for (size_t i = 0; i < _varLenAttributes.size(); ++i) {
        attributes.push_back(_varLenAttributes[i]->GetAttrName());
    }
}

PlainFormatEncoder* PackAttributeFormatter::CreatePlainFormatEncoder() const
{
    if (!_enablePlainFormat || _varLenAttributes.empty()) {
        return nullptr;
    }
    std::vector<std::shared_ptr<AttributeReference>> fixLenAttrRefs(_attrRefs.begin(),
                                                                    _attrRefs.begin() + _fixLenAttributes.size());
    std::vector<std::shared_ptr<AttributeReference>> varLenAttrRefs(_attrRefs.begin() + _fixLenAttributes.size(),
                                                                    _attrRefs.end());
    return new PlainFormatEncoder(_fixAttrSize, fixLenAttrRefs, varLenAttrRefs);
}

PlainFormatEncoder*
PackAttributeFormatter::CreatePlainFormatEncoder(const std::shared_ptr<indexlibv2::index::PackAttributeConfig>& config)
{
    PackAttributeFormatter formatter;
    if (!formatter.Init(config)) {
        AUTIL_LOG(WARN, "init pack attribute formatter fail.");
        return nullptr;
    }
    return formatter.CreatePlainFormatEncoder();
}

} // namespace indexlibv2::index
