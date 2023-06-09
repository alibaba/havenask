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
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"

#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common/field_format/attribute/default_attribute_value_initializer_creator.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/util/slice_array/DefragSliceArray.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, PackAttributeFormatter);

bool PackAttributeFormatter::Init(const PackAttributeConfigPtr& packAttrConfig)
{
    assert(packAttrConfig);
    mPackAttrConfig = packAttrConfig;
    mEnableImpact = mPackAttrConfig->HasEnableImpact();
    mEnablePlainFormat = mPackAttrConfig->HasEnablePlainFormat();

    const vector<AttributeConfigPtr>& subAttrConfs = packAttrConfig->GetAttributeConfigVec();
    ClassifySubAttributeConfigs(subAttrConfs);

    if (!InitAttributeConvertors()) {
        IE_LOG(ERROR, "init attribute convertors fail!");
        ERROR_COLLECTOR_LOG(ERROR, "init attribute convertors fail!");
        return false;
    }

    if (!InitAttributeReferences()) {
        IE_LOG(ERROR, "init attribute references fail!");
        ERROR_COLLECTOR_LOG(ERROR, "init attribute references fail!");
        return false;
    }

    if (!InitSingleValueAttributeInitializers()) {
        IE_LOG(ERROR, "init attribute value initializer fail!");
        ERROR_COLLECTOR_LOG(ERROR, "init attribute value initializer fail!");
        return false;
    }
    return true;
}

bool PackAttributeFormatter::AddImpactAttributeReference(const AttributeConfigPtr& attrConfig, size_t varIdx)
{
    assert(attrConfig->IsMultiValue() || attrConfig->GetFieldType() == ft_string);
    const string& attrName = attrConfig->GetAttrName();
    if (IsAttributeReferenceExist(attrName)) {
        IE_LOG(ERROR, "Init pack attribute formatter failed, attribute [%s] duplicated", attrName.c_str());
        return false;
    }

    config::CompressTypeOption compressType = attrConfig->GetCompressType();
    FieldType fieldType = attrConfig->GetFieldType();
    mAttrNameMap[attrName] = mAttrRefs.size();
    AddItemToAttrIdMap(attrConfig->GetAttrId(), mAttrRefs.size());
    switch (fieldType) {
#define CASE_MACRO(ft)                                                                                                 \
    case ft: {                                                                                                         \
        typedef FieldTypeTraits<ft>::AttrItemType T;                                                                   \
        bool isLastVar = ((varIdx + 1) == mVarLenAttributes.size());                                                   \
        ReferenceOffset offset =                                                                                       \
            ReferenceOffset::impactOffset(mFixAttrSize, varIdx, mVarLenAttributes.size(), isLastVar);                  \
        AttributeReferencePtr ref(new AttributeReferenceTyped<autil::MultiValueType<T>>(                               \
            offset, attrConfig->GetAttrName(), compressType, -1));                                                     \
        mAttrRefs.push_back(ref);                                                                                      \
        break;                                                                                                         \
    }

        NUMBER_FIELD_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO

    case ft_string: {
        if (attrConfig->IsMultiValue()) {
            AttributeReferencePtr ref(new AttributeReferenceTyped<autil::MultiValueType<autil::MultiChar>>(
                ReferenceOffset::impactOffset(mFixAttrSize, varIdx, mVarLenAttributes.size(), true),
                attrConfig->GetAttrName(), compressType, -1));
            mAttrRefs.push_back(ref);
        } else {
            bool isLastVar = ((varIdx + 1) == mVarLenAttributes.size());
            AttributeReferencePtr ref(new AttributeReferenceTyped<autil::MultiChar>(
                ReferenceOffset::impactOffset(mFixAttrSize, varIdx, mVarLenAttributes.size(), isLastVar),
                attrConfig->GetAttrName(), compressType, -1));
            mAttrRefs.push_back(ref);
        }
        break;
    }
    default:
        assert(false);
        break;
    }
    return true;
}

bool PackAttributeFormatter::AddNormalAttributeReference(const AttributeConfigPtr& attrConfig, size_t& offset)
{
    const string& attrName = attrConfig->GetAttrName();
    if (IsAttributeReferenceExist(attrName)) {
        IE_LOG(ERROR, "Init pack attribute formatter failed, attribute [%s] duplicated", attrName.c_str());
        return false;
    }
    FieldType fieldType = attrConfig->GetFieldType();
    size_t fieldSlotLen = 0;
    if (attrConfig->IsLengthFixed()) {
        fieldSlotLen = attrConfig->GetFixLenFieldSize();
    } else {
        fieldSlotLen = sizeof(MultiValueOffsetType);
    }

    mAttrNameMap[attrName] = mAttrRefs.size();
    AddItemToAttrIdMap(attrConfig->GetAttrId(), mAttrRefs.size());

    switch (fieldType) {
#define CASE_MACRO(ft)                                                                                                 \
    case ft: {                                                                                                         \
        typedef FieldTypeTraits<ft>::AttrItemType T;                                                                   \
        mAttrRefs.push_back(CreateAttributeReference<T>(offset, attrConfig));                                          \
        break;                                                                                                         \
    }
        NUMBER_FIELD_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO

    case ft_string:
        mAttrRefs.push_back(CreateStringAttributeReference(offset, attrConfig));
        break;

    default:
        assert(false);
        break;
    }
    offset += fieldSlotLen;
    return true;
}

bool PackAttributeFormatter::IsAttributeReferenceExist(const string& attrName) const
{
    return mAttrNameMap.find(attrName) != mAttrNameMap.end();
}

AttributeReferencePtr PackAttributeFormatter::CreateStringAttributeReference(size_t offset,
                                                                             const AttributeConfigPtr& attrConfig)
{
    CompressTypeOption compressType = attrConfig->GetCompressType();
    if (attrConfig->IsMultiValue()) {
        return AttributeReferencePtr(new AttributeReferenceTyped<autil::MultiString>(
            ReferenceOffset::normalOffset(offset, true), attrConfig->GetAttrName(), compressType, UNKNOWN_VALUE_COUNT));
    }

    if (attrConfig->IsLengthFixed()) {
        return AttributeReferencePtr(new AttributeReferenceTyped<autil::MultiChar>(
            ReferenceOffset::normalOffset(offset, false), attrConfig->GetAttrName(), compressType,
            attrConfig->GetFieldConfig()->GetFixedMultiValueCount()));
    }
    return AttributeReferencePtr(new AttributeReferenceTyped<autil::MultiChar>(
        ReferenceOffset::normalOffset(offset, true), attrConfig->GetAttrName(), compressType, UNKNOWN_VALUE_COUNT));
}

StringView PackAttributeFormatter::Format(const vector<StringView>& fields, Pool* pool)
{
    assert(mPackAttrConfig);
    assert(pool);

    assert(fields.size() == mAttrRefs.size());
    size_t cursor = 0;
    vector<AttributeFieldData> attrFieldData;
    attrFieldData.reserve(fields.size());

    // fixLenAttributes lays before varLenAttributes in both mAttrRefs[] and attrFieldData[]
    // and in the exactly same order
    for (size_t i = 0; i < mFixLenAttributes.size(); ++i) {
        AttributeConvertorPtr convertor = mFixLenAttrConvertors[i];
        assert(convertor);
        const auto& attrConfig = mFixLenAttributes[i];
        const StringView& field = fields[cursor++];
        if (field.empty()) {
            if (attrConfig->IsMultiValue()) {
                INDEXLIB_FATAL_ERROR(InconsistentState, "Fixed MultiValue field[%s] should not be empty",
                                     attrConfig->GetAttrName().c_str());
            }
            assert(mFixLenAttrDefaultValueInitializer[i]);
            StringView defaultValue = mFixLenAttrDefaultValueInitializer[i]->GetDefaultValue(pool);
            attrFieldData.push_back(make_pair(convertor->Decode(defaultValue).data, UNKNOWN_VALUE_COUNT));
        } else {
            attrFieldData.push_back(make_pair(convertor->Decode(field).data, UNKNOWN_VALUE_COUNT));
        }
    }

    for (size_t i = 0; i < mVarLenAttributes.size(); ++i) {
        const StringView& field = fields[cursor++];
        AttributeConvertorPtr convertor = mVarLenAttrConvertors[i];
        assert(convertor);
        attrFieldData.push_back(make_pair(convertor->Decode(field).data, UNKNOWN_VALUE_COUNT));
    }

    size_t offsetUnitSize = 0;
    StringView buf = ReserveBuffer(attrFieldData, pool, offsetUnitSize);
    if (!MergePackAttributeFields(attrFieldData, offsetUnitSize, buf)) {
        return StringView::empty_instance();
    }
    // convertor convert buf
    return mDataConvertor->Encode(buf, pool);
}

string PackAttributeFormatter::GetFieldStringValueFromPackedValue(const StringView& packValue, attrid_t attrId,
                                                                  autil::mem_pool::Pool* pool)
{
    AttributeReference* attrRef = GetAttributeReference(attrId);
    if (packValue.empty() || attrRef == NULL) {
        return string("");
    }

    AttrValueMeta attrMeta = mDataConvertor->Decode(packValue);
    MultiChar value;
    value.init(attrMeta.data.data());
    string ret;
    if (attrRef->GetStrValue(value.data(), ret, pool)) {
        return ret;
    }
    return string("");
}

AttributeReference::DataValue PackAttributeFormatter::GetFieldValueFromPackedValue(const StringView& packValue,
                                                                                   attrid_t attrId)
{
    AttributeReference* attrRef = GetAttributeReference(attrId);
    if (packValue.empty() || attrRef == NULL) {
        return AttributeReference::DataValue();
    }

    AttrValueMeta attrMeta = mDataConvertor->Decode(packValue);
    MultiChar value;
    value.init(attrMeta.data.data());
    return attrRef->GetDataValue(value.data());
}

StringView PackAttributeFormatter::MergeAndFormatUpdateFields(const char* baseAddr,
                                                              const PackAttributeFields& packAttrFields,
                                                              bool hasHashKeyInAttrFields, MemBuffer& buffer)
{
    vector<AttributeFieldData> attrFieldData;
    if (!GetMergedAttributeFieldData(baseAddr, packAttrFields, hasHashKeyInAttrFields, attrFieldData)) {
        return StringView::empty_instance();
    }

    size_t offsetUnitSize = 0;
    size_t dataLen = CalculatePackDataSize(attrFieldData, offsetUnitSize);
    size_t encodeCountLen = VarNumAttributeFormatter::GetEncodedCountLength(dataLen);
    if (DefragSliceArray::IsOverLength(dataLen + encodeCountLen,
                                       VarNumAttributeFormatter::VAR_NUM_ATTRIBUTE_SLICE_LEN)) {
        IE_LOG(WARN, "updated pack attribute data is overlength, ignore");
        ERROR_COLLECTOR_LOG(WARN, "updated pack attribute data is overlength, ignore");
        return StringView::empty_instance();
    }

    // another dataLen + hashKey + count for DataConvertor memory use
    size_t buffSize = dataLen * 2 + sizeof(uint64_t) + encodeCountLen;
    buffer.Reserve(buffSize);

    StringView mergeBuffer(buffer.GetBuffer(), dataLen);
    if (!MergePackAttributeFields(attrFieldData, offsetUnitSize, mergeBuffer)) {
        return StringView::empty_instance();
    }
    // convertor convert buf
    return mDataConvertor->Encode(mergeBuffer, buffer.GetBuffer() + dataLen);
}

const AttributeConfigPtr& PackAttributeFormatter::GetAttributeConfig(const string& attrName) const
{
    AttrNameToIdxMap::const_iterator iter = mAttrNameMap.find(attrName);
    if (iter == mAttrNameMap.end() || (iter->second >= mFixLenAttributes.size() + mVarLenAttributes.size())) {
        static AttributeConfigPtr retNullPtr;
        return retNullPtr;
    }
    size_t idx = iter->second;
    assert(idx < mFixLenAttributes.size() + mVarLenAttributes.size());
    if (idx < mFixLenAttributes.size()) {
        return mFixLenAttributes[idx];
    }
    return mVarLenAttributes[idx - mFixLenAttributes.size()];
}

bool PackAttributeFormatter::MergePackAttributeFields(const vector<AttributeFieldData>& attrFieldData,
                                                      size_t offsetUnitSize, StringView& mergeBuffer)
{
    if (offsetUnitSize != sizeof(uint64_t) && mVarLenAttributes.size() > 1) {
        char* baseAddr = (char*)mergeBuffer.data();
        assert(mFixAttrSize < mergeBuffer.size());
        baseAddr[mFixAttrSize] = (char)offsetUnitSize;
    }
    size_t dataCursor = 0;
    for (size_t i = 0; i < mAttrRefs.size(); ++i) {
        if (i == mFixLenAttributes.size()) {
            // all fixLen attributes have been processed, dataCursor skip the MultiValueType Array
            if (offsetUnitSize == sizeof(uint64_t)) {
                dataCursor += sizeof(MultiValueOffsetType) * mVarLenAttributes.size();
            } else if (mVarLenAttributes.size() > 1) {
                dataCursor += sizeof(char);
                dataCursor += (mVarLenAttributes.size() - 1) * offsetUnitSize;
            }
        }
        if (attrFieldData[i].second == UNKNOWN_VALUE_COUNT) {
            dataCursor += mAttrRefs[i]->SetValue((char*)mergeBuffer.data(), dataCursor, attrFieldData[i].first);
        } else {
            dataCursor += mAttrRefs[i]->SetDataValue((char*)mergeBuffer.data(), dataCursor, attrFieldData[i].first,
                                                     attrFieldData[i].second);
        }
    }

    if (dataCursor != mergeBuffer.size()) {
        IE_LOG(ERROR, "format pack attributes failed due to unexpected data length.");
        ERROR_COLLECTOR_LOG(ERROR, "format pack attributes failed due to unexpected data length.");
        return false;
    }
    return true;
}

bool PackAttributeFormatter::GetMergedAttributeFieldData(const char* baseAddr,
                                                         const PackAttributeFields& packAttrFields,
                                                         bool hasHashKeyInAttrFields,
                                                         vector<AttributeFieldData>& attrFieldData)
{
    assert(baseAddr);
    assert(!packAttrFields.empty());
    assert(attrFieldData.empty());
    attrFieldData.resize(mAttrRefs.size(), make_pair(StringView::empty_instance(), UNKNOWN_VALUE_COUNT));
    for (size_t i = 0; i < packAttrFields.size(); ++i) {
        assert(!packAttrFields[i].second.empty());
        int32_t idx = GetIdxByAttrId(packAttrFields[i].first);
        if (idx == UNKNOWN_VALUE_COUNT) {
            IE_LOG(ERROR, "attribute [attrId:%u] not in pack attribute [%s]", packAttrFields[i].first,
                   mPackAttrConfig->GetAttrName().c_str());
            ERROR_COLLECTOR_LOG(ERROR, "attribute [attrId:%u] not in pack attribute [%s]", packAttrFields[i].first,
                                mPackAttrConfig->GetAttrName().c_str());
            return false;
        }
        if (hasHashKeyInAttrFields) {
            const AttributeConvertorPtr& attrConvertor = GetAttributeConvertorByIdx(idx);
            assert(attrConvertor);
            attrFieldData[idx] = make_pair(attrConvertor->Decode(packAttrFields[i].second).data, UNKNOWN_VALUE_COUNT);
        } else {
            attrFieldData[idx] = make_pair(packAttrFields[i].second, UNKNOWN_VALUE_COUNT);
        }
    }

    for (size_t i = 0; i < attrFieldData.size(); ++i) {
        if (attrFieldData[i].first.empty()) {
            auto dataValue = mAttrRefs[i]->GetDataValue(baseAddr);
            if (dataValue.isFixedValueLen || dataValue.hasCountInValueStr) {
                attrFieldData[i] = make_pair(dataValue.valueStr, UNKNOWN_VALUE_COUNT);
            } else {
                attrFieldData[i] = make_pair(dataValue.valueStr, dataValue.valueCount);
            }
        }
    }
    return true;
}

size_t PackAttributeFormatter::CalculatePackDataSize(const vector<AttributeFieldData>& attrFieldData,
                                                     size_t& offsetUnitSize)
{
    size_t bufLen = 0;
    size_t totalVarDataLen = 0;
    size_t lastVarDataLen = 0;
    for (size_t i = 0; i < attrFieldData.size(); ++i) {
        if (i < mFixLenAttributes.size() || !mEnableImpact) {
            bufLen += attrFieldData[i].first.length();
            continue;
        }

        bool needEncodeCount = mAttrRefs[i]->NeedVarLenHeader();
        if (!needEncodeCount && attrFieldData[i].second == UNKNOWN_VALUE_COUNT) {
            size_t encodeCountLen =
                VarNumAttributeFormatter::GetEncodedCountFromFirstByte(attrFieldData[i].first.data()[0]);
            assert(attrFieldData[i].first.length() >= encodeCountLen);
            lastVarDataLen = attrFieldData[i].first.length() - encodeCountLen;
        } else {
            lastVarDataLen = attrFieldData[i].first.length();
        }
        bufLen += lastVarDataLen;
        totalVarDataLen += lastVarDataLen;
    }

    if (!mEnableImpact) {
        // add MultiValueType array length for var-len attributes
        offsetUnitSize = sizeof(MultiValueOffsetType);
        bufLen += sizeof(MultiValueOffsetType) * mVarLenAttributes.size();
    } else {
        size_t maxOffset = totalVarDataLen - lastVarDataLen;
        if (maxOffset <= (size_t)std::numeric_limits<uint8_t>::max()) {
            offsetUnitSize = sizeof(uint8_t);
        } else if (maxOffset <= (size_t)std::numeric_limits<uint16_t>::max()) {
            offsetUnitSize = sizeof(uint16_t);
        } else {
            offsetUnitSize = sizeof(uint32_t);
        }
        if (mVarLenAttributes.size() > 1) {
            bufLen += sizeof(char); // store offsetUnitSize in 1 byte
            bufLen += offsetUnitSize * (mVarLenAttributes.size() - 1);
        }
    }
    return bufLen;
}

StringView PackAttributeFormatter::ReserveBuffer(const vector<AttributeFieldData>& attrFieldData, Pool* pool,
                                                 size_t& offsetUnitSize)
{
    size_t dataLen = CalculatePackDataSize(attrFieldData, offsetUnitSize);
    char* bufAddr = (char*)pool->allocate(dataLen);
    return StringView(bufAddr, dataLen);
}

AttributeReference* PackAttributeFormatter::GetAttributeReference(const string& subAttrName) const
{
    AttrNameToIdxMap::const_iterator it = mAttrNameMap.find(subAttrName);
    if (it == mAttrNameMap.end()) {
        return NULL;
    }

    assert(it->second < mAttrRefs.size());
    return mAttrRefs[it->second].get();
}

AttributeReference* PackAttributeFormatter::GetAttributeReference(attrid_t subAttrId) const
{
    int32_t idx = GetIdxByAttrId(subAttrId);
    if (idx < 0) {
        return NULL;
    }

    assert(idx < (int32_t)mAttrRefs.size());
    return mAttrRefs[idx].get();
}

void PackAttributeFormatter::ClassifySubAttributeConfigs(const vector<AttributeConfigPtr>& subAttrConfs)
{
    mFixAttrSize = 0;
    for (size_t i = 0; i < subAttrConfs.size(); i++) {
        const AttributeConfigPtr& attrConfig = subAttrConfs[i];
        if (attrConfig->IsLengthFixed()) {
            mFixLenAttributes.push_back(attrConfig);
            mFixAttrSize += attrConfig->GetFixLenFieldSize();
        } else {
            mVarLenAttributes.push_back(attrConfig);
        }
    }
}

bool PackAttributeFormatter::InitAttributeConvertors()
{
    for (size_t i = 0; i < mFixLenAttributes.size(); i++) {
        AttributeConvertorPtr convertor(
            AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(mFixLenAttributes[i]));
        if (!convertor) {
            IE_LOG(ERROR, "fail to create AttributeConvertor for Attribute: %s",
                   mFixLenAttributes[i]->GetAttrName().c_str());
            ERROR_COLLECTOR_LOG(ERROR, "fail to create AttributeConvertor for Attribute: %s",
                                mFixLenAttributes[i]->GetAttrName().c_str());
            return false;
        }
        mFixLenAttrConvertors.push_back(convertor);
    }

    for (size_t i = 0; i < mVarLenAttributes.size(); i++) {
        AttributeConvertorPtr convertor(
            AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(mVarLenAttributes[i]));
        if (!convertor) {
            IE_LOG(ERROR, "fail to create AttributeConvertor for Attribute: %s",
                   mVarLenAttributes[i]->GetAttrName().c_str());
            ERROR_COLLECTOR_LOG(ERROR, "fail to create AttributeConvertor for Attribute: %s",
                                mVarLenAttributes[i]->GetAttrName().c_str());
            return false;
        }
        mVarLenAttrConvertors.push_back(convertor);
    }

    AttributeConfigPtr packDataAttrConfig = mPackAttrConfig->CreateAttributeConfig();
    mDataConvertor.reset(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(packDataAttrConfig));
    if (!mDataConvertor) {
        IE_LOG(ERROR, "fail to create AttributeConvertor for pack attribute: %s",
               packDataAttrConfig->GetAttrName().c_str());
        ERROR_COLLECTOR_LOG(ERROR, "fail to create AttributeConvertor for pack attribute: %s",
                            packDataAttrConfig->GetAttrName().c_str());
        return false;
    }
    return true;
}

bool PackAttributeFormatter::InitSingleValueAttributeInitializers()
{
    for (size_t i = 0; i < mFixLenAttributes.size(); i++) {
        if (mFixLenAttributes[i]->IsMultiValue()) {
            // fixed length multiValue does not support default initializer
            mFixLenAttrDefaultValueInitializer.push_back(DefaultAttributeValueInitializerPtr());
            continue;
        }
        DefaultAttributeValueInitializerCreator initializerCreator(mFixLenAttributes[i]);

        DefaultAttributeValueInitializerPtr initializer(initializerCreator.Create());
        if (!initializer) {
            IE_LOG(ERROR, "fail to init AttrValueInitializer for Attribute: %s",
                   mFixLenAttributes[i]->GetAttrName().c_str());
            ERROR_COLLECTOR_LOG(ERROR, "fail to init AttrValueInitializer for Attribute: %s",
                                mFixLenAttributes[i]->GetAttrName().c_str());
            return false;
        }
        mFixLenAttrDefaultValueInitializer.push_back(initializer);
    }
    return true;
}

bool PackAttributeFormatter::InitAttributeReferences()
{
    size_t offset = 0;
    for (size_t i = 0; i < mFixLenAttributes.size(); i++) {
        mFieldIdVec.push_back(mFixLenAttributes[i]->GetFieldId());
        if (!AddNormalAttributeReference(mFixLenAttributes[i], offset)) {
            mFieldIdVec.clear();
            return false;
        }
    }
    for (size_t i = 0; i < mVarLenAttributes.size(); i++) {
        mFieldIdVec.push_back(mVarLenAttributes[i]->GetFieldId());
        bool ret = false;
        if (mEnableImpact) {
            ret = AddImpactAttributeReference(mVarLenAttributes[i], i);
        } else {
            ret = AddNormalAttributeReference(mVarLenAttributes[i], offset);
        }
        if (!ret) {
            mFieldIdVec.clear();
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
        len += VarNumAttributeFormatter::GetEncodedCountLength(patchDataLen);

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
        size_t encodeLength = VarNumAttributeFormatter::GetEncodedCountLength(patchDataLen);
        if (size_t(end - cur) < (sizeof(attrid_t) + encodeLength + patchDataLen)) {
            // buffer overflow
            return 0;
        }
        *(attrid_t*)cur = patchFields[i].first;
        cur += sizeof(attrid_t);

        VarNumAttributeFormatter::EncodeCount(patchDataLen, (char*)cur, encodeLength);
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
        size_t encodeCountLen = VarNumAttributeFormatter::GetEncodedCountFromFirstByte(*cur);
        if (size_t(end - cur) < encodeCountLen) {
            return false;
        }

        bool isNull = false;
        uint32_t dataLen = VarNumAttributeFormatter::DecodeCount((const char*)cur, encodeCountLen, isNull);
        assert(!isNull);

        cur += encodeCountLen;
        if (size_t(end - cur) < dataLen) {
            return false;
        }
        patchFields.push_back(make_pair(attrId, StringView((const char*)cur, dataLen)));
        cur += dataLen;
    }
    return cur == end;
}

bool PackAttributeFormatter::CheckLength(const StringView& packAttrField)
{
    AttrValueMeta attrMeta = mDataConvertor->Decode(packAttrField);

    MultiChar value;
    value.init(attrMeta.data.data());
    size_t size = value.size();
    if (mVarLenAttributes.empty()) {
        return mFixAttrSize == size;
    }

    size_t minLength = 0;
    if (mEnableImpact) {
        minLength = mFixAttrSize;
        if (mVarLenAttributes.size() > 1) {
            minLength += sizeof(char);
            minLength += (mVarLenAttributes.size() - 1) * sizeof(char);
        }
    } else {
        minLength = mFixAttrSize + mVarLenAttributes.size() * sizeof(MultiValueOffsetType);
    }
    // not use strict check logic : for build efficiency
    return size > minLength;
}

void PackAttributeFormatter::GetAttributesWithStoreOrder(vector<string>& attributes) const
{
    for (size_t i = 0; i < mFixLenAttributes.size(); ++i) {
        attributes.push_back(mFixLenAttributes[i]->GetAttrName());
    }
    for (size_t i = 0; i < mVarLenAttributes.size(); ++i) {
        attributes.push_back(mVarLenAttributes[i]->GetAttrName());
    }
}

PlainFormatEncoder* PackAttributeFormatter::CreatePlainFormatEncoder() const
{
    if (!mEnablePlainFormat || mVarLenAttributes.empty()) {
        return nullptr;
    }

    vector<AttributeReferencePtr> fixLenAttrRefs(mAttrRefs.begin(), mAttrRefs.begin() + mFixLenAttributes.size());
    vector<AttributeReferencePtr> varLenAttrRefs(mAttrRefs.begin() + mFixLenAttributes.size(), mAttrRefs.end());
    return new PlainFormatEncoder(mFixAttrSize, fixLenAttrRefs, varLenAttrRefs);
}

PlainFormatEncoder* PackAttributeFormatter::CreatePlainFormatEncoder(const PackAttributeConfigPtr& config)
{
    PackAttributeFormatter formatter;
    if (!formatter.Init(config)) {
        IE_LOG(WARN, "init pack attribute formatter fail.");
        return nullptr;
    }
    return formatter.CreatePlainFormatEncoder();
}

}} // namespace indexlib::common
