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
#pragma once

#include <memory>
#include <unordered_map>

#include "autil/ConstString.h"
#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common/field_format/attribute/default_attribute_value_initializer.h"
#include "indexlib/common/field_format/pack_attribute/attribute_reference_typed.h"
#include "indexlib/common/field_format/pack_attribute/plain_format_encoder.h"
#include "indexlib/common_define.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/MemBuffer.h"

DECLARE_REFERENCE_CLASS(common, AttributeConvertor);
DECLARE_REFERENCE_CLASS(partition, PackAttributeAppender);

namespace indexlib { namespace common {

class PackAttributeFormatter
{
public:
    typedef autil::PackOffset ReferenceOffset;
    typedef std::pair<attrid_t, autil::StringView> PackAttributeField;
    typedef std::vector<PackAttributeField> PackAttributeFields;

    typedef std::unordered_map<std::string, size_t> AttrNameToIdxMap;

    typedef std::vector<int32_t> AttrIdToIdxMap;
    typedef std::vector<fieldid_t> FieldIdVec;

    // second: value count, default -1 means data value with count; >= 0 mean data value without count
    static constexpr int32_t UNKNOWN_VALUE_COUNT = (int32_t)-1;
    typedef std::pair<autil::StringView, int32_t> AttributeFieldData;

public:
    PackAttributeFormatter() : mFixAttrSize(0), mEnableImpact(false), mEnablePlainFormat(false) {}

    ~PackAttributeFormatter() {}

public:
    bool Init(const config::PackAttributeConfigPtr& packAttrConfig);
    const std::vector<AttributeReferencePtr>& GetAttributeReferences() const { return mAttrRefs; }
    AttributeReference* GetAttributeReference(const std::string& subAttrName) const;
    AttributeReference* GetAttributeReference(attrid_t subAttrId) const;

    AttributeReference::DataValue GetFieldValueFromPackedValue(const autil::StringView& packValue, attrid_t attrId);

    std::string GetFieldStringValueFromPackedValue(const autil::StringView& packValue, attrid_t attrId,
                                                   autil::mem_pool::Pool* pool);

    template <typename T>
    AttributeReferenceTyped<T>* GetAttributeReferenceTyped(const std::string& subAttrName) const;

    template <typename T>
    AttributeReferenceTyped<T>* GetAttributeReferenceTyped(attrid_t subAttrId) const;

    const FieldIdVec& GetFieldIds() const { return mFieldIdVec; }

    autil::StringView Format(const std::vector<autil::StringView>& attrFieldData, autil::mem_pool::Pool* pool);

    autil::StringView MergeAndFormatUpdateFields(const char* baseAddr, const PackAttributeFields& packAttrFields,
                                                 bool hasHashKeyInAttrFields, util::MemBuffer& buffer);

    const config::AttributeConfigPtr& GetAttributeConfig(const std::string& attrName) const;

    static size_t EncodePatchValues(const PackAttributeFields& patchFields, uint8_t* buffer, size_t bufferLen);

    static bool DecodePatchValues(uint8_t* buffer, size_t bufferLen, PackAttributeFields& patchFields);

    static size_t GetEncodePatchValueLen(const PackAttributeFields& patchFields);

    static PlainFormatEncoder* CreatePlainFormatEncoder(const config::PackAttributeConfigPtr& config);

    AttributeConvertorPtr GetAttributeConvertorByAttrId(attrid_t attrId) const
    {
        int32_t idx = GetIdxByAttrId(attrId);
        if (idx == -1) {
            return AttributeConvertorPtr();
        }
        return GetAttributeConvertorByIdx(idx);
    }

    bool CheckLength(const autil::StringView& packAttrField);

    const config::PackAttributeConfigPtr& GetPackAttributeConfig() const { return mPackAttrConfig; }

    void GetAttributesWithStoreOrder(std::vector<std::string>& attributes) const;

    PlainFormatEncoder* CreatePlainFormatEncoder() const;

    bool GetAttributeStoreIndex(const std::string& attrName, size_t& idx) const;

public:
    // public for remote_indexlib
    autil::StringView ReserveBuffer(const std::vector<AttributeFieldData>& attrFieldData, autil::mem_pool::Pool* pool,
                                    size_t& offsetUnitSize);

    bool MergePackAttributeFields(const std::vector<AttributeFieldData>& attrFieldData, size_t offsetUnitSize,
                                  autil::StringView& mergeBuffer);

private:
    bool GetMergedAttributeFieldData(const char* baseAddr, const PackAttributeFields& packAttrFields,
                                     bool hasHashKeyInAttrFields, std::vector<AttributeFieldData>& attrFieldData);

    void ClassifySubAttributeConfigs(const std::vector<config::AttributeConfigPtr>& subAttrConfs);

    bool InitAttributeConvertors();
    bool InitAttributeReferences();
    bool InitSingleValueAttributeInitializers();

    bool AddNormalAttributeReference(const config::AttributeConfigPtr& attrConfig, size_t& offset);
    bool AddImpactAttributeReference(const config::AttributeConfigPtr& attrConfig, size_t varIdx);

    bool IsAttributeReferenceExist(const std::string& attrName) const;

    template <typename T>
    AttributeReferencePtr CreateAttributeReference(size_t offset, const config::AttributeConfigPtr& attrConfig);

    AttributeReferencePtr CreateStringAttributeReference(size_t offset, const config::AttributeConfigPtr& attrConfig);

    size_t CalculatePackDataSize(const std::vector<AttributeFieldData>& attrFieldData, size_t& offsetUnitSize);

    void AddItemToAttrIdMap(attrid_t attrId, int32_t idx)
    {
        assert(idx >= 0);
        if (attrId >= mAttrIdMap.size()) {
            mAttrIdMap.resize(attrId + 1, -1);
        }
        mAttrIdMap[attrId] = idx;
    }

    inline int32_t GetIdxByAttrId(attrid_t attrId) const
    {
        if (attrId >= (attrid_t)mAttrIdMap.size()) {
            return -1;
        }
        return mAttrIdMap[attrId];
    }

    const common::AttributeConvertorPtr& GetAttributeConvertorByIdx(int32_t idx) const
    {
        assert(idx >= 0 && idx < (int32_t)mAttrRefs.size());
        if (idx >= (int32_t)mFixLenAttributes.size()) {
            return mVarLenAttrConvertors[idx - mFixLenAttributes.size()];
        }
        return mFixLenAttrConvertors[idx];
    }

private:
    typedef int64_t MultiValueOffsetType;

private:
    size_t mFixAttrSize;
    bool mEnableImpact;
    bool mEnablePlainFormat;
    config::PackAttributeConfigPtr mPackAttrConfig;
    AttrNameToIdxMap mAttrNameMap;
    AttrIdToIdxMap mAttrIdMap;
    std::vector<AttributeReferencePtr> mAttrRefs;
    std::vector<config::AttributeConfigPtr> mFixLenAttributes;
    std::vector<config::AttributeConfigPtr> mVarLenAttributes;

    std::vector<AttributeConvertorPtr> mFixLenAttrConvertors;
    std::vector<AttributeConvertorPtr> mVarLenAttrConvertors;
    std::vector<DefaultAttributeValueInitializerPtr> mFixLenAttrDefaultValueInitializer;
    AttributeConvertorPtr mDataConvertor;
    FieldIdVec mFieldIdVec;

private:
    friend class partition::PackAttributeAppender;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackAttributeFormatter);

template <typename T>
inline AttributeReferencePtr
PackAttributeFormatter::CreateAttributeReference(size_t offset, const config::AttributeConfigPtr& attrConfig)
{
    const auto& attrName = attrConfig->GetAttrName();
    config::CompressTypeOption compressType = attrConfig->GetCompressType();
    if (!attrConfig->IsMultiValue()) {
        return AttributeReferencePtr(
            new AttributeReferenceTyped<T>(ReferenceOffset::normalOffset(offset, false), attrName, compressType, -1));
    }

    auto fixedValueCount = attrConfig->GetFieldConfig()->GetFixedMultiValueCount();
    const auto& separator = attrConfig->GetFieldConfig()->GetSeparator();
    if (attrConfig->IsLengthFixed()) {
        return AttributeReferencePtr(new AttributeReferenceTyped<autil::MultiValueType<T>>(
            ReferenceOffset::normalOffset(offset, false), attrName, compressType, fixedValueCount, separator));
    }
    return AttributeReferencePtr(new AttributeReferenceTyped<autil::MultiValueType<T>>(
        ReferenceOffset::normalOffset(offset, true), attrName, compressType, -1, separator));
}

template <typename T>
inline AttributeReferenceTyped<T>*
PackAttributeFormatter::GetAttributeReferenceTyped(const std::string& subAttrName) const
{
    AttributeReference* attrRef = GetAttributeReference(subAttrName);
    if (!attrRef) {
        return NULL;
    }
    return dynamic_cast<AttributeReferenceTyped<T>*>(attrRef);
}

template <typename T>
AttributeReferenceTyped<T>* PackAttributeFormatter::GetAttributeReferenceTyped(attrid_t subAttrId) const
{
    AttributeReference* attrRef = GetAttributeReference(subAttrId);
    if (!attrRef) {
        return NULL;
    }
    return dynamic_cast<AttributeReferenceTyped<T>*>(attrRef);
}

inline bool PackAttributeFormatter::GetAttributeStoreIndex(const std::string& attrName, size_t& idx) const
{
    auto iter = mAttrNameMap.find(attrName);
    if (iter != mAttrNameMap.end()) {
        idx = iter->second;
        return true;
    }
    return false;
}

}} // namespace indexlib::common
