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
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/DefaultAttributeValueInitializer.h"
#include "indexlib/index/common/field_format/pack_attribute/AttributeReferenceTyped.h"
#include "indexlib/index/common/field_format/pack_attribute/PlainFormatEncoder.h"
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "indexlib/util/MemBuffer.h"

namespace indexlibv2::index {
class AttributeConvertor;
class PackAttributeFormatter
{
public:
    using ReferenceOffset = autil::PackOffset;
    using PackAttributeField = std::pair<attrid_t, autil::StringView>;
    using PackAttributeFields = std::vector<PackAttributeField>;
    using AttrNameToIdx = std::unordered_map<std::string, size_t>;
    using AttrIdToIdx = std::vector<int32_t>;
    using FieldIdVec = std::vector<fieldid_t>;
    using AttributeFieldData = std::pair<autil::StringView, int32_t>;

    // second: value count, default -1 means data value with count; >= 0 mean data value without count
    static constexpr int32_t UNKNOWN_VALUE_COUNT = (int32_t)-1;

public:
    PackAttributeFormatter() : _fixAttrSize(0), _enableImpact(false), _enablePlainFormat(false) {}

    ~PackAttributeFormatter() = default;

public:
    bool Init(const std::shared_ptr<indexlibv2::index::PackAttributeConfig>& packAttrConfig);
    const std::vector<std::shared_ptr<AttributeReference>>& GetAttributeReferences() const { return _attrRefs; }
    AttributeReference* GetAttributeReference(const std::string& subAttrName) const;
    AttributeReference* GetAttributeReference(attrid_t subAttrId) const;

    AttributeReference::DataValue GetFieldValueFromPackedValue(const autil::StringView& packValue, attrid_t attrId);

    std::string GetFieldStringValueFromPackedValue(const autil::StringView& packValue, attrid_t attrId,
                                                   autil::mem_pool::Pool* pool);

    template <typename T>
    AttributeReferenceTyped<T>* GetAttributeReferenceTyped(const std::string& subAttrName) const;

    template <typename T>
    AttributeReferenceTyped<T>* GetAttributeReferenceTyped(attrid_t subAttrId) const;

    const FieldIdVec& GetFieldIds() const { return _fieldIdVec; }

    autil::StringView Format(const std::vector<autil::StringView>& attrFieldData, autil::mem_pool::Pool* pool);

    autil::StringView FormatAttrField(const std::vector<autil::StringView>& attrFieldData, autil::mem_pool::Pool* pool);

    autil::StringView MergeAndFormatUpdateFields(const char* baseAddr, const PackAttributeFields& packAttrFields,
                                                 bool hasHashKeyInAttrFields, indexlib::util::MemBuffer& buffer);

    const std::shared_ptr<index::AttributeConfig>& GetAttributeConfig(const std::string& attrName) const;

    static size_t EncodePatchValues(const PackAttributeFields& patchFields, uint8_t* buffer, size_t bufferLen);

    static bool DecodePatchValues(uint8_t* buffer, size_t bufferLen, PackAttributeFields& patchFields);

    static size_t GetEncodePatchValueLen(const PackAttributeFields& patchFields);

    static PlainFormatEncoder*
    CreatePlainFormatEncoder(const std::shared_ptr<indexlibv2::index::PackAttributeConfig>& config);

    std::shared_ptr<AttributeConvertor> GetAttributeConvertorByAttrId(attrid_t attrId) const
    {
        int32_t idx = GetIdxByAttrId(attrId);
        if (idx == -1) {
            return nullptr;
        }
        return GetAttributeConvertorByIdx(idx);
    }

    bool CheckLength(const autil::StringView& packAttrField);

    const std::shared_ptr<indexlibv2::index::PackAttributeConfig>& GetPackAttributeConfig() const
    {
        return _packAttrConfig;
    }

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

    void ClassifySubAttributeConfigs(const std::vector<std::shared_ptr<index::AttributeConfig>>& subAttrConfs);

    bool InitAttributeConvertors();
    bool InitAttributeReferences();
    bool InitSingleValueAttributeInitializers();

    bool AddNormalAttributeReference(const std::shared_ptr<index::AttributeConfig>& attrConfig, size_t& offset);
    bool AddImpactAttributeReference(const std::shared_ptr<index::AttributeConfig>& attrConfig, size_t varIdx);

    bool IsAttributeReferenceExist(const std::string& attrName) const;

    template <typename T>
    std::shared_ptr<AttributeReference>
    CreateAttributeReference(size_t offset, const std::shared_ptr<index::AttributeConfig>& attrConfig);

    std::shared_ptr<AttributeReference>
    CreateStringAttributeReference(size_t offset, const std::shared_ptr<index::AttributeConfig>& attrConfig);

    size_t CalculatePackDataSize(const std::vector<AttributeFieldData>& attrFieldData, size_t& offsetUnitSize);

    void AddItemToAttrIdMap(attrid_t attrId, int32_t idx)
    {
        assert(idx >= 0);
        if (attrId >= _attrId2Idx.size()) {
            _attrId2Idx.resize(attrId + 1, -1);
        }
        _attrId2Idx[attrId] = idx;
    }

    inline int32_t GetIdxByAttrId(attrid_t attrId) const
    {
        if (attrId >= (attrid_t)_attrId2Idx.size()) {
            return -1;
        }
        return _attrId2Idx[attrId];
    }

    const std::shared_ptr<AttributeConvertor>& GetAttributeConvertorByIdx(int32_t idx) const
    {
        assert(idx >= 0 && idx < (int32_t)_attrRefs.size());
        if (idx >= (int32_t)_fixLenAttributes.size()) {
            return _varLenAttributeConvertors[idx - _fixLenAttributes.size()];
        }
        return _fixLenAttrConvertors[idx];
    }

private:
    typedef int64_t MultiValueOffsetType;

private:
    size_t _fixAttrSize;
    bool _enableImpact;
    bool _enablePlainFormat;
    std::shared_ptr<indexlibv2::index::PackAttributeConfig> _packAttrConfig;
    AttrNameToIdx _attrName2Idx;
    AttrIdToIdx _attrId2Idx;
    std::vector<std::shared_ptr<AttributeReference>> _attrRefs;
    std::vector<std::shared_ptr<index::AttributeConfig>> _fixLenAttributes;
    std::vector<std::shared_ptr<index::AttributeConfig>> _varLenAttributes;

    std::vector<std::shared_ptr<AttributeConvertor>> _fixLenAttrConvertors;
    std::vector<std::shared_ptr<AttributeConvertor>> _varLenAttributeConvertors;
    std::vector<std::shared_ptr<DefaultAttributeValueInitializer>> _fixLenAttrDefaultValueInitializer;
    std::shared_ptr<AttributeConvertor> _dataConvertor;
    FieldIdVec _fieldIdVec;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
inline std::shared_ptr<AttributeReference>
PackAttributeFormatter::CreateAttributeReference(size_t offset,
                                                 const std::shared_ptr<index::AttributeConfig>& attrConfig)
{
    const auto& attrName = attrConfig->GetAttrName();
    indexlib::config::CompressTypeOption compressType = attrConfig->GetCompressType();
    if (!attrConfig->IsMultiValue()) {
        return std::make_shared<AttributeReferenceTyped<T>>(ReferenceOffset::normalOffset(offset, false), attrName,
                                                            compressType, -1);
    }

    auto fixedValueCount = attrConfig->GetFixedMultiValueCount();
    const auto& separator = attrConfig->GetFieldConfig()->GetSeparator();
    if (attrConfig->IsLengthFixed()) {
        return std::make_shared<AttributeReferenceTyped<autil::MultiValueType<T>>>(
            ReferenceOffset::normalOffset(offset, false), attrName, compressType, fixedValueCount, separator);
    }
    return std::make_shared<AttributeReferenceTyped<autil::MultiValueType<T>>>(
        ReferenceOffset::normalOffset(offset, true), attrName, compressType, -1, separator);
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
    auto iter = _attrName2Idx.find(attrName);
    if (iter != _attrName2Idx.end()) {
        idx = iter->second;
        return true;
    }
    return false;
}

} // namespace indexlibv2::index
