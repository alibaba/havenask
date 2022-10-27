#ifndef __INDEXLIB_PACK_ATTRIBUTE_FORMATTER_H
#define __INDEXLIB_PACK_ATTRIBUTE_FORMATTER_H

#include <tr1/memory>
#include <autil/MultiValueType.h>
#include <autil/mem_pool/Pool.h>
#include <autil/ConstString.h>
#include <tr1/unordered_map>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/config/field_config.h"
#include "indexlib/common/field_format/pack_attribute/attribute_reference_typed.h"
#include "indexlib/common/field_format/attribute/default_attribute_value_initializer.h"
#include "indexlib/util/mem_buffer.h"

DECLARE_REFERENCE_CLASS(common, AttributeConvertor);
DECLARE_REFERENCE_CLASS(partition, PackAttributeAppender);

IE_NAMESPACE_BEGIN(common);

class PackAttributeFormatter
{
public:
    typedef std::pair<attrid_t, autil::ConstString> PackAttributeField;
    typedef std::vector<PackAttributeField> PackAttributeFields;

    typedef std::tr1::unordered_map<std::string, size_t> AttrNameToIdxMap;

    typedef std::vector<int32_t> AttrIdToIdxMap;
    typedef std::vector<fieldid_t> FieldIdVec;
        
public:
    PackAttributeFormatter()
        : mFixAttrSize(0)
    {}
    
    ~PackAttributeFormatter() {}
    
public:
    bool Init(const config::PackAttributeConfigPtr& packAttrConfig);
    AttributeReference* GetAttributeReference(const std::string& subAttrName) const;
    AttributeReference* GetAttributeReference(attrid_t subAttrId) const;

    autil::ConstString GetFieldValueFromPackedValue(
            const autil::ConstString& packValue, attrid_t attrId);

    template <typename T>
    AttributeReferenceTyped<T>* GetAttributeReferenceTyped(
            const std::string& subAttrName) const;

    template <typename T>
    AttributeReferenceTyped<T>* GetAttributeReferenceTyped(
            attrid_t subAttrId) const;

    const FieldIdVec& GetFieldIds() const { return mFieldIdVec; }
    
    autil::ConstString Format(
            const std::vector<autil::ConstString>& attrFieldData,
            autil::mem_pool::Pool* pool);

    autil::ConstString MergeAndFormatUpdateFields(
        const char* baseAddr,
        const PackAttributeFields& packAttrFields,
        bool hasHashKeyInAttrFields,
        util::MemBuffer& buffer);

    const config::AttributeConfigPtr& GetAttributeConfig(const std::string& attrName) const;

    static size_t EncodePatchValues(const PackAttributeFields& patchFields,
                                    uint8_t* buffer, size_t bufferLen);

    static bool DecodePatchValues(uint8_t* buffer, size_t bufferLen,
                                  PackAttributeFields& patchFields);

    static size_t GetEncodePatchValueLen(const PackAttributeFields& patchFields);

    AttributeConvertorPtr GetAttributeConvertorByAttrId(attrid_t attrId) const
    {
        int32_t idx = GetIdxByAttrId(attrId);
        if (idx == -1)
        {
            return AttributeConvertorPtr();
        }
        return GetAttributeConvertorByIdx(idx);
    }

    bool CheckLength(const autil::ConstString& packAttrField);
    
private:
    bool GetMergedAttributeFieldData(
        const char* baseAddr,
        const PackAttributeFields& packAttrFields,
        bool hasHashKeyInAttrFields,
        std::vector<autil::ConstString>& attrFieldData);
    
    void ClassifySubAttributeConfigs(
            const std::vector<config::AttributeConfigPtr>& subAttrConfs);

    bool InitAttributeConvertors();
    bool InitAttributeReferences();
    bool InitSingleValueAttributeInitializers();
    
    bool AddAttributeReference(
        const config::AttributeConfigPtr& attrConfig, size_t& offset);

    bool IsAttributeReferenceExist(const std::string& attrName) const;

    template <typename T>
    AttributeReferencePtr CreateAttributeReference(
            size_t offset, const config::AttributeConfigPtr& attrConfig);

    AttributeReferencePtr CreateStringAttributeReference(
            size_t offset, const config::AttributeConfigPtr& attrConfig);

    size_t CalculatePackDataSize(
        const std::vector<autil::ConstString>& attrFieldData);
    
    autil::ConstString ReserveBuffer(
        const std::vector<autil::ConstString>& attrFieldData,
        autil::mem_pool::Pool* pool);

    bool MergePackAttributeFields(
        const std::vector<autil::ConstString>& attrFieldData,
        autil::ConstString& mergeBuffer);
    
    void AddItemToAttrIdMap(attrid_t attrId, int32_t idx)
    {
        assert(idx >= 0);
        if (attrId >= mAttrIdMap.size())
        {
            mAttrIdMap.resize(attrId + 1, -1);
        }
        mAttrIdMap[attrId] = idx;
    }

    inline int32_t GetIdxByAttrId(attrid_t attrId) const
    {
        if (attrId >= (attrid_t)mAttrIdMap.size())
        {
            return -1;
        }
        return mAttrIdMap[attrId];
    }

    const common::AttributeConvertorPtr& GetAttributeConvertorByIdx(int32_t idx) const
    {
        assert(idx >= 0 && idx < (int32_t)mAttrRefs.size());
        if (idx >= (int32_t)mFixLenAttributes.size())
        {
            return mVarLenAttrConvertors[idx - mFixLenAttributes.size()];
        }
        return mFixLenAttrConvertors[idx];
    }

private:
    typedef int64_t MultiValueOffsetType;
private:
    size_t mFixAttrSize;
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
inline AttributeReferencePtr PackAttributeFormatter::CreateAttributeReference(
        size_t offset, const config::AttributeConfigPtr& attrConfig)        
{
    config::CompressTypeOption compressType = attrConfig->GetFieldConfig()->GetCompressType();
    if (!attrConfig->IsMultiValue())
    {
        return AttributeReferencePtr(
            new AttributeReferenceTyped<T>(
                offset, attrConfig->GetAttrName(), compressType, -1)); 
    }

    if (attrConfig->IsLengthFixed())
    {
        return AttributeReferencePtr(
                new AttributeReferenceTyped<autil::CountedMultiValueType<T> >(
                    offset, attrConfig->GetAttrName(), compressType,
                    attrConfig->GetFieldConfig()->GetFixedMultiValueCount())); 
    }
    return AttributeReferencePtr(
            new AttributeReferenceTyped<autil::MultiValueType<T> >(
                    offset, attrConfig->GetAttrName(), compressType, -1));
}

template <typename T>
inline AttributeReferenceTyped<T>* PackAttributeFormatter::GetAttributeReferenceTyped(
    const std::string& subAttrName) const
{
    AttributeReference* attrRef = GetAttributeReference(subAttrName);
    if (!attrRef)
    {
        return NULL;
    }
    return dynamic_cast<AttributeReferenceTyped<T>* >(attrRef);
}

template <typename T>
AttributeReferenceTyped<T>* PackAttributeFormatter::GetAttributeReferenceTyped(
    attrid_t subAttrId) const
{
    AttributeReference* attrRef = GetAttributeReference(subAttrId);
    if (!attrRef)
    {
        return NULL;
    }
    return dynamic_cast<AttributeReferenceTyped<T>* >(attrRef);
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_PACK_ATTRIBUTE_FORMATTER_H
