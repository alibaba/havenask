#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/util/slice_array/defrag_slice_array.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common/field_format/attribute/default_attribute_value_initializer_creator.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, PackAttributeFormatter);


bool PackAttributeFormatter::Init(const PackAttributeConfigPtr& packAttrConfig)
{
    assert(packAttrConfig);
    mPackAttrConfig = packAttrConfig;
    
    const vector<AttributeConfigPtr>& subAttrConfs =
        packAttrConfig->GetAttributeConfigVec();
    ClassifySubAttributeConfigs(subAttrConfs);

    if (!InitAttributeConvertors())
    {
        IE_LOG(ERROR, "init attribute convertors fail!");
        ERROR_COLLECTOR_LOG(ERROR, "init attribute convertors fail!");
        return false;
    }

    if (!InitAttributeReferences())
    {
        IE_LOG(ERROR, "init attribute references fail!");
        ERROR_COLLECTOR_LOG(ERROR, "init attribute references fail!");
        return false;
    }

    if (!InitSingleValueAttributeInitializers())
    {
        IE_LOG(ERROR, "init attribute value initializer fail!");
        ERROR_COLLECTOR_LOG(ERROR, "init attribute value initializer fail!");
        return false;
    }
    return true;   
}

bool PackAttributeFormatter::AddAttributeReference(
    const AttributeConfigPtr& attrConfig, size_t& offset)
{
    const string& attrName = attrConfig->GetAttrName();
    if (IsAttributeReferenceExist(attrName))
    {
        IE_LOG(ERROR,
               "Init pack attribute formatter failed, attribute [%s] duplicated",
               attrName.c_str());
        return false;
    }
    FieldType fieldType = attrConfig->GetFieldType();

    size_t fieldSlotLen = 0;
    if (attrConfig->IsLengthFixed())
    {
        fieldSlotLen = PackAttributeConfig::GetFixLenFieldSize(attrConfig->GetFieldConfig());
    }
    else
    {
        fieldSlotLen = sizeof(MultiValueOffsetType);
    }

    mAttrNameMap[attrName] = mAttrRefs.size(); 
    AddItemToAttrIdMap(attrConfig->GetAttrId(), mAttrRefs.size()); 
    
    switch (fieldType)
    {
    #define CASE_MACRO(ft)                                              \
        case ft:                                                        \
        {                                                               \
            typedef FieldTypeTraits<ft>::AttrItemType T;                \
            mAttrRefs.push_back(CreateAttributeReference<T>(offset, attrConfig)); \
            break;                                                      \
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

AttributeReferencePtr PackAttributeFormatter::CreateStringAttributeReference(
        size_t offset, const AttributeConfigPtr& attrConfig)
{
    CompressTypeOption compressType = attrConfig->GetFieldConfig()->GetCompressType();
    if (attrConfig->IsMultiValue())
    {
        return AttributeReferencePtr(
                new AttributeReferenceTyped<autil::MultiString>(
                    offset, attrConfig->GetAttrName(), compressType, -1));
    }

    if (attrConfig->IsLengthFixed())
    {
        return AttributeReferencePtr(
                new AttributeReferenceTyped<autil::CountedMultiChar>(
                        offset, attrConfig->GetAttrName(), compressType,
                        attrConfig->GetFieldConfig()->GetFixedMultiValueCount()));
    }
    return AttributeReferencePtr(
            new AttributeReferenceTyped<autil::MultiChar>(
                    offset, attrConfig->GetAttrName(), compressType, -1));
}

ConstString PackAttributeFormatter::Format(
        const vector<ConstString>& fields, Pool* pool)
{
    assert(mPackAttrConfig);
    assert(pool);

    assert(fields.size() == mAttrRefs.size());
    size_t cursor = 0;
    vector<ConstString> attrFieldData;
    attrFieldData.reserve(fields.size());

    // fixLenAttributes lays before varLenAttributes in both mAttrRefs[] and attrFieldData[]
    // and in the exactly same order
    for (size_t i = 0; i < mFixLenAttributes.size(); ++i)
    {
        AttributeConvertorPtr convertor = mFixLenAttrConvertors[i];
        assert(convertor);
        const auto& attrConfig = mFixLenAttributes[i];
        const ConstString& field = fields[cursor++];
        if (field.empty())
        {
            if (attrConfig->IsMultiValue())
            {
                INDEXLIB_FATAL_ERROR(InconsistentState,
                        "Fixed MultiValue field[%s] should not be empty",
                        attrConfig->GetAttrName().c_str()); 
            }
            assert(mFixLenAttrDefaultValueInitializer[i]);
            ConstString defaultValue =
                mFixLenAttrDefaultValueInitializer[i]->GetDefaultValue(pool);
            attrFieldData.push_back(convertor->Decode(defaultValue).data);
        }
        else
        {
            attrFieldData.push_back(convertor->Decode(field).data);
        }
    }
    
    for (size_t i = 0; i < mVarLenAttributes.size(); ++i)
    {
        const ConstString& field = fields[cursor++];
        AttributeConvertorPtr convertor = mVarLenAttrConvertors[i];
        assert(convertor);
        attrFieldData.push_back(convertor->Decode(field).data);
    }
    
    ConstString buf = ReserveBuffer(attrFieldData, pool);
    if (!MergePackAttributeFields(attrFieldData, buf))
    {
        return ConstString::EMPTY_STRING;
    }
    // convertor convert buf
    return mDataConvertor->Encode(buf, pool);
}

ConstString PackAttributeFormatter::GetFieldValueFromPackedValue(
        const ConstString& packValue, attrid_t attrId)
{
    AttributeReference* attrRef = GetAttributeReference(attrId);

    if (packValue.empty() || attrRef == NULL)
    {
        return ConstString::EMPTY_STRING;
    }

    AttrValueMeta attrMeta = mDataConvertor->Decode(packValue);
    MultiChar value;
    value.init(attrMeta.data.data());
    return attrRef->GetDataValue(value.data());
}

ConstString PackAttributeFormatter::MergeAndFormatUpdateFields(
    const char* baseAddr,
    const PackAttributeFields& packAttrFields,
    bool hasHashKeyInAttrFields, MemBuffer& buffer)
{
    vector<ConstString> attrFieldData;
    if (!GetMergedAttributeFieldData(
            baseAddr, packAttrFields, hasHashKeyInAttrFields, attrFieldData))
    {
        return ConstString::EMPTY_STRING;
    }

    size_t dataLen = CalculatePackDataSize(attrFieldData);
    size_t encodeCountLen = MultiValueFormatter::getEncodedCountLength(dataLen);
    if (DefragSliceArray::IsOverLength(dataLen + encodeCountLen, 
                    VAR_NUM_ATTRIBUTE_SLICE_LEN))
    {
        IE_LOG(WARN, "updated pack attribute data is overlength, ignore");
        ERROR_COLLECTOR_LOG(WARN, "updated pack attribute data is overlength, ignore");
        return ConstString::EMPTY_STRING;
    }

    // another dataLen + hashKey + count for DataConvertor memory use
    size_t buffSize = dataLen * 2 + sizeof(uint64_t) + encodeCountLen;
    buffer.Reserve(buffSize);

    ConstString mergeBuffer(buffer.GetBuffer(), dataLen);
    if (!MergePackAttributeFields(attrFieldData, mergeBuffer))
    {
        return ConstString::EMPTY_STRING;
    }
    // convertor convert buf
    return mDataConvertor->Encode(mergeBuffer, buffer.GetBuffer() + dataLen);
}

const AttributeConfigPtr& PackAttributeFormatter::GetAttributeConfig(const string& attrName) const
{
    AttrNameToIdxMap::const_iterator iter = mAttrNameMap.find(attrName);
    if (iter == mAttrNameMap.end()
        || (iter->second >= mFixLenAttributes.size() + mVarLenAttributes.size()))
    {
        static AttributeConfigPtr retNullPtr;
        return retNullPtr;
    }
    size_t idx = iter->second;
    assert(idx < mFixLenAttributes.size() + mVarLenAttributes.size());
    if (idx < mFixLenAttributes.size())
    {
        return mFixLenAttributes[idx];
    }
    return mVarLenAttributes[idx - mFixLenAttributes.size()];
}

bool PackAttributeFormatter::MergePackAttributeFields(
     const vector<ConstString>& attrFieldData,
     ConstString& mergeBuffer)
{
    size_t dataCursor = 0;
    for (size_t i = 0; i < mAttrRefs.size(); ++i)
    {
        if (i == mFixLenAttributes.size())
        {
            // all fixLen attributes have been processed, dataCursor skip the MultiValueType Array
            dataCursor += sizeof(MultiValueOffsetType) * mVarLenAttributes.size();
        }
        dataCursor += mAttrRefs[i]->SetValue(
            mergeBuffer.data(), dataCursor, attrFieldData[i]);
    }

    if (dataCursor != mergeBuffer.size())
    {
        IE_LOG(ERROR, "format pack attributes failed due to unexpected data length.");
        ERROR_COLLECTOR_LOG(ERROR, "format pack attributes failed due to unexpected data length.");
        return false;
    }
    return true;
}

bool PackAttributeFormatter::GetMergedAttributeFieldData(
    const char* baseAddr,
    const PackAttributeFields& packAttrFields,
    bool hasHashKeyInAttrFields,
    vector<ConstString>& attrFieldData)
{
    assert(baseAddr);
    assert(!packAttrFields.empty());
    assert(attrFieldData.empty());
    attrFieldData.resize(mAttrRefs.size(), ConstString::EMPTY_STRING);
    for (size_t i = 0; i < packAttrFields.size(); ++i)
    {
        assert(!packAttrFields[i].second.empty());
        int32_t idx = GetIdxByAttrId(packAttrFields[i].first);
        if (idx == -1)
        {
            IE_LOG(ERROR, "attribute [attrId:%u] not in pack attribute [%s]",
                   packAttrFields[i].first, mPackAttrConfig->GetAttrName().c_str());
            ERROR_COLLECTOR_LOG(ERROR, "attribute [attrId:%u] not in pack attribute [%s]",
                   packAttrFields[i].first, mPackAttrConfig->GetAttrName().c_str());
            return false;
        }
        if (hasHashKeyInAttrFields)
        {
            const AttributeConvertorPtr& attrConvertor =
                GetAttributeConvertorByIdx(idx);
            assert(attrConvertor);
            attrFieldData[idx] = attrConvertor->Decode(
                    packAttrFields[i].second).data;
        }
        else
        {
            attrFieldData[idx] = packAttrFields[i].second;
        }
    }

    for (size_t i = 0; i < attrFieldData.size(); ++i)
    {
        if (attrFieldData[i].empty())
        {
            attrFieldData[i] = mAttrRefs[i]->GetDataValue(baseAddr);
        }
    }
    return true;
}

size_t PackAttributeFormatter::CalculatePackDataSize(
        const vector<ConstString>& attrFieldData)
{
    size_t bufLen = 0;
    for (size_t i = 0; i < attrFieldData.size(); ++i)
    {
        bufLen += attrFieldData[i].length();
    }

    // add MultiValueType array length for var-len attributes
    bufLen += sizeof(MultiValueOffsetType) * mVarLenAttributes.size();
    return bufLen;
}

ConstString PackAttributeFormatter::ReserveBuffer(
    const vector<ConstString>& attrFieldData, Pool* pool)
{
    size_t dataLen = CalculatePackDataSize(attrFieldData);
    char* bufAddr = (char*)pool->allocate(dataLen);
    return ConstString(bufAddr, dataLen);
}

AttributeReference* PackAttributeFormatter::GetAttributeReference(
    const string& subAttrName) const
{
    AttrNameToIdxMap::const_iterator it = mAttrNameMap.find(subAttrName);
    if (it == mAttrNameMap.end())
    {
        return NULL;
    }

    assert(it->second < mAttrRefs.size());
    return mAttrRefs[it->second].get();
}

AttributeReference* PackAttributeFormatter::GetAttributeReference(
    attrid_t subAttrId) const
{
    int32_t idx = GetIdxByAttrId(subAttrId);
    if (idx < 0)
    {
        return NULL;
    }

    assert(idx < (int32_t)mAttrRefs.size());
    return mAttrRefs[idx].get();
}

void PackAttributeFormatter::ClassifySubAttributeConfigs(
    const vector<AttributeConfigPtr>& subAttrConfs)
{
    mFixAttrSize = 0;
    for (size_t i = 0; i < subAttrConfs.size(); i++)
    {
        const AttributeConfigPtr& attrConfig = subAttrConfs[i];
        if (attrConfig->IsLengthFixed())
        {
            mFixLenAttributes.push_back(attrConfig);
            mFixAttrSize += PackAttributeConfig::GetFixLenFieldSize(attrConfig->GetFieldConfig());
        }
        else
        {
            mVarLenAttributes.push_back(attrConfig);
        }
    }
}

bool PackAttributeFormatter::InitAttributeConvertors()
{
    for (size_t i = 0; i < mFixLenAttributes.size(); i++)
    {
        AttributeConvertorPtr convertor(
            AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
                mFixLenAttributes[i]->GetFieldConfig()));
        if (!convertor)
        {
            IE_LOG(ERROR, "fail to create AttributeConvertor for Attribute: %s",
                   mFixLenAttributes[i]->GetAttrName().c_str());
            ERROR_COLLECTOR_LOG(ERROR, "fail to create AttributeConvertor for Attribute: %s",
                   mFixLenAttributes[i]->GetAttrName().c_str());
            return false;
        }
        mFixLenAttrConvertors.push_back(convertor);
    }
    
    for (size_t i = 0; i < mVarLenAttributes.size(); i++)
    {
        AttributeConvertorPtr convertor(
            AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
                mVarLenAttributes[i]->GetFieldConfig()));
        if (!convertor)
        {
            IE_LOG(ERROR, "fail to create AttributeConvertor for Attribute: %s",
                   mVarLenAttributes[i]->GetAttrName().c_str());
            ERROR_COLLECTOR_LOG(ERROR, "fail to create AttributeConvertor for Attribute: %s",
                    mVarLenAttributes[i]->GetAttrName().c_str());
            return false;
        }
        mVarLenAttrConvertors.push_back(convertor);
    }

    AttributeConfigPtr packDataAttrConfig =
        mPackAttrConfig->CreateAttributeConfig();
    mDataConvertor.reset(
        AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
            packDataAttrConfig->GetFieldConfig()));
    if (!mDataConvertor)
    {
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
    for (size_t i = 0; i < mFixLenAttributes.size(); i++)
    {
        if (mFixLenAttributes[i]->IsMultiValue())
        {
            // fixed length multiValue does not support default initializer
            mFixLenAttrDefaultValueInitializer.push_back(DefaultAttributeValueInitializerPtr());
            continue;
        }
        DefaultAttributeValueInitializerCreator initializerCreator(
            mFixLenAttributes[i]->GetFieldConfig());
            
        DefaultAttributeValueInitializerPtr initializer(
            initializerCreator.Create());
        if (!initializer)
        {
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
    for (size_t i = 0; i < mFixLenAttributes.size(); i++)
    {
        mFieldIdVec.push_back(mFixLenAttributes[i]->GetFieldId());
        if (!AddAttributeReference(mFixLenAttributes[i], offset))
        {
            mFieldIdVec.clear();
            return false;
        }
    }
    for (size_t i = 0; i < mVarLenAttributes.size(); i++)
    {
        mFieldIdVec.push_back(mVarLenAttributes[i]->GetFieldId());
        if (!AddAttributeReference(mVarLenAttributes[i], offset))
        {
            mFieldIdVec.clear();
            return false;
        }
    }
    return true;
}

size_t PackAttributeFormatter::GetEncodePatchValueLen(
        const PackAttributeFields& patchFields)
{
    if (patchFields.empty())
    {
        return 0;
    }
    size_t len = sizeof(uint32_t); // count
    for (size_t i = 0; i < patchFields.size(); ++i)
    {
        size_t patchDataLen = patchFields[i].second.size();
        len += MultiValueFormatter::getEncodedCountLength(patchDataLen);

        len += sizeof(attrid_t);
        len += patchDataLen;
    }
    return len;
}

size_t PackAttributeFormatter::EncodePatchValues(
        const PackAttributeFields& patchFields,
        uint8_t* buffer, size_t bufferLen)
{
    if (!buffer || patchFields.empty())
    {
        return 0;
    }
    uint8_t* end = buffer + bufferLen;
    uint8_t* cur = buffer;
    *(uint32_t*)cur = patchFields.size();
    cur += sizeof(uint32_t);
    
    for (size_t i = 0; i < patchFields.size(); ++i)
    {
        size_t patchDataLen = patchFields[i].second.size();
        size_t encodeLength = 
            MultiValueFormatter::getEncodedCountLength(patchDataLen);
        if (size_t(end - cur) < (sizeof(attrid_t) + encodeLength + patchDataLen))
        {
            // buffer overflow
            return 0;
        }
        *(attrid_t*)cur = patchFields[i].first;
        cur += sizeof(attrid_t);

        MultiValueFormatter::encodeCount(patchDataLen, 
                (char*)cur, encodeLength);
        cur += encodeLength;
        memcpy(cur, patchFields[i].second.data(), patchDataLen);
        cur += patchDataLen;
    }
    return cur - buffer;
}
    
bool PackAttributeFormatter::DecodePatchValues(
    uint8_t* buffer, size_t bufferLen, PackAttributeFields& patchFields)
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
    
    for (size_t i = 0; i < fieldCount; ++i)
    {
        // 1 byte for first byte of count
        if (size_t(end - cur) < sizeof(attrid_t) + sizeof(uint8_t))
        {
            return false;
        }
        attrid_t attrId = *(attrid_t*)cur;
        cur += sizeof(attrid_t);
        size_t encodeCountLen = 
            MultiValueFormatter::getEncodedCountFromFirstByte(*cur);
        if (size_t(end - cur) < encodeCountLen)
        {
            return false;
        }

        uint32_t dataLen = MultiValueFormatter::decodeCount(
                (const char*)cur, encodeCountLen);
        cur += encodeCountLen;
        if (size_t(end - cur) < dataLen)
        {
            return false;
        }
        patchFields.push_back(
            make_pair(attrId, ConstString((const char*)cur, dataLen)));
        cur += dataLen;
    }
    return cur == end;
}


bool PackAttributeFormatter::CheckLength(const ConstString& packAttrField)
{
    AttrValueMeta attrMeta = mDataConvertor->Decode(packAttrField);

    MultiChar value;
    value.init(attrMeta.data.data());
    size_t size = value.size();
    if (mVarLenAttributes.empty())
    {
        return mFixAttrSize == size;
    }
    
    size_t minLength = mFixAttrSize +
                       mVarLenAttributes.size() * sizeof(MultiValueOffsetType);
    // not use strict check logic : for build efficiency
    return size > minLength;
}

IE_NAMESPACE_END(common);

