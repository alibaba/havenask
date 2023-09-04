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
#include "indexlib/index/pack_attribute/PackAttributeReader.h"

#include "indexlib/index/pack_attribute/PackAttributeIteratorTyped.h"
#include "indexlib/index/pack_attribute/PackAttributeMetrics.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PackAttributeReader);

PackAttributeReader::PackAttributeReader(const std::shared_ptr<PackAttributeMetrics>& packAttributeMetrics)
    : _packAttributeMetrics(packAttributeMetrics)
{
}

PackAttributeReader::~PackAttributeReader() {}

Status PackAttributeReader::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                 const framework::TabletData* tabletData)
{
    _packAttributeConfig = std::dynamic_pointer_cast<PackAttributeConfig>(indexConfig);
    _packAttributeFormatter = std::make_unique<PackAttributeFormatter>();
    if (!_packAttributeFormatter->Init(_packAttributeConfig)) {
        auto status = Status::InternalError("open pack attribute reader for pack attribute: %s failed.",
                                            _packAttributeConfig->GetPackName().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    const auto& attrConfigs = _packAttributeConfig->GetAttributeConfigVec();
    for (const auto& attrConfig : attrConfigs) {
        const auto& [fieldPrinter, success] = _fieldPrinters.try_emplace(attrConfig->GetIndexName());
        assert(success);
        fieldPrinter->second.Init(attrConfig->GetFieldConfig());
    }
    _dataReader = std::make_unique<MultiValueAttributeReader<char>>(config::SortPattern::sp_nosort);
    auto attributeConfig = _packAttributeConfig->CreateAttributeConfig();
    if (attributeConfig == nullptr) {
        AUTIL_LOG(ERROR, "attr config is nullptr, index name [%s]", indexConfig->GetIndexName().c_str());
        assert(false);
        return Status::InternalError();
    }
    if (_packAttributeMetrics) {
        _packAttributeMetrics->RegisterAttributeAccess(_packAttributeConfig);
    }
    return _dataReader->Open(attributeConfig, tabletData);
    // mDataConvertor.reset(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    // if (!mDataConvertor) {
    //     IE_LOG(ERROR,
    //            "fail to create AttributeConvertor"
    //            " for pack attribute: %s",
    //            attrConfig->GetAttrName().c_str());
    //     return false;
    // }
    // InitBuffer();
    // bool ret = mDataReader->Open(attrConfig, partitionData, PatchApplyStrategy::PAS_APPLY_NO_PATCH,
    //                              dynamic_cast<const AttributeReader*>(GET_IF_NOT_NULL(hintReader,
    //                              mDataReader.get())));
}

// void PackAttributeReader::InitBuildResourceMetricsNode(BuildResourceMetrics* buildResourceMetrics)
// {
//     if (buildResourceMetrics) {
//         mBuildResourceMetricsNode = buildResourceMetrics->AllocateNode();
//         IE_LOG(INFO, "allocate build resource node [id:%d] for pack attribute reader[%s]",
//                mBuildResourceMetricsNode->GetNodeId(), mPackAttrConfig->GetPackName().c_str());
//         mBuildResourceMetricsNode->Update(BMT_CURRENT_MEMORY_USE, mSimplePool.getUsedBytes());
//     }
// }

void PackAttributeReader::IncreaseAccessCounter(const AttributeReference* attributeRef) const
{
    if (attributeRef && _packAttributeMetrics) {
        _packAttributeMetrics->IncAttributeAccess(attributeRef->GetAttrName());
    }
}

AttributeReference* PackAttributeReader::GetSubAttributeReference(const std::string& subAttrName) const
{
    if (!_packAttributeFormatter) {
        return nullptr;
    }
    AttributeReference* attributeRef = _packAttributeFormatter->GetAttributeReference(subAttrName);
    IncreaseAccessCounter(attributeRef);
    return attributeRef;
}

AttributeReference* PackAttributeReader::GetSubAttributeReference(attrid_t subAttrId) const
{
    if (!_packAttributeFormatter) {
        return nullptr;
    }
    AttributeReference* attributeRef = _packAttributeFormatter->GetAttributeReference(subAttrId);
    IncreaseAccessCounter(attributeRef);
    return attributeRef;
}

PackAttributeIterator* PackAttributeReader::CreateIterator(autil::mem_pool::Pool* pool)
{
    AttributeIteratorBase* iter = _dataReader->CreateIterator(pool);
    if (!iter) {
        return nullptr;
    }
    AttributeIteratorTyped<autil::MultiChar>* iterTyped = dynamic_cast<AttributeIteratorTyped<autil::MultiChar>*>(iter);
    assert(iterTyped);
    return IE_POOL_COMPATIBLE_NEW_CLASS(pool, PackAttributeIterator, iterTyped, pool);
}

AttributeIteratorBase* PackAttributeReader::CreateIterator(autil::mem_pool::Pool* pool,
                                                           const std::string& subAttrName) const
{
    const auto& attrConfig = _packAttributeFormatter->GetAttributeConfig(subAttrName);
    if (!attrConfig) {
        return nullptr;
    }
    bool isMulti = attrConfig->IsMultiValue();
    switch (attrConfig->GetFieldType()) {
    case ft_int8:
        return CreateIteratorTyped<int8_t>(pool, subAttrName, isMulti);
    case ft_uint8:
        return CreateIteratorTyped<uint8_t>(pool, subAttrName, isMulti);
    case ft_int16:
        return CreateIteratorTyped<int16_t>(pool, subAttrName, isMulti);
    case ft_uint16:
        return CreateIteratorTyped<uint16_t>(pool, subAttrName, isMulti);
    case ft_int32:
        return CreateIteratorTyped<int32_t>(pool, subAttrName, isMulti);
    case ft_uint32:
        return CreateIteratorTyped<uint32_t>(pool, subAttrName, isMulti);
    case ft_int64:
        return CreateIteratorTyped<int64_t>(pool, subAttrName, isMulti);
    case ft_uint64:
    case ft_hash_64: // uint64:  only used for primary key
        return CreateIteratorTyped<uint64_t>(pool, subAttrName, isMulti);
    case ft_float:
        return CreateIteratorTyped<float>(pool, subAttrName, isMulti);
    case ft_fp8:
        assert(false);
        return CreateIteratorTyped<int8_t>(pool, subAttrName, isMulti);
    case ft_fp16:
        assert(false);
        return CreateIteratorTyped<int16_t>(pool, subAttrName, isMulti);
    case ft_double:
        return CreateIteratorTyped<double>(pool, subAttrName, isMulti);
    case ft_string:
        return CreateIteratorTyped<autil::MultiChar>(pool, subAttrName, isMulti);
    case ft_hash_128: // uint128: only used for primary key
        return CreateIteratorTyped<autil::uint128_t>(pool, subAttrName, isMulti);
    case ft_time:
    case ft_location:
    case ft_line:
    case ft_polygon:
    case ft_online:
    case ft_property:
    case ft_enum:
    case ft_text:
    case ft_unknown:
    default:
        return nullptr;
    }
    return nullptr;
}

template <typename T>
AttributeIteratorBase* PackAttributeReader::CreateIteratorTyped(autil::mem_pool::Pool* pool,
                                                                const std::string& subAttrName, bool isMulti) const
{
    AttributeReference* ref = GetSubAttributeReference(subAttrName);
    if (!ref) {
        return nullptr;
    }
    AttributeIteratorBase* iter = _dataReader->CreateIterator(pool);
    if (!iter) {
        return nullptr;
    }
    const AttributeFieldPrinter* fieldPrinter = GetFieldPrinter(subAttrName);
    if (!fieldPrinter) {
        return nullptr;
    }
    AttributeIteratorTyped<autil::MultiChar>* iterTyped = dynamic_cast<AttributeIteratorTyped<autil::MultiChar>*>(iter);
    assert(iterTyped);
    if (isMulti) {
        AttributeReferenceTyped<autil::MultiValueType<T>>* refTyped =
            dynamic_cast<AttributeReferenceTyped<autil::MultiValueType<T>>*>(ref);
        if (refTyped == nullptr) {
            POOL_DELETE_CLASS(iter);
            return nullptr;
        }
        return IE_POOL_COMPATIBLE_NEW_CLASS(pool, PackAttributeIteratorTyped<autil::MultiValueType<T>>, refTyped,
                                            iterTyped, fieldPrinter, pool);
    } else {
        AttributeReferenceTyped<T>* refTyped = dynamic_cast<AttributeReferenceTyped<T>*>(ref);
        if (refTyped == nullptr) {
            POOL_DELETE_CLASS(iter);
            return nullptr;
        }
        return IE_POOL_COMPATIBLE_NEW_CLASS(pool, PackAttributeIteratorTyped<T>, refTyped, iterTyped, fieldPrinter,
                                            pool);
    }
}

const AttributeFieldPrinter* PackAttributeReader::GetFieldPrinter(const std::string& subAttrName) const
{
    const auto& it = _fieldPrinters.find(subAttrName);
    if (it != _fieldPrinters.end()) {
        return &it->second;
    }
    return nullptr;
}

bool PackAttributeReader::Read(docid_t docId, const std::string& attrName, std::string& value,
                               autil::mem_pool::Pool* pool) const
{
    const char* baseAddr = GetBaseAddress(docId, pool);
    if (!baseAddr) {
        return false;
    }
    AttributeReference* attributeRef = GetSubAttributeReference(attrName);
    if (!attributeRef) {
        return false;
    }
    attributeRef->GetStrValue(baseAddr, value, pool);
    return true;
}

// bool PackAttributeReader::UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen)
// {
//     PackAttributeFields patchFields;
//     if (!PackAttributeFormatter::DecodePatchValues(buf, bufLen, patchFields)) {
//         IE_LOG(ERROR, "decode patch values for doc [%d] failed!", docId);
//         return false;
//     }
//     return UpdatePackFields(docId, patchFields, false);
// }

// bool PackAttributeReader::UpdatePackFields(docid_t docId, const PackAttributeFields& packFields,
//                                            bool hasHashKeyInFields)
// {
//     if (!mUpdatble) {
//         return false;
//     }
//     const char* data = GetBaseAddress(docId, nullptr);
//     if (!data) {
//         IE_LOG(ERROR,
//                "update pack attribute fields"
//                " for doc [%d] fail!",
//                docId);
//         return false;
//     }

//     // todo xxx: first encode in MergeAndFormatUpdateFields and then decode is unnecessary
//     int64_t oldBufSize = (int64_t)mBuffer->GetBufferSize();
//     StringView mergeValue =
//         _packAttributeFormatter->MergeAndFormatUpdateFields(data, packFields, hasHashKeyInFields, *mBuffer);
//     int64_t diffSize = (int64_t)mBuffer->GetBufferSize() - oldBufSize;
//     if (mAttrMetrics && diffSize != 0) {
//         mAttrMetrics->IncreasePackAttributeReaderBufferSizeValue(diffSize);
//     }

//     if (mBuildResourceMetricsNode && diffSize != 0) {
//         mBuildResourceMetricsNode->Update(BMT_CURRENT_MEMORY_USE, mSimplePool.getUsedBytes());
//     }

//     if (mergeValue.empty()) {
//         IE_LOG(ERROR,
//                "MergeAndFormat pack attribute fields"
//                " for doc [%d] fail!",
//                docId);
//         return false;
//     }

//     // remove hashKey
//     common::AttrValueMeta decodeValue = mDataConvertor->Decode(mergeValue);

//     return mDataReader->UpdateField(docId, (uint8_t*)decodeValue.data.data(), decodeValue.data.size());
// }

// void PackAttributeReader::InitBuffer()
// {
//     mBuffer.reset(new MemBuffer(PACK_ATTR_BUFF_INIT_SIZE, &mSimplePool));
//     if (mAttrMetrics) {
//         mAttrMetrics->IncreasePackAttributeReaderBufferSizeValue(mBuffer->GetBufferSize());
//     }
// }

} // namespace indexlibv2::index
