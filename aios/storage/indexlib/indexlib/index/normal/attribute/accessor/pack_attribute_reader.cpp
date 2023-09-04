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
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"

#include "autil/ConstString.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_iterator.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_iterator_typed.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, PackAttributeReader);

bool PackAttributeReader::Open(const PackAttributeConfigPtr& packAttrConfig, const PartitionDataPtr& partitionData,
                               const PackAttributeReader* hintReader)
{
    mPackAttrConfig = packAttrConfig;
    mPackAttrFormatter.reset(new PackAttributeFormatter());
    if (!mPackAttrFormatter->Init(packAttrConfig)) {
        IE_LOG(ERROR, "open pack attribute reader for pack attribute: %s failed.",
               packAttrConfig->GetPackName().c_str());
        return false;
    }

    mDataReader.reset(new StringAttributeReader(mAttrMetrics));
    AttributeConfigPtr attrConfig = packAttrConfig->CreateAttributeConfig();
    assert(attrConfig);

    mDataConvertor.reset(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    if (!mDataConvertor) {
        IE_LOG(ERROR,
               "fail to create AttributeConvertor"
               " for pack attribute: %s",
               attrConfig->GetAttrName().c_str());
        return false;
    }

    InitBuffer();
    bool ret = mDataReader->Open(attrConfig, partitionData, PatchApplyStrategy::PAS_APPLY_NO_PATCH,
                                 dynamic_cast<const AttributeReader*>(GET_IF_NOT_NULL(hintReader, mDataReader.get())));
    mUpdatble = mDataReader->Updatable();
    return ret;
}

void PackAttributeReader::InitBuildResourceMetricsNode(BuildResourceMetrics* buildResourceMetrics)
{
    if (buildResourceMetrics) {
        mBuildResourceMetricsNode = buildResourceMetrics->AllocateNode();
        IE_LOG(INFO, "allocate build resource node [id:%d] for pack attribute reader[%s]",
               mBuildResourceMetricsNode->GetNodeId(), mPackAttrConfig->GetPackName().c_str());
        mBuildResourceMetricsNode->Update(BMT_CURRENT_MEMORY_USE, mSimplePool.getUsedBytes());
    }
}

AttributeReference* PackAttributeReader::GetSubAttributeReference(const string& subAttrName) const
{
    if (!mPackAttrFormatter) {
        return NULL;
    }
    AttributeReference* attrRef = mPackAttrFormatter->GetAttributeReference(subAttrName);
    IncreaseAccessCounter(attrRef);
    return attrRef;
}

AttributeReference* PackAttributeReader::GetSubAttributeReference(attrid_t subAttrId) const
{
    if (!mPackAttrFormatter) {
        return NULL;
    }
    AttributeReference* attrRef = mPackAttrFormatter->GetAttributeReference(subAttrId);
    IncreaseAccessCounter(attrRef);
    return attrRef;
}

PackAttributeIterator* PackAttributeReader::CreateIterator(Pool* pool)
{
    AttributeIteratorBase* iter = mDataReader->CreateIterator(pool);
    if (!iter) {
        return NULL;
    }

    AttributeIteratorTyped<MultiChar>* iterTyped = dynamic_cast<AttributeIteratorTyped<MultiChar>*>(iter);
    assert(iterTyped);
    return IE_POOL_COMPATIBLE_NEW_CLASS(pool, PackAttributeIterator, iterTyped, pool);
}

AttributeIteratorBase* PackAttributeReader::CreateIterator(Pool* pool, const string& subAttrName) const
{
    AttributeConfigPtr attrConfig = mPackAttrFormatter->GetAttributeConfig(subAttrName);
    if (!attrConfig) {
        return NULL;
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
        return NULL;
    }
    return NULL;
}

template <typename T>
AttributeIteratorBase* PackAttributeReader::CreateIteratorTyped(Pool* pool, const string& subAttrName,
                                                                bool isMulti) const
{
    AttributeReference* ref = GetSubAttributeReference(subAttrName);
    if (!ref) {
        return NULL;
    }
    AttributeIteratorBase* iter = mDataReader->CreateIterator(pool);
    if (!iter) {
        return NULL;
    }
    AttributeIteratorTyped<MultiChar>* iterTyped = dynamic_cast<AttributeIteratorTyped<MultiChar>*>(iter);
    assert(iterTyped);
    if (isMulti) {
        AttributeReferenceTyped<MultiValueType<T>>* refTyped =
            dynamic_cast<AttributeReferenceTyped<MultiValueType<T>>*>(ref);
        if (refTyped == NULL) {
            POOL_DELETE_CLASS(iter);
            return NULL;
        }
        return IE_POOL_COMPATIBLE_NEW_CLASS(pool, PackAttributeIteratorTyped<MultiValueType<T>>, refTyped, iterTyped,
                                            pool);
    } else {
        AttributeReferenceTyped<T>* refTyped = dynamic_cast<AttributeReferenceTyped<T>*>(ref);
        if (refTyped == NULL) {
            POOL_DELETE_CLASS(iter);
            return NULL;
        }
        return IE_POOL_COMPATIBLE_NEW_CLASS(pool, PackAttributeIteratorTyped<T>, refTyped, iterTyped, pool);
    }
}

bool PackAttributeReader::Read(docid_t docId, const string& attrName, string& value, Pool* pool) const
{
    const char* baseAddr = GetBaseAddress(docId, pool);
    if (!baseAddr) {
        return false;
    }
    AttributeReference* attrRef = GetSubAttributeReference(attrName);
    if (!attrRef) {
        return false;
    }
    attrRef->GetStrValue(baseAddr, value, pool);
    return true;
}

bool PackAttributeReader::UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen)
{
    PackAttributeFields patchFields;
    if (!PackAttributeFormatter::DecodePatchValues(buf, bufLen, patchFields)) {
        IE_LOG(ERROR, "decode patch values for doc [%d] failed!", docId);
        return false;
    }
    return UpdatePackFields(docId, patchFields, false);
}

bool PackAttributeReader::UpdatePackFields(docid_t docId, const PackAttributeFields& packFields,
                                           bool hasHashKeyInFields)
{
    if (!mUpdatble) {
        return false;
    }
    const char* data = GetBaseAddress(docId, nullptr);
    if (!data) {
        IE_LOG(ERROR,
               "update pack attribute fields"
               " for doc [%d] fail!",
               docId);
        return false;
    }

    // todo xxx: first encode in MergeAndFormatUpdateFields and then decode is unnecessary
    int64_t oldBufSize = (int64_t)mBuffer->GetBufferSize();
    StringView mergeValue =
        mPackAttrFormatter->MergeAndFormatUpdateFields(data, packFields, hasHashKeyInFields, *mBuffer);
    int64_t diffSize = (int64_t)mBuffer->GetBufferSize() - oldBufSize;
    if (mAttrMetrics && diffSize != 0) {
        mAttrMetrics->IncreasePackAttributeReaderBufferSizeValue(diffSize);
    }

    if (mBuildResourceMetricsNode && diffSize != 0) {
        mBuildResourceMetricsNode->Update(BMT_CURRENT_MEMORY_USE, mSimplePool.getUsedBytes());
    }

    if (mergeValue.empty()) {
        IE_LOG(ERROR,
               "MergeAndFormat pack attribute fields"
               " for doc [%d] fail!",
               docId);
        return false;
    }

    // remove hashKey
    common::AttrValueMeta decodeValue = mDataConvertor->Decode(mergeValue);

    return mDataReader->UpdateField(docId, (uint8_t*)decodeValue.data.data(), decodeValue.data.size());
}

void PackAttributeReader::InitBuffer()
{
    mBuffer.reset(new MemBuffer(PACK_ATTR_BUFF_INIT_SIZE, &mSimplePool));
    if (mAttrMetrics) {
        mAttrMetrics->IncreasePackAttributeReaderBufferSizeValue(mBuffer->GetBufferSize());
    }
}
}} // namespace indexlib::index
