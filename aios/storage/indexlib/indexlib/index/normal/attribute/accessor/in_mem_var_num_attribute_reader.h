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

#include "autil/MultiValueType.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common/field_format/pack_attribute/float_compress_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/index/data_structure/var_len_data_accessor.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename T>
class InMemVarNumAttributeReader : public AttributeSegmentReader
{
public:
    InMemVarNumAttributeReader(VarLenDataAccessor* accessor, config::CompressTypeOption compressType,
                               int32_t fixedValueCount, bool supportNull);
    ~InMemVarNumAttributeReader();

public:
    bool IsInMemory() const override { return true; }
    uint32_t TEST_GetDataLength(docid_t docId) const override;
    uint64_t GetOffset(docid_t docId, const ReadContextBasePtr& ctx) const override;
    bool Read(docid_t docId, const ReadContextBasePtr& ctx, uint8_t* buf, uint32_t bufLen, bool& isNull) override;
    bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen, bool isNull) override;
    bool Updatable() const override { return true; }
    ReadContextBasePtr CreateReadContextPtr(autil::mem_pool::Pool* pool) const override { return nullptr; }

public:
    inline bool Read(docid_t docId, autil::MultiValueType<T>& value, bool& isNull,
                     autil::mem_pool::Pool* pool = NULL) const __ALWAYS_INLINE;

    uint64_t GetDocCount() const { return mAccessor->GetDocCount(); }
    bool CheckDocId(docid_t docId) const { return docId >= 0 && docId < (docid_t)GetDocCount(); }

private:
    VarLenDataAccessor* mAccessor;
    config::CompressTypeOption mCompressType;
    int32_t mFixedValueCount;
    bool mSupportNull;
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, InMemVarNumAttributeReader);

////////////////////////////////
template <typename T>
InMemVarNumAttributeReader<T>::InMemVarNumAttributeReader(VarLenDataAccessor* accessor,
                                                          config::CompressTypeOption compressType,
                                                          int32_t fixedValueCount, bool supportNull)
    : mAccessor(accessor)
    , mCompressType(compressType)
    , mFixedValueCount(fixedValueCount)
    , mSupportNull(supportNull)
{
}

template <typename T>
InMemVarNumAttributeReader<T>::~InMemVarNumAttributeReader()
{
}

template <typename T>
inline uint64_t InMemVarNumAttributeReader<T>::GetOffset(docid_t docId, const ReadContextBasePtr& ctx) const
{
    return mAccessor->GetOffset(docId);
}

template <typename T>
inline uint32_t InMemVarNumAttributeReader<T>::TEST_GetDataLength(docid_t docId) const
{
    uint8_t* data;
    uint32_t dataLength;
    mAccessor->ReadData(docId, data, dataLength);
    return dataLength;
}

template <typename T>
inline bool InMemVarNumAttributeReader<T>::Read(docid_t docId, const ReadContextBasePtr& ctx, uint8_t* buf,
                                                uint32_t bufLen, bool& isNull)
{
    if (!CheckDocId(docId)) {
        return false;
    }
    assert(TEST_GetDataLength(docId) == bufLen);
    uint32_t dataLength;
    mAccessor->ReadData(docId, buf, dataLength);
    if (buf == NULL) {
        return false;
    }

    if (mFixedValueCount == -1) {
        size_t countLen = 0;
        common::VarNumAttributeFormatter::DecodeCount((const char*)buf, countLen, isNull);
    } else {
        isNull = false;
    }
    return true;
}

template <typename T>
inline bool InMemVarNumAttributeReader<T>::Read(docid_t docId, autil::MultiValueType<T>& value, bool& isNull,
                                                autil::mem_pool::Pool* pool) const
{
    if (!CheckDocId(docId)) {
        return false;
    }
    uint8_t* data = NULL;
    uint32_t dataLength = 0;
    mAccessor->ReadData(docId, data, dataLength);
    if (data == NULL) {
        return false;
    }

    if (mFixedValueCount == -1) {
        value.init((const void*)data);
        isNull = mSupportNull ? value.isNull() : false;
        return true;
    } else {
        value.init(data, mFixedValueCount);
        isNull = false;
        return true;
    }
}

template <>
inline bool InMemVarNumAttributeReader<float>::Read(docid_t docId, autil::MultiValueType<float>& value, bool& isNull,
                                                    autil::mem_pool::Pool* pool) const
{
    if (!CheckDocId(docId)) {
        return false;
    }
    uint8_t* data = NULL;
    uint32_t dataLength = 0;
    mAccessor->ReadData(docId, data, dataLength);
    if (data == NULL) {
        return false;
    }

    if (mFixedValueCount == -1) {
        value.init((const void*)data);
        isNull = mSupportNull ? value.isNull() : false;
        return true;
    }
    if (!pool) {
        IE_LOG(ERROR, "pool is nullptr when read MultiValueType value from fixed length attribute");
        return false;
    }

    isNull = false;
    common::FloatCompressConvertor convertor(mCompressType, mFixedValueCount);
    return convertor.GetValue((const char*)data, value, pool);
}

template <typename T>
inline bool InMemVarNumAttributeReader<T>::UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen, bool isNull)
{
    return mAccessor->UpdateValue(docId, autil::StringView(reinterpret_cast<char*>(buf), bufLen));
}
}} // namespace indexlib::index
