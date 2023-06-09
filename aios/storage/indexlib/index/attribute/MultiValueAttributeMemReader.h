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

#include "autil/Log.h"
#include "indexlib/base/Define.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/attribute/AttributeMemReader.h"
#include "indexlib/index/common/data_structure/VarLenDataAccessor.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"
#include "indexlib/index/common/field_format/pack_attribute/FloatCompressConvertor.h"

namespace indexlibv2::index {

template <typename T>
class MultiValueAttributeMemReader : public AttributeMemReader
{
public:
    MultiValueAttributeMemReader(const VarLenDataAccessor* accessor,
                                 const indexlib::config::CompressTypeOption compressType, int32_t fixedValueCount,
                                 bool supportNull);
    ~MultiValueAttributeMemReader() = default;

public:
    size_t EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory) override;
    size_t EvaluateCurrentMemUsed() override;

public:
    bool IsInMemory() const { return true; }
    inline uint64_t GetOffset(docid_t docId) const { return _varLenDataAccessor->GetOffset(docId); }

    bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen, bool& isNull) override;
    inline bool Read(docid_t docId, autil::MultiValueType<T>& value, bool& isNull,
                     autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;
    uint64_t GetDocCount() const { return _varLenDataAccessor->GetDocCount(); }
    bool Updatable() const { return false; }

public:
    inline uint32_t TEST_GetDataLength(docid_t docId) const { return GetDataLength(docId); }

private:
    bool CheckDocId(docid_t docId) const { return docId >= 0 && docId < (docid_t)GetDocCount(); }
    inline uint32_t GetDataLength(docid_t docId) const;

private:
    const VarLenDataAccessor* _varLenDataAccessor;
    indexlib::config::CompressTypeOption _compressType;
    int32_t _fixedValueCount;
    bool _supportNull;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, MultiValueAttributeMemReader, T);

/////////////////////////////////////////////////////////

template <typename T>
inline MultiValueAttributeMemReader<T>::MultiValueAttributeMemReader(
    const VarLenDataAccessor* accessor, const indexlib::config::CompressTypeOption compressType,
    int32_t fixedValueCount, bool supportNull)
    : AttributeMemReader()
    , _varLenDataAccessor(accessor)
    , _compressType(compressType)
    , _fixedValueCount(fixedValueCount)
    , _supportNull(supportNull)
{
}

template <typename T>
inline uint32_t MultiValueAttributeMemReader<T>::GetDataLength(docid_t docId) const
{
    uint8_t* data;
    uint32_t dataLength;
    _varLenDataAccessor->ReadData(docId, data, dataLength);
    return dataLength;
}

template <typename T>
inline bool MultiValueAttributeMemReader<T>::Read(docid_t docId, uint8_t* buf, uint32_t bufLen, bool& isNull)
{
    if (!CheckDocId(docId)) {
        return false;
    }
    assert(GetDataLength(docId) == bufLen);
    uint32_t dataLength;
    _varLenDataAccessor->ReadData(docId, buf, dataLength);
    if (buf == NULL) {
        return false;
    }

    if (_fixedValueCount == -1) {
        size_t countLen = 0;
        MultiValueAttributeFormatter::DecodeCount((const char*)buf, countLen, isNull);
    } else {
        isNull = false;
    }
    return true;
}

template <typename T>
inline bool MultiValueAttributeMemReader<T>::Read(docid_t docId, autil::MultiValueType<T>& value, bool& isNull,
                                                  autil::mem_pool::Pool* pool) const
{
    if (!CheckDocId(docId)) {
        return false;
    }
    uint8_t* data = NULL;
    uint32_t dataLength = 0;
    _varLenDataAccessor->ReadData(docId, data, dataLength);
    if (data == NULL) {
        return false;
    }

    if (_fixedValueCount == -1) {
        value.init((const void*)data);
        isNull = _supportNull ? value.isNull() : false;
        return true;
    } else {
        value.init(data, _fixedValueCount);
        isNull = false;
        return true;
    }
}

template <>
inline bool MultiValueAttributeMemReader<float>::Read(docid_t docId, autil::MultiValueType<float>& value, bool& isNull,
                                                      autil::mem_pool::Pool* pool) const
{
    if (!CheckDocId(docId)) {
        return false;
    }
    uint8_t* data = NULL;
    uint32_t dataLength = 0;
    _varLenDataAccessor->ReadData(docId, data, dataLength);
    if (data == NULL) {
        return false;
    }

    if (_fixedValueCount == -1) {
        value.init((const void*)data);
        isNull = _supportNull ? value.isNull() : false;
        return true;
    }
    if (!pool) {
        AUTIL_LOG(ERROR, "pool is nullptr when read MultiValueType value from fixed length attribute");
        return false;
    }

    isNull = false;
    FloatCompressConvertor convertor(_compressType, _fixedValueCount);
    return convertor.GetValue((const char*)data, value, pool);
}

template <typename T>
inline size_t MultiValueAttributeMemReader<T>::EstimateMemUsed(
    const std::shared_ptr<config::IIndexConfig>& indexConfig,
    const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory)
{
    AUTIL_LOG(ERROR, "not implement yet!");
    return 0;
}

template <typename T>
inline size_t MultiValueAttributeMemReader<T>::EvaluateCurrentMemUsed()
{
    AUTIL_LOG(ERROR, "not implement yet!");
    return 0;
}

} // namespace indexlibv2::index
