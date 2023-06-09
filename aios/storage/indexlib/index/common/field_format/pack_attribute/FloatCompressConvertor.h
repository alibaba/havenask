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
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Define.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"
#include "indexlib/util/BlockFpEncoder.h"
#include "indexlib/util/FloatInt8Encoder.h"
#include "indexlib/util/Fp16Encoder.h"

namespace indexlibv2::index {

class FloatCompressConvertor
{
public:
    FloatCompressConvertor(indexlib::config::CompressTypeOption compressType, int32_t fixedMultiValueCount)
        : _compressType(compressType)
        , _fixedMultiValueCount(fixedMultiValueCount)
        , _separator(INDEXLIB_MULTI_VALUE_SEPARATOR_STR)
    {
    }

    FloatCompressConvertor(indexlib::config::CompressTypeOption compressType, int32_t fixedMultiValueCount,
                           const std::string& seperator)
        : _compressType(compressType)
        , _fixedMultiValueCount(fixedMultiValueCount)
        , _separator(seperator)
    {
    }

    ~FloatCompressConvertor() = default;

public:
    inline bool GetValue(const char* buffer, autil::MultiValueType<float>& value,
                         autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;

    inline bool GetStrValue(const char* baseAddr, std::string& value,
                            autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;

    inline bool GetValue(const char* buffer, float& value, autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;

    inline static size_t GetMultiValueCompressLen(const indexlib::config::CompressTypeOption& compressType,
                                                  int32_t valueCount) __ALWAYS_INLINE;

    inline static uint8_t
    GetSingleValueCompressLen(const indexlib::config::CompressTypeOption& compressType) __ALWAYS_INLINE;

    inline static FieldType
    GetActualFieldType(const indexlib::config::CompressTypeOption& compressType) __ALWAYS_INLINE;

private:
    indexlib::config::CompressTypeOption _compressType;
    int32_t _fixedMultiValueCount;
    const std::string& _separator;
};

///////////////////////////////////////////////////////////////////////////////////////////
inline bool FloatCompressConvertor::GetValue(const char* buffer, autil::MultiValueType<float>& value,
                                             autil::mem_pool::Pool* pool) const
{
    assert(_fixedMultiValueCount > 0);
#define GET_VALUE_FOR_ENCODED_DATA(ENCODER)                                                                            \
    if (!pool) {                                                                                                       \
        return false;                                                                                                  \
    }                                                                                                                  \
    size_t tmpBufferSize = _fixedMultiValueCount * sizeof(float);                                                      \
    char* tmpBuffer = (char*)pool->allocate(tmpBufferSize);                                                            \
    if (!tmpBuffer) {                                                                                                  \
        return false;                                                                                                  \
    }                                                                                                                  \
    size_t encodedSize = ENCODER::GetEncodeBytesLen(_fixedMultiValueCount);                                            \
    autil::StringView encodedData(buffer, encodedSize);                                                                \
    int32_t decodedCount = ENCODER::Decode(encodedData, (char*)tmpBuffer, tmpBufferSize);                              \
    assert(decodedCount == _fixedMultiValueCount);                                                                     \
    value = autil::MultiValueType<float>(tmpBuffer, decodedCount);

    if (_compressType.HasBlockFpEncodeCompress()) {
        GET_VALUE_FOR_ENCODED_DATA(indexlib::util::BlockFpEncoder);
    } else if (_compressType.HasFp16EncodeCompress()) {
        GET_VALUE_FOR_ENCODED_DATA(indexlib::util::Fp16Encoder);
    } else if (_compressType.HasInt8EncodeCompress()) {
        if (!pool) {
            return false;
        }
        size_t tmpBufferSize = _fixedMultiValueCount * sizeof(float);
        char* tmpBuffer = (char*)pool->allocate(tmpBufferSize);
        if (!tmpBuffer) {
            return false;
        }
        size_t encodedSize = indexlib::util::FloatInt8Encoder::GetEncodeBytesLen(_fixedMultiValueCount);
        autil::StringView encodedData(buffer, encodedSize);
        int32_t decodedCount = indexlib::util::FloatInt8Encoder::Decode(_compressType.GetInt8AbsMax(), encodedData,
                                                                        (char*)tmpBuffer, tmpBufferSize);
        assert(decodedCount == _fixedMultiValueCount);
        value = autil::MultiValueType<float>(tmpBuffer, decodedCount);
    } else {
        value = autil::MultiValueType<float>(buffer, _fixedMultiValueCount);
    }
#undef GET_VALUE_FOR_ENCODED_DATA

    return true;
}

inline bool FloatCompressConvertor::GetStrValue(const char* buffer, std::string& value,
                                                autil::mem_pool::Pool* pool) const
{
#define GET_STR_VALUE_FOR_ENCODED_DATA(ENCODER)                                                                        \
    std::vector<float> tmpBufferVec;                                                                                   \
    tmpBufferVec.resize(_fixedMultiValueCount);                                                                        \
    char* tmpBuffer = (char*)tmpBufferVec.data();                                                                      \
    size_t encodedSize = ENCODER::GetEncodeBytesLen(_fixedMultiValueCount);                                            \
    autil::StringView encodedData(buffer, encodedSize);                                                                \
    int32_t decodedCount = ENCODER::Decode(encodedData, (char*)tmpBuffer, sizeof(float) * _fixedMultiValueCount);      \
    assert(decodedCount == _fixedMultiValueCount);                                                                     \
    (void)decodedCount;                                                                                                \
    value = autil::StringUtil::toString(tmpBufferVec, _separator);

    if (_compressType.HasBlockFpEncodeCompress()) {
        GET_STR_VALUE_FOR_ENCODED_DATA(indexlib::util::BlockFpEncoder);
    } else if (_compressType.HasFp16EncodeCompress()) {
        GET_STR_VALUE_FOR_ENCODED_DATA(indexlib::util::Fp16Encoder);
    } else if (_compressType.HasInt8EncodeCompress()) {
        std::vector<float> tmpBufferVec;
        tmpBufferVec.resize(_fixedMultiValueCount);
        char* tmpBuffer = (char*)tmpBufferVec.data();
        size_t encodedSize = indexlib::util::FloatInt8Encoder::GetEncodeBytesLen(_fixedMultiValueCount);
        autil::StringView encodedData(buffer, encodedSize);
        int32_t decodedCount = indexlib::util::FloatInt8Encoder::Decode(
            _compressType.GetInt8AbsMax(), encodedData, (char*)tmpBuffer, sizeof(float) * _fixedMultiValueCount);
        assert(decodedCount == _fixedMultiValueCount);
        (void)decodedCount;
        value = autil::StringUtil::toString(tmpBufferVec, _separator);
    } else {
        autil::MultiValueType<float> typedValue(buffer, _fixedMultiValueCount);
        value.clear();
        for (size_t i = 0; i < typedValue.size(); ++i) {
            std::string item = autil::StringUtil::toString<float>(typedValue[i]);
            if (i != 0) {
                value += _separator;
            }
            value += item;
        }
    }
#undef GET_STR_VALUE_FOR_ENCODED_DATA
    return true;
}

inline bool FloatCompressConvertor::GetValue(const char* buffer, float& value, autil::mem_pool::Pool* pool) const
{
    if (_compressType.HasFp16EncodeCompress()) {
        autil::StringView input(buffer, sizeof(int16_t));
        if (indexlib::util::Fp16Encoder::Decode(input, (char*)&value) != 1) {
            return false;
        }
        return true;
    }
    if (_compressType.HasInt8EncodeCompress()) {
        autil::StringView input(buffer, sizeof(int8_t));
        if (indexlib::util::FloatInt8Encoder::Decode(_compressType.GetInt8AbsMax(), input, (char*)&value) != 1) {
            return false;
        }
        return true;
    }
    // no compress
#if !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
    value = *(float*)(buffer);
#pragma GCC diagnostic pop

#else
    value = *(float*)(buffer);
#endif
    return true;
}

inline size_t FloatCompressConvertor::GetMultiValueCompressLen(const indexlib::config::CompressTypeOption& compressType,
                                                               int32_t valueCount)
{
    return indexlib::config::CompressTypeOption::GetMultiValueCompressLen(compressType, valueCount);
}

inline uint8_t
FloatCompressConvertor::GetSingleValueCompressLen(const indexlib::config::CompressTypeOption& compressType)
{
    return indexlib::config::CompressTypeOption::GetSingleValueCompressLen(compressType);
}

inline FieldType FloatCompressConvertor::GetActualFieldType(const indexlib::config::CompressTypeOption& compressType)
{
    if (compressType.HasFp16EncodeCompress()) {
        return ft_int16;
    }
    if (compressType.HasInt8EncodeCompress()) {
        return ft_int8;
    }
    return ft_float;
}
} // namespace indexlibv2::index
