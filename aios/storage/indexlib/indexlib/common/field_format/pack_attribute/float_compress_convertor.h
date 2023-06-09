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
#ifndef __INDEXLIB_FLOAT_COMPRESS_CONVERTOR_H
#define __INDEXLIB_FLOAT_COMPRESS_CONVERTOR_H

#include <memory>

#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/BlockFpEncoder.h"
#include "indexlib/util/FloatInt8Encoder.h"
#include "indexlib/util/Fp16Encoder.h"

namespace indexlib { namespace common {

class FloatCompressConvertor
{
public:
    FloatCompressConvertor(config::CompressTypeOption compressType, int32_t fixedMultiValueCount,
                           const std::string& seperator = INDEXLIB_MULTI_VALUE_SEPARATOR_STR)
        : mCompressType(compressType)
        , mFixedMultiValueCount(fixedMultiValueCount)
        , mSeparator(seperator)
    {
    }

    ~FloatCompressConvertor() {}

public:
    inline bool GetValue(const char* buffer, autil::MultiValueType<float>& value,
                         autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;

    inline bool GetStrValue(const char* baseAddr, std::string& value,
                            autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;

    inline bool GetValue(const char* buffer, float& value, autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;

    inline static size_t GetMultiValueCompressLen(const config::CompressTypeOption& compressType,
                                                  int32_t valueCount) __ALWAYS_INLINE;

    inline static uint8_t GetSingleValueCompressLen(const config::CompressTypeOption& compressType) __ALWAYS_INLINE;

    inline static FieldType GetActualFieldType(const config::CompressTypeOption& compressType) __ALWAYS_INLINE;

private:
    config::CompressTypeOption mCompressType;
    int32_t mFixedMultiValueCount;
    const std::string& mSeparator;
};

DEFINE_SHARED_PTR(FloatCompressConvertor);

///////////////////////////////////////////////////////////////////////////////////////////
inline bool FloatCompressConvertor::GetValue(const char* buffer, autil::MultiValueType<float>& value,
                                             autil::mem_pool::Pool* pool) const
{
    assert(mFixedMultiValueCount > 0);
#define GET_VALUE_FOR_ENCODED_DATA(ENCODER)                                                                            \
    if (!pool) {                                                                                                       \
        return false;                                                                                                  \
    }                                                                                                                  \
    size_t tmpBufferSize = mFixedMultiValueCount * sizeof(float);                                                      \
    char* tmpBuffer = (char*)pool->allocate(tmpBufferSize);                                                            \
    if (!tmpBuffer) {                                                                                                  \
        return false;                                                                                                  \
    }                                                                                                                  \
    size_t encodedSize = ENCODER::GetEncodeBytesLen(mFixedMultiValueCount);                                            \
    autil::StringView encodedData(buffer, encodedSize);                                                                \
    int32_t decodedCount = ENCODER::Decode(encodedData, (char*)tmpBuffer, tmpBufferSize);                              \
    assert(decodedCount == mFixedMultiValueCount);                                                                     \
    value = autil::MultiValueType<float>(tmpBuffer, decodedCount);

    if (mCompressType.HasBlockFpEncodeCompress()) {
        GET_VALUE_FOR_ENCODED_DATA(util::BlockFpEncoder);
    } else if (mCompressType.HasFp16EncodeCompress()) {
        GET_VALUE_FOR_ENCODED_DATA(util::Fp16Encoder);
    } else if (mCompressType.HasInt8EncodeCompress()) {
        if (!pool) {
            return false;
        }
        size_t tmpBufferSize = mFixedMultiValueCount * sizeof(float);
        char* tmpBuffer = (char*)pool->allocate(tmpBufferSize);
        if (!tmpBuffer) {
            return false;
        }
        size_t encodedSize = util::FloatInt8Encoder::GetEncodeBytesLen(mFixedMultiValueCount);
        autil::StringView encodedData(buffer, encodedSize);
        int32_t decodedCount =
            util::FloatInt8Encoder::Decode(mCompressType.GetInt8AbsMax(), encodedData, (char*)tmpBuffer, tmpBufferSize);
        assert(decodedCount == mFixedMultiValueCount);
        value = autil::MultiValueType<float>(tmpBuffer, decodedCount);
    } else {
        value = autil::MultiValueType<float>(buffer, mFixedMultiValueCount);
    }
#undef GET_VALUE_FOR_ENCODED_DATA

    return true;
}

inline bool FloatCompressConvertor::GetStrValue(const char* buffer, std::string& value,
                                                autil::mem_pool::Pool* pool) const
{
#define GET_STR_VALUE_FOR_ENCODED_DATA(ENCODER)                                                                        \
    std::vector<float> tmpBufferVec;                                                                                   \
    tmpBufferVec.resize(mFixedMultiValueCount);                                                                        \
    char* tmpBuffer = (char*)tmpBufferVec.data();                                                                      \
    size_t encodedSize = ENCODER::GetEncodeBytesLen(mFixedMultiValueCount);                                            \
    autil::StringView encodedData(buffer, encodedSize);                                                                \
    int32_t decodedCount = ENCODER::Decode(encodedData, (char*)tmpBuffer, sizeof(float) * mFixedMultiValueCount);      \
    assert(decodedCount == mFixedMultiValueCount);                                                                     \
    (void)decodedCount;                                                                                                \
    value = autil::StringUtil::toString(tmpBufferVec, mSeparator);

    if (mCompressType.HasBlockFpEncodeCompress()) {
        GET_STR_VALUE_FOR_ENCODED_DATA(util::BlockFpEncoder);
    } else if (mCompressType.HasFp16EncodeCompress()) {
        GET_STR_VALUE_FOR_ENCODED_DATA(util::Fp16Encoder);
    } else if (mCompressType.HasInt8EncodeCompress()) {
        std::vector<float> tmpBufferVec;
        tmpBufferVec.resize(mFixedMultiValueCount);
        char* tmpBuffer = (char*)tmpBufferVec.data();
        size_t encodedSize = util::FloatInt8Encoder::GetEncodeBytesLen(mFixedMultiValueCount);
        autil::StringView encodedData(buffer, encodedSize);
        int32_t decodedCount = util::FloatInt8Encoder::Decode(mCompressType.GetInt8AbsMax(), encodedData,
                                                              (char*)tmpBuffer, sizeof(float) * mFixedMultiValueCount);
        assert(decodedCount == mFixedMultiValueCount);
        (void)decodedCount;
        value = autil::StringUtil::toString(tmpBufferVec, mSeparator);
    } else {
        autil::MultiValueType<float> typedValue(buffer, mFixedMultiValueCount);
        value.clear();
        for (size_t i = 0; i < typedValue.size(); ++i) {
            std::string item = autil::StringUtil::toString<float>(typedValue[i]);
            if (i != 0) {
                value += mSeparator;
            }
            value += item;
        }
    }
#undef GET_STR_VALUE_FOR_ENCODED_DATA
    return true;
}

inline bool FloatCompressConvertor::GetValue(const char* buffer, float& value, autil::mem_pool::Pool* pool) const
{
    if (mCompressType.HasFp16EncodeCompress()) {
        autil::StringView input(buffer, sizeof(int16_t));
        if (util::Fp16Encoder::Decode(input, (char*)&value) != 1) {
            return false;
        }
        return true;
    }
    if (mCompressType.HasInt8EncodeCompress()) {
        autil::StringView input(buffer, sizeof(int8_t));
        if (util::FloatInt8Encoder::Decode(mCompressType.GetInt8AbsMax(), input, (char*)&value) != 1) {
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

inline size_t FloatCompressConvertor::GetMultiValueCompressLen(const config::CompressTypeOption& compressType,
                                                               int32_t valueCount)
{
    return config::CompressTypeOption::GetMultiValueCompressLen(compressType, valueCount);
}

inline uint8_t FloatCompressConvertor::GetSingleValueCompressLen(const config::CompressTypeOption& compressType)
{
    return config::CompressTypeOption::GetSingleValueCompressLen(compressType);
}

inline FieldType FloatCompressConvertor::GetActualFieldType(const config::CompressTypeOption& compressType)
{
    if (compressType.HasFp16EncodeCompress()) {
        return ft_int16;
    }
    if (compressType.HasInt8EncodeCompress()) {
        return ft_int8;
    }
    return ft_float;
}
}} // namespace indexlib::common

#endif //__INDEXLIB_FLOAT_COMPRESS_CONVERTOR_H
