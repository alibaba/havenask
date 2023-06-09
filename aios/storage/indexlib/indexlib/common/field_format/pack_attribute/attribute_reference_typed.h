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
#ifndef __INDEXLIB_ATTRIBUTE_REFERENCE_TYPED_H
#define __INDEXLIB_ATTRIBUTE_REFERENCE_TYPED_H

#include <memory>

#include "autil/PackDataReader.h"
#include "autil/StringUtil.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common/field_format/pack_attribute/attribute_reference.h"
#include "indexlib/common/field_format/pack_attribute/float_compress_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace common {

template <typename T>
class AttributeReferenceTyped : public AttributeReference
{
public:
    AttributeReferenceTyped(ReferenceOffset offset, const std::string& attrName,
                            config::CompressTypeOption compressType, int32_t fixedMultiValueCount,
                            const std::string& seperator = INDEXLIB_MULTI_VALUE_SEPARATOR_STR)
        : AttributeReference(offset, attrName, compressType, fixedMultiValueCount, seperator)
    {
        mFieldType = CppType2IndexlibFieldType<T>::GetFieldType();
        mIsMultiValue = CppType2IndexlibFieldType<T>::IsMultiValue();
    }

    ~AttributeReferenceTyped() {}

public:
    size_t SetValue(char* baseAddr, size_t dataCursor, const autil::StringView& value) override;

    size_t SetDataValue(char* baseAddr, size_t dataCursor, const autil::StringView& value, uint32_t count) override;

    bool GetStrValue(const char* baseAddr, std::string& value, autil::mem_pool::Pool* pool) const override;

    DataValue GetDataValue(const char* baseAddr) const override;

    size_t CalculateDataLength(const char* fieldData) const override;

    template <typename ValueType>
    bool GetValue(const char* buffer, ValueType& value, autil::mem_pool::Pool* pool = NULL) const __ALWAYS_INLINE;

    bool LessThan(const char* lftAddr, const char* rhtAddr) const override;

private:
    IE_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////

template <typename T>
template <typename ValueType>
inline bool AttributeReferenceTyped<T>::GetValue(const char* buffer, ValueType& value,
                                                 autil::mem_pool::Pool* pool) const
{
    assert(mFixedMultiValueCount == -1);
    assert(!mOffset.isImpactFormat());
#if !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
    value = autil::PackDataReader::read<ValueType>(buffer, mOffset);
#if !defined(__clang__)
#pragma GCC diagnostic pop
#endif
    return true;
}

template <>
template <>
inline bool AttributeReferenceTyped<float>::GetValue(const char* buffer, float& value,
                                                     autil::mem_pool::Pool* pool) const
{
    assert(!mOffset.isImpactFormat());
    FloatCompressConvertor convertor(mCompressType, 1);
    return convertor.GetValue(buffer + mOffset.getOffset(), value, pool);
}

template <typename T>
inline bool AttributeReferenceTyped<T>::GetStrValue(const char* baseAddr, std::string& value,
                                                    autil::mem_pool::Pool* pool) const
{
    T typedValue {};
    if (!GetValue(baseAddr, typedValue, pool)) {
        return false;
    }
    value = autil::StringUtil::toString<T>(typedValue);
    return true;
}

#define GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(T)                                                               \
    template <>                                                                                                        \
    template <>                                                                                                        \
    inline bool AttributeReferenceTyped<autil::MultiValueType<T>>::GetValue(                                           \
        const char* buffer, autil::MultiValueType<T>& value, autil::mem_pool::Pool* pool) const                        \
    {                                                                                                                  \
        if (!IsFixedMultiValue()) {                                                                                    \
            value = autil::PackDataReader::readMultiValue<T>(buffer, mOffset);                                         \
            return true;                                                                                               \
        }                                                                                                              \
        assert(GetFixMultiValueCount() > 0);                                                                           \
        value = autil::PackDataReader::readFixedMultiValue<T>(buffer, mOffset, GetFixMultiValueCount());               \
        return true;                                                                                                   \
    }

GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int8_t);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int16_t);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int32_t);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int64_t);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint8_t);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint16_t);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint32_t);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint64_t);
// GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(float);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(double);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(char);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(autil::MultiChar);

#undef GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE

/*  specialization for counted multi float : support compress */
template <>
template <>
inline bool AttributeReferenceTyped<autil::MultiValueType<float>>::GetValue(const char* buffer,
                                                                            autil::MultiValueType<float>& value,
                                                                            autil::mem_pool::Pool* pool) const
{
    if (!IsFixedMultiValue()) {
        value = autil::PackDataReader::readMultiValue<float>(buffer, mOffset);
        return true;
    }
    assert(!mOffset.isImpactFormat());
    FloatCompressConvertor convertor(mCompressType, GetFixMultiValueCount());
    return convertor.GetValue(buffer + mOffset.getOffset(), value, pool);
}

template <>
inline bool AttributeReferenceTyped<autil::MultiChar>::GetStrValue(const char* baseAddr, std::string& value,
                                                                   autil::mem_pool::Pool* pool) const
{
    autil::MultiChar typedValue;
    if (!GetValue(baseAddr, typedValue, pool)) {
        return false;
    }
    value = std::string(typedValue.data(), typedValue.size() * sizeof(char));
    return true;
}

#define GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(T)                                                           \
    template <>                                                                                                        \
    inline bool AttributeReferenceTyped<autil::MultiValueType<T>>::GetStrValue(                                        \
        const char* baseAddr, std::string& value, autil::mem_pool::Pool* pool) const                                   \
    {                                                                                                                  \
        autil::MultiValueType<T> typedValue;                                                                           \
        if (!GetValue(baseAddr, typedValue, pool)) {                                                                   \
            return false;                                                                                              \
        }                                                                                                              \
        value.clear();                                                                                                 \
        for (size_t i = 0; i < typedValue.size(); ++i) {                                                               \
            std::string item = autil::StringUtil::toString<T>(typedValue[i]);                                          \
            if (i != 0) {                                                                                              \
                value += mSeparator;                                                                                   \
            }                                                                                                          \
            value += item;                                                                                             \
        }                                                                                                              \
        return true;                                                                                                   \
    }

template <>
inline bool AttributeReferenceTyped<autil::MultiValueType<float>>::GetStrValue(const char* baseAddr, std::string& value,
                                                                               autil::mem_pool::Pool* pool) const
{
    if (IsFixedMultiValue()) {
        assert(!mOffset.isImpactFormat());
        FloatCompressConvertor convertor(mCompressType, mFixedMultiValueCount);
        return convertor.GetStrValue(baseAddr + mOffset.getOffset(), value, pool);
    }

    autil::MultiValueType<float> typedValue;
    if (!GetValue(baseAddr, typedValue, pool)) {
        return false;
    }
    value.clear();
    if (typedValue.size() > 0) {
        const float* begin = typedValue.data();
        const float* end = begin + typedValue.size();
        value = autil::StringUtil::toString(begin, end, mSeparator);
    }
    return true;
}

template <>
inline bool AttributeReferenceTyped<float>::GetStrValue(const char* baseAddr, std::string& value,
                                                        autil::mem_pool::Pool* pool) const
{
    float typedValue;
    FloatCompressConvertor convertor(mCompressType, 1);
    assert(!mOffset.isImpactFormat());
    if (!convertor.GetValue(baseAddr + mOffset.getOffset(), typedValue, pool)) {
        return false;
    }
    value = autil::StringUtil::toString<float>(typedValue);
    return true;
}

GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int8_t);
GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int16_t);
GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int32_t);
GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int64_t);

GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint8_t);
GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint16_t);
GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint32_t);
GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint64_t);

// GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(float);
GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(double);
GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(autil::MultiChar);

#undef GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE
///////////////////////////////////////////////////////////////////////
template <typename T>
inline size_t AttributeReferenceTyped<T>::SetDataValue(char* baseAddr, size_t dataCursor,
                                                       const autil::StringView& value, uint32_t count)
{
    assert(count == 1);
    assert(!mOffset.isImpactFormat());
    assert(dataCursor == mOffset.getOffset());
    T* pvalue = (T*)(baseAddr + mOffset.getOffset());
    *pvalue = *(T*)value.data();
    return sizeof(T);
}

template <>
inline size_t AttributeReferenceTyped<float>::SetDataValue(char* baseAddr, size_t dataCursor,
                                                           const autil::StringView& value, uint32_t count)
{
    assert(count == 1);
    assert(!mOffset.isImpactFormat());
    assert(dataCursor == mOffset.getOffset());
    assert(FloatCompressConvertor::GetSingleValueCompressLen(mCompressType) == value.length());
    memcpy(baseAddr + dataCursor, value.data(), value.length());
    return value.length();
}

#define SET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(T)                                                          \
    template <>                                                                                                        \
    inline size_t AttributeReferenceTyped<autil::MultiValueType<T>>::SetDataValue(                                     \
        char* baseAddr, size_t dataCursor, const autil::StringView& value, uint32_t count)                             \
    {                                                                                                                  \
        if (IsFixedMultiValue()) {                                                                                     \
            assert(!mOffset.isImpactFormat());                                                                         \
            assert(GetFixMultiValueCount() > 0);                                                                       \
            assert((value.size() == sizeof(T) * mFixedMultiValueCount) || mCompressType.HasBlockFpEncodeCompress() ||  \
                   mCompressType.HasFp16EncodeCompress() || mCompressType.HasInt8EncodeCompress());                    \
            memcpy(baseAddr + dataCursor, value.data(), value.length());                                               \
            assert(dataCursor >= mOffset.getOffset());                                                                 \
            return value.length();                                                                                     \
        }                                                                                                              \
        size_t retLen = value.length();                                                                                \
        char* buffer = baseAddr + dataCursor;                                                                          \
        if (mOffset.needVarLenHeader()) {                                                                              \
            size_t bufferSize = autil::MultiValueFormatter::getEncodedCountLength(count);                              \
            size_t countLen = autil::MultiValueFormatter::encodeCount(count, buffer, bufferSize);                      \
            buffer = buffer + countLen;                                                                                \
            retLen += countLen;                                                                                        \
        }                                                                                                              \
        memcpy(buffer, value.data(), value.length());                                                                  \
        autil::PackDataFormatter::setVarLenOffset(mOffset, baseAddr, dataCursor);                                      \
        return retLen;                                                                                                 \
    }

SET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int8_t);
SET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int16_t);
SET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int32_t);
SET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int64_t);

SET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint8_t);
SET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint16_t);
SET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint32_t);
SET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint64_t);

SET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(float);
SET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(double);
SET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(char);
SET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(autil::MultiChar);

#undef SET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE

/////////////////////////////////////////////////////////////////////////////

template <typename T>
inline size_t AttributeReferenceTyped<T>::SetValue(char* baseAddr, size_t dataCursor, const autil::StringView& value)
{
    return SetDataValue(baseAddr, dataCursor, value, 1);
}

#define SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(T)                                                               \
    template <>                                                                                                        \
    inline size_t AttributeReferenceTyped<autil::MultiValueType<T>>::SetValue(char* baseAddr, size_t dataCursor,       \
                                                                              const autil::StringView& value)          \
    {                                                                                                                  \
        if (IsFixedMultiValue()) {                                                                                     \
            return SetDataValue(baseAddr, dataCursor, value, GetFixMultiValueCount());                                 \
        }                                                                                                              \
        size_t countLen = 0;                                                                                           \
        uint32_t count = autil::MultiValueFormatter::decodeCount(value.data(), countLen);                              \
        assert(countLen <= value.size());                                                                              \
        autil::StringView dataValue(value.data() + countLen, value.size() - countLen);                                 \
        return SetDataValue(baseAddr, dataCursor, dataValue, count);                                                   \
    }

SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int8_t);
SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int16_t);
SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int32_t);
SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int64_t);

SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint8_t);
SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint16_t);
SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint32_t);
SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint64_t);

SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(float);
SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(double);
SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(char);
SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(autil::MultiChar);

#undef SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE
//////////////////////////////////////////////////////////////////////////////////

template <typename T>
inline AttributeReference::DataValue AttributeReferenceTyped<T>::GetDataValue(const char* baseAddr) const
{
    assert(!mOffset.isImpactFormat());
    return DataValue(autil::StringView(baseAddr + mOffset.getOffset(), sizeof(T)), 1, false, true);
}

template <>
inline AttributeReference::DataValue AttributeReferenceTyped<float>::GetDataValue(const char* baseAddr) const
{
    assert(!mOffset.isImpactFormat());
    uint8_t dataLen = FloatCompressConvertor::GetSingleValueCompressLen(mCompressType);
    return DataValue(autil::StringView(baseAddr + mOffset.getOffset(), dataLen), 1, false, true);
}

#define GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(T)                                                          \
    template <>                                                                                                        \
    inline AttributeReference::DataValue AttributeReferenceTyped<autil::MultiValueType<T>>::GetDataValue(              \
        const char* baseAddr) const                                                                                    \
    {                                                                                                                  \
        autil::MultiValueType<T> value;                                                                                \
        if (!GetValue(baseAddr, value)) {                                                                              \
            return DataValue();                                                                                        \
        }                                                                                                              \
        if (IsFixedMultiValue()) {                                                                                     \
            return DataValue(autil::StringView(value.getData(), value.getDataSize()), value.getEncodedCountValue(),    \
                             false, true);                                                                             \
        }                                                                                                              \
        return DataValue(autil::StringView(value.getBaseAddress(), value.length()), value.getEncodedCountValue(),      \
                         mOffset.needVarLenHeader(), false);                                                           \
    }

template <>
inline AttributeReference::DataValue
AttributeReferenceTyped<autil::MultiValueType<float>>::GetDataValue(const char* baseAddr) const
{
    if (IsFixedMultiValue()) {
        assert(!mOffset.isImpactFormat());
        size_t dataLen = sizeof(float) * mFixedMultiValueCount;
        if (mCompressType.HasBlockFpEncodeCompress()) {
            dataLen = util::BlockFpEncoder::GetEncodeBytesLen(mFixedMultiValueCount);
        } else if (mCompressType.HasFp16EncodeCompress()) {
            dataLen = util::Fp16Encoder::GetEncodeBytesLen(mFixedMultiValueCount);
        } else if (mCompressType.HasInt8EncodeCompress()) {
            dataLen = util::FloatInt8Encoder::GetEncodeBytesLen(mFixedMultiValueCount);
        }
        return DataValue(autil::StringView(baseAddr + mOffset.getOffset(), dataLen), mFixedMultiValueCount, false,
                         true);
    } else {
        autil::MultiValueType<float> value;
        if (!GetValue(baseAddr, value)) {
            return DataValue();
        }
        return DataValue(autil::StringView(value.getBaseAddress(), value.length()), value.getEncodedCountValue(),
                         mOffset.needVarLenHeader(), false);
    }
}

GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int8_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int16_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int32_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int64_t);

GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint8_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint16_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint32_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint64_t);

GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(double);
GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(char);

#undef GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE

template <>
inline AttributeReference::DataValue
AttributeReferenceTyped<autil::MultiValueType<autil::MultiChar>>::GetDataValue(const char* baseAddr) const
{
    // MultiString can not be fixed size
    assert(!IsFixedMultiValue() && "MultiString can not be fixed size");
    assert(mOffset.needVarLenHeader());
    autil::MultiValueType<autil::MultiChar> value;
    GetValue(baseAddr, value);
    if (value.size() == 0) {
        // * empty multiString only store count in index format *
        // attention:
        // MultiValueType<MultiChar>::length = count + 1byte(offset), not consistent with index
        // so autil::MultiString seralize & deserialize with 1 more byte for compitablity
        return DataValue(autil::StringView(value.getBaseAddress(), VarNumAttributeFormatter::GetEncodedCountLength(0)),
                         0, true, false);
    } else {
        return DataValue(autil::StringView(value.getBaseAddress(), value.length()), value.size(), true, false);
    }
}

/////////////////////////////////////////////////////////////////////////////////////
template <typename T>
inline bool AttributeReferenceTyped<T>::LessThan(const char* lftAddr, const char* rhtAddr) const
{
    assert(!mOffset.isImpactFormat());
    return *(T*)(lftAddr + mOffset.getOffset()) < *(T*)(rhtAddr + mOffset.getOffset());
}

#define LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(T)                                                               \
    template <>                                                                                                        \
    inline bool AttributeReferenceTyped<autil::MultiValueType<T>>::LessThan(const char* lftAddr, const char* rhtAddr)  \
        const                                                                                                          \
    {                                                                                                                  \
        INDEXLIB_THROW(util::UnSupportedException, "not support LessThan");                                            \
        return false;                                                                                                  \
    }

LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int8_t);
LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int16_t);
LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int32_t);
LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int64_t);

LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint8_t);
LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint16_t);
LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint32_t);
LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint64_t);

LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(float);
LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(double);
LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(char);
LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(autil::MultiChar);

#undef LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE

template <typename T>
inline size_t AttributeReferenceTyped<T>::CalculateDataLength(const char* fieldData) const
{
    assert(!mOffset.isImpactFormat());
    return sizeof(T);
}

template <>
inline size_t AttributeReferenceTyped<float>::CalculateDataLength(const char* fieldData) const
{
    assert(!mOffset.isImpactFormat());
    return FloatCompressConvertor::GetSingleValueCompressLen(mCompressType);
}

#define CAL_DATA_LEN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(T)                                                            \
    template <>                                                                                                        \
    inline size_t AttributeReferenceTyped<autil::MultiValueType<T>>::CalculateDataLength(const char* fieldData) const  \
    {                                                                                                                  \
        assert(!mOffset.isImpactFormat());                                                                             \
        if (IsFixedMultiValue()) {                                                                                     \
            return sizeof(T) * mFixedMultiValueCount;                                                                  \
        }                                                                                                              \
        assert(mFixedMultiValueCount == -1);                                                                           \
        autil::MultiValueType<T> value(fieldData);                                                                     \
        return value.length();                                                                                         \
    }

template <>
inline size_t AttributeReferenceTyped<autil::MultiValueType<float>>::CalculateDataLength(const char* fieldData) const
{
    assert(!mOffset.isImpactFormat());
    if (IsFixedMultiValue()) {
        size_t dataLen = sizeof(float) * mFixedMultiValueCount;
        if (mCompressType.HasBlockFpEncodeCompress()) {
            dataLen = util::BlockFpEncoder::GetEncodeBytesLen(mFixedMultiValueCount);
        } else if (mCompressType.HasFp16EncodeCompress()) {
            dataLen = util::Fp16Encoder::GetEncodeBytesLen(mFixedMultiValueCount);
        } else if (mCompressType.HasInt8EncodeCompress()) {
            dataLen = util::FloatInt8Encoder::GetEncodeBytesLen(mFixedMultiValueCount);
        }
        return dataLen;
    } else {
        autil::MultiValueType<float> value(fieldData);
        return value.length();
    }
}

CAL_DATA_LEN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int8_t);
CAL_DATA_LEN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int16_t);
CAL_DATA_LEN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int32_t);
CAL_DATA_LEN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int64_t);

CAL_DATA_LEN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint8_t);
CAL_DATA_LEN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint16_t);
CAL_DATA_LEN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint32_t);
CAL_DATA_LEN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint64_t);

CAL_DATA_LEN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(double);
CAL_DATA_LEN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(char);

#undef CAL_DATA_LEN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE

template <>
inline size_t
AttributeReferenceTyped<autil::MultiValueType<autil::MultiChar>>::CalculateDataLength(const char* fieldData) const
{
    // MultiString can not be fixed size
    assert(!IsFixedMultiValue() && "MultiString can not be fixed size");
    assert(mOffset.needVarLenHeader());
    autil::MultiValueType<autil::MultiChar> value(fieldData);
    if (value.size() == 0) {
        // * empty multiString only store count in index format *
        // attention:
        // MultiValueType<MultiChar>::length = count + 1byte(offset), not consistent with index
        // so autil::MultiString seralize & deserialize with 1 more byte for compitablity
        return VarNumAttributeFormatter::GetEncodedCountLength(0);
    } else {
        return value.length();
    }
}

}} // namespace indexlib::common

#endif //__INDEXLIB_ATTRIBUTE_REFERENCE_TYPED_H
