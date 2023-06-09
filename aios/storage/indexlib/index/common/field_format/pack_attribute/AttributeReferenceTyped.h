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

#include "autil/PackDataReader.h"
#include "autil/StringUtil.h"
#include "indexlib/base/FieldTypeUtil.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"
#include "indexlib/index/common/field_format/pack_attribute/AttributeReference.h"
#include "indexlib/index/common/field_format/pack_attribute/FloatCompressConvertor.h"

namespace indexlibv2::index {

template <typename T>
class AttributeReferenceTyped : public AttributeReference
{
public:
    AttributeReferenceTyped(ReferenceOffset offset, const std::string& attrName,
                            indexlib::config::CompressTypeOption compressType, int32_t fixedMultiValueCount)
        : AttributeReference(offset, attrName, compressType, fixedMultiValueCount)
    {
        _fieldType = indexlib::CppType2IndexlibFieldType<T>::GetFieldType();
        _isMultiValue = indexlib::CppType2IndexlibFieldType<T>::IsMultiValue();
    }
    AttributeReferenceTyped(ReferenceOffset offset, const std::string& attrName,
                            indexlib::config::CompressTypeOption compressType, int32_t fixedMultiValueCount,
                            const std::string& seperator)
        : AttributeReference(offset, attrName, compressType, fixedMultiValueCount, seperator)
    {
        _fieldType = indexlib::CppType2IndexlibFieldType<T>::GetFieldType();
        _isMultiValue = indexlib::CppType2IndexlibFieldType<T>::IsMultiValue();
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

    bool Equal(const char* lftAddr, const char* rhtAddr) const override;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, AttributeReferenceTyped, T);

template <typename T>
template <typename ValueType>
inline bool AttributeReferenceTyped<T>::GetValue(const char* buffer, ValueType& value,
                                                 autil::mem_pool::Pool* pool) const
{
    assert(_fixedMultiValueCount == -1);
    assert(!_offset.isImpactFormat());
#if !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
    value = autil::PackDataReader::read<ValueType>(buffer, _offset);
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
    assert(!_offset.isImpactFormat());
    FloatCompressConvertor convertor(_compressType, 1);
    return convertor.GetValue(buffer + _offset.getOffset(), value, pool);
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
            value = autil::PackDataReader::readMultiValue<T>(buffer, _offset);                                         \
            return true;                                                                                               \
        }                                                                                                              \
        assert(GetFixMultiValueCount() > 0);                                                                           \
        value = autil::PackDataReader::readFixedMultiValue<T>(buffer, _offset, GetFixMultiValueCount());               \
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
        value = autil::PackDataReader::readMultiValue<float>(buffer, _offset);
        return true;
    }
    assert(!_offset.isImpactFormat());
    FloatCompressConvertor convertor(_compressType, GetFixMultiValueCount());
    return convertor.GetValue(buffer + _offset.getOffset(), value, pool);
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
                value += _separator;                                                                                   \
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
        assert(!_offset.isImpactFormat());
        FloatCompressConvertor convertor(_compressType, _fixedMultiValueCount);
        return convertor.GetStrValue(baseAddr + _offset.getOffset(), value, pool);
    }

    autil::MultiValueType<float> typedValue;
    if (!GetValue(baseAddr, typedValue, pool)) {
        return false;
    }
    value.clear();
    if (typedValue.size() > 0) {
        const float* begin = typedValue.data();
        const float* end = begin + typedValue.size();
        value = autil::StringUtil::toString(begin, end, _separator);
    }
    return true;
}

template <>
inline bool AttributeReferenceTyped<float>::GetStrValue(const char* baseAddr, std::string& value,
                                                        autil::mem_pool::Pool* pool) const
{
    float typedValue;
    FloatCompressConvertor convertor(_compressType, 1);
    assert(!_offset.isImpactFormat());
    if (!convertor.GetValue(baseAddr + _offset.getOffset(), typedValue, pool)) {
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
    assert(!_offset.isImpactFormat());
    assert(dataCursor == _offset.getOffset());
    T* pvalue = (T*)(baseAddr + _offset.getOffset());
    *pvalue = *(T*)value.data();
    return sizeof(T);
}

template <>
inline size_t AttributeReferenceTyped<float>::SetDataValue(char* baseAddr, size_t dataCursor,
                                                           const autil::StringView& value, uint32_t count)
{
    assert(count == 1);
    assert(!_offset.isImpactFormat());
    assert(dataCursor == _offset.getOffset());
    assert(FloatCompressConvertor::GetSingleValueCompressLen(_compressType) == value.length());
    memcpy(baseAddr + dataCursor, value.data(), value.length());
    return value.length();
}

#define SET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(T)                                                          \
    template <>                                                                                                        \
    inline size_t AttributeReferenceTyped<autil::MultiValueType<T>>::SetDataValue(                                     \
        char* baseAddr, size_t dataCursor, const autil::StringView& value, uint32_t count)                             \
    {                                                                                                                  \
        if (IsFixedMultiValue()) {                                                                                     \
            assert(!_offset.isImpactFormat());                                                                         \
            assert(GetFixMultiValueCount() > 0);                                                                       \
            assert((value.size() == sizeof(T) * _fixedMultiValueCount) || _compressType.HasBlockFpEncodeCompress() ||  \
                   _compressType.HasFp16EncodeCompress() || _compressType.HasInt8EncodeCompress());                    \
            memcpy(baseAddr + dataCursor, value.data(), value.length());                                               \
            assert(dataCursor >= _offset.getOffset());                                                                 \
            return value.length();                                                                                     \
        }                                                                                                              \
        size_t retLen = value.length();                                                                                \
        char* buffer = baseAddr + dataCursor;                                                                          \
        if (_offset.needVarLenHeader()) {                                                                              \
            size_t bufferSize = autil::MultiValueFormatter::getEncodedCountLength(count);                              \
            size_t countLen = autil::MultiValueFormatter::encodeCount(count, buffer, bufferSize);                      \
            buffer = buffer + countLen;                                                                                \
            retLen += countLen;                                                                                        \
        }                                                                                                              \
        memcpy(buffer, value.data(), value.length());                                                                  \
        autil::PackDataFormatter::setVarLenOffset(_offset, baseAddr, dataCursor);                                      \
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
    assert(!_offset.isImpactFormat());
    return DataValue(autil::StringView(baseAddr + _offset.getOffset(), sizeof(T)), 1, false, true);
}

template <>
inline AttributeReference::DataValue AttributeReferenceTyped<float>::GetDataValue(const char* baseAddr) const
{
    assert(!_offset.isImpactFormat());
    uint8_t dataLen = FloatCompressConvertor::GetSingleValueCompressLen(_compressType);
    return DataValue(autil::StringView(baseAddr + _offset.getOffset(), dataLen), 1, false, true);
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
                         _offset.needVarLenHeader(), false);                                                           \
    }

template <>
inline AttributeReference::DataValue
AttributeReferenceTyped<autil::MultiValueType<float>>::GetDataValue(const char* baseAddr) const
{
    if (IsFixedMultiValue()) {
        assert(!_offset.isImpactFormat());
        size_t dataLen = sizeof(float) * _fixedMultiValueCount;
        if (_compressType.HasBlockFpEncodeCompress()) {
            dataLen = indexlib::util::BlockFpEncoder::GetEncodeBytesLen(_fixedMultiValueCount);
        } else if (_compressType.HasFp16EncodeCompress()) {
            dataLen = indexlib::util::Fp16Encoder::GetEncodeBytesLen(_fixedMultiValueCount);
        } else if (_compressType.HasInt8EncodeCompress()) {
            dataLen = indexlib::util::FloatInt8Encoder::GetEncodeBytesLen(_fixedMultiValueCount);
        }
        return DataValue(autil::StringView(baseAddr + _offset.getOffset(), dataLen), _fixedMultiValueCount, false,
                         true);
    } else {
        autil::MultiValueType<float> value;
        if (!GetValue(baseAddr, value)) {
            return DataValue();
        }
        return DataValue(autil::StringView(value.getBaseAddress(), value.length()), value.getEncodedCountValue(),
                         _offset.needVarLenHeader(), false);
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
    assert(_offset.needVarLenHeader());
    autil::MultiValueType<autil::MultiChar> value;
    GetValue(baseAddr, value);
    if (value.size() == 0) {
        // * empty multiString only store count in index format *
        // attention:
        // MultiValueType<MultiChar>::length = count + 1byte(offset), not consistent with index
        // so autil::MultiString seralize & deserialize with 1 more byte for compitablity
        return DataValue(
            autil::StringView(value.getBaseAddress(), MultiValueAttributeFormatter::GetEncodedCountLength(0)), 0, true,
            false);
    } else {
        return DataValue(autil::StringView(value.getBaseAddress(), value.length()), value.size(), true, false);
    }
}

/////////////////////////////////////////////////////////////////////////////////////
template <typename T>
inline bool AttributeReferenceTyped<T>::LessThan(const char* lftAddr, const char* rhtAddr) const
{
    assert(!_offset.isImpactFormat());
    return *(T*)(lftAddr + _offset.getOffset()) < *(T*)(rhtAddr + _offset.getOffset());
}

#define LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(T)                                                               \
    template <>                                                                                                        \
    inline bool AttributeReferenceTyped<autil::MultiValueType<T>>::LessThan(const char* lftAddr, const char* rhtAddr)  \
        const                                                                                                          \
    {                                                                                                                  \
        AUTIL_LOG(ERROR, "not support LessThan type");                                                                 \
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
inline bool AttributeReferenceTyped<T>::Equal(const char* lftAddr, const char* rhtAddr) const
{
    assert(!_offset.isImpactFormat());
    return *(T*)(lftAddr + _offset.getOffset()) == *(T*)(rhtAddr + _offset.getOffset());
}

#define EQUAL_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(T)                                                                   \
    template <>                                                                                                        \
    inline bool AttributeReferenceTyped<autil::MultiValueType<T>>::Equal(const char* lftAddr, const char* rhtAddr)     \
        const                                                                                                          \
    {                                                                                                                  \
        AUTIL_LOG(ERROR, "not support Equal type");                                                                    \
        return false;                                                                                                  \
    }

EQUAL_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int8_t);
EQUAL_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int16_t);
EQUAL_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int32_t);
EQUAL_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int64_t);

EQUAL_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint8_t);
EQUAL_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint16_t);
EQUAL_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint32_t);
EQUAL_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint64_t);

EQUAL_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(float);
EQUAL_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(double);
EQUAL_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(char);
EQUAL_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(autil::MultiChar);

#undef EQUAL_SPECIALIZATION_FOR_MULTI_VALUE_TYPE

template <typename T>
inline size_t AttributeReferenceTyped<T>::CalculateDataLength(const char* fieldData) const
{
    assert(!_offset.isImpactFormat());
    return sizeof(T);
}

template <>
inline size_t AttributeReferenceTyped<float>::CalculateDataLength(const char* fieldData) const
{
    assert(!_offset.isImpactFormat());
    return FloatCompressConvertor::GetSingleValueCompressLen(_compressType);
}

#define CAL_DATA_LEN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(T)                                                            \
    template <>                                                                                                        \
    inline size_t AttributeReferenceTyped<autil::MultiValueType<T>>::CalculateDataLength(const char* fieldData) const  \
    {                                                                                                                  \
        assert(!_offset.isImpactFormat());                                                                             \
        if (IsFixedMultiValue()) {                                                                                     \
            return sizeof(T) * _fixedMultiValueCount;                                                                  \
        }                                                                                                              \
        assert(_fixedMultiValueCount == -1);                                                                           \
        autil::MultiValueType<T> value(fieldData);                                                                     \
        return value.length();                                                                                         \
    }

template <>
inline size_t AttributeReferenceTyped<autil::MultiValueType<float>>::CalculateDataLength(const char* fieldData) const
{
    assert(!_offset.isImpactFormat());
    if (IsFixedMultiValue()) {
        size_t dataLen = sizeof(float) * _fixedMultiValueCount;
        if (_compressType.HasBlockFpEncodeCompress()) {
            dataLen = indexlib::util::BlockFpEncoder::GetEncodeBytesLen(_fixedMultiValueCount);
        } else if (_compressType.HasFp16EncodeCompress()) {
            dataLen = indexlib::util::Fp16Encoder::GetEncodeBytesLen(_fixedMultiValueCount);
        } else if (_compressType.HasInt8EncodeCompress()) {
            dataLen = indexlib::util::FloatInt8Encoder::GetEncodeBytesLen(_fixedMultiValueCount);
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
    assert(_offset.needVarLenHeader());
    autil::MultiValueType<autil::MultiChar> value(fieldData);
    if (value.size() == 0) {
        // * empty multiString only store count in index format *
        // attention:
        // MultiValueType<MultiChar>::length = count + 1byte(offset), not consistent with index
        // so autil::MultiString seralize & deserialize with 1 more byte for compitablity
        return MultiValueAttributeFormatter::GetEncodedCountLength(0);
    } else {
        return value.length();
    }
}

} // namespace indexlibv2::index
