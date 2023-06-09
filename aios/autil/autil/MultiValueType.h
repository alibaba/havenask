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

#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>
#include <type_traits>
#include <cstring>
#include <functional>
#include <string_view>

#include "autil/CommonMacros.h"
#include "autil/LongHashValue.h"
#include "autil/MultiValueFormatter.h"
#include "autil/HashAlgorithm.h"
#include "autil/Span.h"

namespace autil {
template <typename T> struct IsMultiType;

const char MULTI_VALUE_DELIMITER = '\x1D';

template <typename T>
class MultiValueType
{
public:
    typedef T type;

public:
    class Iterator {
    public:
        Iterator(const char *data, const uint32_t size)
            : _data(data)
            , _size(size)
            , _idx(0)
        {
        }
    public:
        size_t size() const { return _size; }
        bool hasNext() const { return _idx < size(); }
        T next() {
            assert(_idx < size());
            return reinterpret_cast<const T*>(_data)[_idx++];
        }
    private:
        const char *_data = nullptr;
        const uint32_t _size = 0;
        uint32_t _idx = 0;
    };

public:
    MultiValueType(const void* buffer = NULL) {
        init(buffer);
    }

    MultiValueType(const void *data, uint32_t count) {
        init(data, count);
    }

    MultiValueType(const MultiValueType &other)
        : _data(other._data)
        , _count(other._count)
    {
    }

    MultiValueType<T>& operator = (const MultiValueType<T>& other) {
        if (this != &other) {
            _data = other._data;
            _count = other._count;
        }
        return *this;
    }

    ~MultiValueType() = default;

public:
    void init(const void* buffer) {
        if (buffer != nullptr) {
            _data = reinterpret_cast<const char*>(buffer);
            setUndecoded();
        } else {
            _data = nullptr;
            _count = 0;
        }
    }

    void init(const void *buffer, uint32_t count) {
        _data = reinterpret_cast<const char*>(buffer);
        _count = count;
    }

    Iterator createIterator() const {
        return Iterator(getData(), size());
    }

    inline T operator[](uint32_t idx) const __attribute__((always_inline)) {
        return data()[idx];
    }

    inline uint32_t size() const __attribute__((always_inline)) {
        uint32_t count = getCount();
        return unlikely(isNull(count)) ? 0 : count;
    }

public:
    // for named requirements: Container
    // not all member types and methods can be implemented, for multi-value type should be immutable
    using value_type = T;

    template <typename T1 = T, typename = std::enable_if_t<!std::is_same_v<T1, autil::MultiValueType<char>>>>
    using const_reference = const T&;

    template <typename T1 = T, typename = std::enable_if_t<!std::is_same_v<T1, autil::MultiValueType<char>>>>
    using const_iterator = const T *;

    using difference_type = ssize_t;
    using size_type = size_t;

    template <typename T1 = T, typename = std::enable_if_t<!std::is_same_v<T1, autil::MultiValueType<char>>>>
    const T *begin() const { return data(); }
    template <typename T1 = T, typename = std::enable_if_t<!std::is_same_v<T1, autil::MultiValueType<char>>>>
    const T *end() const { return data() + size(); }
    template <typename T1 = T, typename = std::enable_if_t<!std::is_same_v<T1, autil::MultiValueType<char>>>>
    const T *cbegin() const { return data(); }
    template <typename T1 = T, typename = std::enable_if_t<!std::is_same_v<T1, autil::MultiValueType<char>>>>
    const T *cend() const { return data() + size(); }

    bool empty() const { return size() == 0; }

    bool isNull(uint32_t count) const {
        return count == MultiValueFormatter::VAR_NUM_NULL_FIELD_VALUE_COUNT;
    }

    bool isNull() const {
        return getCount() == MultiValueFormatter::VAR_NUM_NULL_FIELD_VALUE_COUNT;
    }

    const char *getData() const {
        if (likely(isUndecoded())) {
            size_t countLen = MultiValueFormatter::getEncodedCountFromFirstByte(
                    *(const uint8_t*)_data);
            return _data + countLen;
        }
        return _data;
    }

    uint32_t getDataSize() const {
        return size() * sizeof(T);
    }

    inline const T* data() const __attribute__((always_inline)) {
        return reinterpret_cast<const T*>(getData());
    }

    // interface for indexlib, need remove
    const char *getBaseAddress() const {
        return _data;
    }

    uint32_t length() const {
        return getDataSize() + (isUndecoded() ? getHeaderSize() : 0);
    }

    bool isEmptyData() const { return _data == nullptr; }
    bool hasEncodedCount() const { return isUndecoded(); }
    uint32_t getEncodedCountValue() const { return getCount(); }
    
public:
    bool operator == (const MultiValueType<T>& other) const {
        uint32_t recordNum = size();
        if (recordNum != other.size()) {
            return false;
        }
        if (isNull() != other.isNull()) {
            return false;
        }
        for (uint32_t i = 0; i < recordNum; ++i) {
            if ((*this)[i] != other[i]) {
                return false;
            }
        }
        return true;
    }

    bool operator != (const MultiValueType<T>& other) const {
        return !(*this == other);
    }

    bool operator > (const MultiValueType<T>& other) const {
        uint32_t recordNum = size();
        uint32_t otherRecordNum = other.size();
        uint32_t size = recordNum < otherRecordNum ? recordNum : otherRecordNum;
        for (uint32_t i = 0; i < size; ++i) {
            if ((*this)[i] > other[i]) {
                return true;
            } else if ((*this)[i] < other[i]) {
                return false;
            }
        }
        if (recordNum > otherRecordNum) {
            return true;
        }
        return false;
    }

    bool operator < (const MultiValueType<T>& other) const {
        uint32_t recordNum = size();
        uint32_t otherRecordNum = other.size();
        uint32_t size = recordNum < otherRecordNum ? recordNum : otherRecordNum;
        for (uint32_t i = 0; i < size; ++i) {
            if ((*this)[i] < other[i]) {
                return true;
            }
            else if ((*this)[i] > other[i]) {
                return false;
            }
        }
        if (recordNum < otherRecordNum) {
            return true;
        }
        return false;
    }

    bool operator >= (const MultiValueType<T>& other) const {
        return !(*this < other);
    }

    bool operator <= (const MultiValueType<T>& other) const {
        return !(*this > other);
    }

#define DEFINE_UNSUPPORTED_OPERATOR(OpType, ReturnType)                 \
    ReturnType operator OpType(const MultiValueType<T>& other) const    \
    {                                                                   \
        assert(false);                                                  \
        return ReturnType();                                            \
    }

    DEFINE_UNSUPPORTED_OPERATOR(&&, bool); // std::logical_and
    DEFINE_UNSUPPORTED_OPERATOR(||, bool); // std::logical_or
    DEFINE_UNSUPPORTED_OPERATOR(&, MultiValueType<T>); // bit_and
    DEFINE_UNSUPPORTED_OPERATOR(|, MultiValueType<T>); // bit_or
    DEFINE_UNSUPPORTED_OPERATOR(^, MultiValueType<T>); // bit_xor
    DEFINE_UNSUPPORTED_OPERATOR(+, MultiValueType<T>); // plus
    DEFINE_UNSUPPORTED_OPERATOR(-, MultiValueType<T>); // minus
    DEFINE_UNSUPPORTED_OPERATOR(*, MultiValueType<T>); // multiplies
    DEFINE_UNSUPPORTED_OPERATOR(/, MultiValueType<T>); // divide
#undef DEFINE_UNSUPPORTED_OPERATOR

    bool operator == (const std::string& fieldOfInput) const;
    bool operator != (const std::string& fieldOfInput) const;

private:
    uint32_t getCount() const {
        if (likely(isUndecoded())) {
            size_t countLen = 0;
            return MultiValueFormatter::decodeCount(_data, countLen);
        }
        return _count;
    }

    // for fixed size MultiValue, length should not include header
    uint32_t getHeaderSize() const {
        if (unlikely(_data == nullptr)) {
            return 0;
        }
        return MultiValueFormatter::getEncodedCountLength(getCount());
    }

    void setUndecoded() {
        _count = MultiValueFormatter::UNDECODED_COUNT;
    }

    inline bool isUndecoded() const __attribute__((always_inline)) {
        return _count == MultiValueFormatter::UNDECODED_COUNT;
    }

private:
    const char *_data = nullptr;
    uint32_t _count = 0;
    friend class DataBuffer;
};

class MultiStringDecoder {
public:
    MultiStringDecoder(const char *data, const uint32_t size)
        : _data(data)
        , _size(size)
    {}
public:
    MultiValueType<char> get(uint32_t idx) const {
        assert(idx < _size);
        uint8_t offsetItemLen = *reinterpret_cast<const uint8_t *>(_data);
        const char* offsetAddr = _data + sizeof(uint8_t);
        uint32_t offset = MultiValueFormatter::getOffset(offsetAddr, offsetItemLen, idx);
        const char* strDataAddr = offsetAddr + offsetItemLen * _size + offset;
        return MultiValueType<char>(strDataAddr);
    }
private:
    const char *_data;
    const uint32_t _size;
};

////////////////////////////////////////////////////////////////////////////////////////
template <>
inline bool MultiValueType<char>::operator == (const std::string& fieldOfInput) const {
    if (_data == nullptr) {
        return false;
    }
    return std::string(data(), size()) == fieldOfInput;
}

template <>
inline bool MultiValueType<char>::operator != (const std::string& fieldOfInput) const {
    return !(*this == fieldOfInput);
}

template<>
inline MultiValueType<char> MultiValueType<MultiValueType<char>>::Iterator::next() {
    MultiStringDecoder decoder(this->_data, this->_size);
    return decoder.get(this->_idx++);
}

template <>
inline MultiValueType<char> MultiValueType<MultiValueType<char> >::operator [] (uint32_t idx) const {
    assert(_data != nullptr);
    size_t recordNum = size();
    assert(idx < recordNum);
    MultiStringDecoder decoder(getData(), recordNum);
    return decoder.get(idx);
}

template <>
inline const MultiValueType<char>* MultiValueType<MultiValueType<char>>::data() const {
    return nullptr;
}

template <>
inline uint32_t MultiValueType<MultiValueType<char>>::getDataSize() const {
    if (unlikely(_data == nullptr)) {
        return 0;
    }
    uint32_t count = getCount();
    const char *data = getData();
    if (count == 0) {
        return sizeof(uint8_t);
    } else if (isNull(count)) {
        return 0;
    } else {
        assert(hasEncodedCount());
        MultiStringDecoder decoder(data, count);
        MultiValueType<char> last = decoder.get(count - 1);
        assert(last.hasEncodedCount());        
        return last.getBaseAddress() + last.length() - data;
    }
}

#define MULTI_VALUE_TYPE_MACRO_HELPER(MY_MACRO)         \
    MY_MACRO(MultiValueType<bool>);                     \
    MY_MACRO(MultiValueType<int8_t>);                   \
    MY_MACRO(MultiValueType<uint8_t>);                  \
    MY_MACRO(MultiValueType<int16_t>);                  \
    MY_MACRO(MultiValueType<uint16_t>);                 \
    MY_MACRO(MultiValueType<int32_t>);                  \
    MY_MACRO(MultiValueType<uint32_t>);                 \
    MY_MACRO(MultiValueType<int64_t>);                  \
    MY_MACRO(MultiValueType<uint64_t>);                 \
    MY_MACRO(MultiValueType<autil::uint128_t>);          \
    MY_MACRO(MultiValueType<float>);                    \
    MY_MACRO(MultiValueType<double>);                   \
    MY_MACRO(MultiValueType<char>);                     \
    MY_MACRO(MultiValueType<MultiValueType<char> >);

// define identifiers
namespace {
enum {
    Bool,
    Int8,
    Uint8,
    Int16,
    Uint16,
    Int32,
    UInt32,
    Int64,
    Uint64,
    Uint128,
    Float,
    Double,
    Char,
    String,
};
}

#define MULTI_VALUE_TYPE_MACRO_HELPER_2(MY_MACRO)               \
    MY_MACRO(MultiValueType<bool>, Bool);                       \
    MY_MACRO(MultiValueType<int8_t>, Int8);                     \
    MY_MACRO(MultiValueType<uint8_t>, UInt8);                   \
    MY_MACRO(MultiValueType<int16_t>, Int16);                   \
    MY_MACRO(MultiValueType<uint16_t>, UInt16);                 \
    MY_MACRO(MultiValueType<int32_t>, Int32);                   \
    MY_MACRO(MultiValueType<uint32_t>, UInt32);                 \
    MY_MACRO(MultiValueType<int64_t>, Int64);                   \
    MY_MACRO(MultiValueType<uint64_t>, UInt64);                 \
    MY_MACRO(MultiValueType<autil::uint128_t>, UInt128);        \
    MY_MACRO(MultiValueType<float>, Float);                     \
    MY_MACRO(MultiValueType<double>, Double);                   \
    MY_MACRO(MultiValueType<char>, Char);                       \
    MY_MACRO(MultiValueType<MultiValueType<char> >, String);

#define MULTI_VALUE_TYPEDEF_1(type, suffix)             \
    typedef type Multi##suffix;
MULTI_VALUE_TYPE_MACRO_HELPER_2(MULTI_VALUE_TYPEDEF_1);
#undef MULTI_VALUE_TYPEDEF_1

#define MULTI_VALUE_TYPEDEF_2(type, suffix)             \
    typedef type FixedNum##suffix;
MULTI_VALUE_TYPE_MACRO_HELPER_2(MULTI_VALUE_TYPEDEF_2);
#undef MULTI_VALUE_TYPEDEF_2

#define MULTI_VALUE_OSTREAM(type)             \
    std::ostream& operator <<(std::ostream& stream, type value);
MULTI_VALUE_TYPE_MACRO_HELPER(MULTI_VALUE_OSTREAM);
#undef MULTI_VALUE_OSTREAM

template<typename T>
struct IsMultiType {
    typedef std::false_type type;
};

#define IS_MULTI_TYPE(m_type)                   \
    template<>                                  \
    struct IsMultiType<m_type> {                \
        typedef std::true_type type;            \
    };
MULTI_VALUE_TYPE_MACRO_HELPER(IS_MULTI_TYPE);
#undef IS_MULTI_TYPE

}

namespace std {

template<>
struct hash<autil::MultiChar> {
    std::size_t operator() (const autil::MultiChar &key) const {
        return autil::HashAlgorithm::hashString(key.data(), key.size(), 0);
    }
};

template<>
struct equal_to<autil::MultiChar>
    : public std::binary_function<autil::MultiChar, autil::MultiChar, bool>
{
    bool operator() (const autil::MultiChar &left, const autil::MultiChar &right) const {
        size_t leftLen = left.size(), rightLen = right.size();
        if (leftLen != rightLen) {
            return false;
        }
        return memcmp(left.data(), right.data(), leftLen) == 0;
    }
};

}

namespace autil {

template <typename T>
struct SpanAdapter<MultiValueType<T>, 
    std::enable_if_t<!std::is_same_v<T, MultiChar>>> {

    [[gnu::always_inline]] inline static ConstSpan<T> from(
        const MultiValueType<T> &other) {
        return { other.data(), (std::size_t)other.size() };
    }

    [[gnu::always_inline]] inline static MultiValueType<T> to(
        const ConstSpan<T> &other) {
        return { other.data(), (uint32_t)other.size() };
    }
};

}
