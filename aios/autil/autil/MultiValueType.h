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
#include <cstring>
#include <functional>
#include <iostream>
#include <string>
#include <string_view>
#include <type_traits>

#include "autil/CommonMacros.h"
#include "autil/DataBuffer.h"
#include "autil/HashAlgorithm.h"
#include "autil/LongHashValue.h"
#include "autil/MultiValueFormatter.h"
#include "autil/Span.h"

namespace autil {
namespace __detail {
struct SimpleFormat {
    const char *_data = nullptr;
    uint32_t _count = 0;
    uint8_t _countLen = 0;

    SimpleFormat() = default;
    ~SimpleFormat() = default;

    SimpleFormat(const SimpleFormat &other);
    SimpleFormat &operator=(const SimpleFormat &other);

    void init(const void *buffer);
    void init(const void *buffer, uint32_t count);

    uint32_t getCount() const { return _count; }
    const char *getData() const { return _data; }
    const char *getBaseAddress() const { return _data ? _data - _countLen : nullptr; }
};

struct MultiStringFormat {
    const char *_data = nullptr;
    uint32_t _count = 0;
    uint8_t _countLen = 0;
    uint8_t _offsetItemLen = 0;

    MultiStringFormat() = default;
    ~MultiStringFormat() = default;

    MultiStringFormat(const MultiStringFormat &other);
    MultiStringFormat &operator=(const MultiStringFormat &other);

    void init(const void *buffer);
    void init(const void *buffer, uint32_t count);
    uint32_t getCount() const { return _count; }
    const char *getData() const { return _data; }
    const char *get(size_t idx) const {
        assert(idx < _count);
        const char *offsets = _data + 1;
        uint32_t offset = MultiValueFormatter::getOffset(offsets, _offsetItemLen, idx);
        const char *blobs = offsets + _offsetItemLen * _count;
        return blobs + offset;
    }
    const char *getBaseAddress() const { return _data ? _data - _countLen : nullptr; }
    uint32_t getDataSize() const;
    void serialize(DataBuffer &dataBuffer) const;
};
} // namespace __detail

template <typename T>
struct IsMultiType;

const char MULTI_VALUE_DELIMITER = '\x1D';

template <typename T>
class MultiValueType {
private:
    class SimpleIterator {
    public:
        SimpleIterator(const char *data, const uint32_t size) : _data(data), _size(size), _idx(0) {}

    public:
        size_t size() const { return _size; }
        bool hasNext() const { return _idx < size(); }
        T next() {
            assert(_idx < size());
            return reinterpret_cast<const T *>(_data)[_idx++];
        }

    private:
        const char *_data = nullptr;
        const uint32_t _size = 0;
        uint32_t _idx = 0;
    };

    class MultiStringIterator {
    public:
        MultiStringIterator(const __detail::MultiStringFormat &format, uint32_t size)
            : _format(format), _size(size), _idx(0) {}

    public:
        size_t size() const { return _size; }
        bool hasNext() const { return _idx < size(); }
        MultiValueType<char> next() {
            assert(_idx < size());
            const char *addr = _format.get(_idx++);
            return MultiValueType<char>(addr);
        }

    private:
        const __detail::MultiStringFormat &_format;
        const uint32_t _size = 0;
        uint32_t _idx = 0;
    };

public:
    template <typename U>
    static constexpr bool IsMultiString = std::is_same<U, MultiValueType<char>>::value;
    using Format =
        typename std::conditional<IsMultiString<T>, __detail::MultiStringFormat, __detail::SimpleFormat>::type;
    using Iterator = typename std::conditional<IsMultiString<T>, MultiStringIterator, SimpleIterator>::type;
    typedef T type;

public:
    MultiValueType() = default;
    MultiValueType(const void *buffer) { init(buffer); }
    MultiValueType(const void *data, uint32_t count) { init(data, count); }

    ~MultiValueType() = default;
    MultiValueType(const MultiValueType &other) : _format(other._format) {}
    MultiValueType &operator=(const MultiValueType &other) {
        _format = other._format;
        return *this;
    }

public:
    void init(const void *buffer) { _format.init(buffer); }
    void init(const void *buffer, uint32_t count) { _format.init(buffer, count); }

    MultiValueType<T> clone(autil::mem_pool::PoolBase *pool) const {
        uint32_t dataSize = this->getDataSize();
        if (dataSize == 0) {
            return MultiValueType<T>();
        }
        char *buf = (char *)pool->allocate(dataSize);
        memcpy(buf, this->getData(), dataSize);
        return MultiValueType<T>(buf, getCount());
    }

    Iterator createIterator() const {
        if constexpr (!IsMultiString<T>) {
            return Iterator(getData(), size());
        } else {
            return Iterator(_format, size());
        }
    }

    inline T operator[](uint32_t idx) const __attribute__((always_inline)) {
        if constexpr (!IsMultiString<T>) {
            return data()[idx];
        } else {
            const char *addr = _format.get(idx);
            return MultiValueType<char>((const void *)addr);
        }
    }

    inline uint32_t size() const __attribute__((always_inline)) {
        uint32_t count = getCount();
        return unlikely(isNull(count)) ? 0 : count;
    }

public:
    void serialize(DataBuffer &dataBuffer) const {
        if constexpr (!IsMultiString<T>) {
            uint32_t len = getDataSize() + getHeaderSize();
            dataBuffer.write(len);
            if (len > 0) {
                char header[MultiValueFormatter::VALUE_COUNT_MAX_BYTES];
                size_t countLen =
                    MultiValueFormatter::encodeCount(getCount(), header, MultiValueFormatter::VALUE_COUNT_MAX_BYTES);
                dataBuffer.writeBytes(header, countLen);
                if (len > countLen) {
                    const char *data = getData();
                    assert(data != nullptr);
                    dataBuffer.writeBytes(data, len - countLen);
                }
            }
        } else {
            _format.serialize(dataBuffer);
        }
    }

    void deserialize(DataBuffer &dataBuffer, autil::mem_pool::Pool *pool) {
        uint32_t len = 0;
        dataBuffer.read(len);
        void *buf = nullptr;
        if (len > 0) {
            if (pool != nullptr) {
                buf = pool->allocate(len);
                dataBuffer.readBytes(buf, len);
            } else {
                buf = (void *)dataBuffer.readNoCopy(len);
            }
        }
        init(buf);
    }

public:
    // for named requirements: Container
    // not all member types and methods can be implemented, for multi-value type should be immutable
    using value_type = T;

    template <typename T1 = T, typename = std::enable_if_t<!IsMultiString<T1>>>
    using const_reference = const T &;

    template <typename T1 = T, typename = std::enable_if_t<!IsMultiString<T1>>>
    using const_iterator = const T *;

    using difference_type = ssize_t;
    using size_type = size_t;

    template <typename T1 = T, typename = std::enable_if_t<!IsMultiString<T1>>>
    const T *begin() const {
        return data();
    }
    template <typename T1 = T, typename = std::enable_if_t<!IsMultiString<T1>>>
    const T *end() const {
        return data() + size();
    }
    template <typename T1 = T, typename = std::enable_if_t<!IsMultiString<T1>>>
    const T *cbegin() const {
        return data();
    }
    template <typename T1 = T, typename = std::enable_if_t<!IsMultiString<T1>>>
    const T *cend() const {
        return data() + size();
    }

    bool empty() const { return size() == 0; }

    bool isNull(uint32_t count) const { return MultiValueFormatter::isNull(count); }

    bool isNull() const { return isNull(getCount()); }

    const char *getData() const { return _format.getData(); }

    uint32_t getDataSize() const {
        if constexpr (!IsMultiString<T>) {
            return size() * sizeof(T);
        } else {
            return _format.getDataSize();
        }
    }

    inline const T *data() const __attribute__((always_inline)) {
        if constexpr (!IsMultiString<T>) {
            return reinterpret_cast<const T *>(getData());
        } else {
            return nullptr;
        }
    }

    // interface for indexlib, need remove
    const char *getBaseAddress() const { return _format.getBaseAddress(); }

    uint32_t length() const { return getDataSize() + (hasEncodedCount() ? getHeaderSize() : 0); }

    bool isEmptyData() const { return _format._data == nullptr; }
    bool hasEncodedCount() const { return _format._countLen > 0; }
    uint32_t getEncodedCountValue() const { return getCount(); }

public:
    bool operator==(const MultiValueType<T> &other) const {
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

    bool operator!=(const MultiValueType<T> &other) const { return !(*this == other); }

    bool operator>(const MultiValueType<T> &other) const {
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

    bool operator<(const MultiValueType<T> &other) const {
        uint32_t recordNum = size();
        uint32_t otherRecordNum = other.size();
        uint32_t size = recordNum < otherRecordNum ? recordNum : otherRecordNum;
        for (uint32_t i = 0; i < size; ++i) {
            if ((*this)[i] < other[i]) {
                return true;
            } else if ((*this)[i] > other[i]) {
                return false;
            }
        }
        if (recordNum < otherRecordNum) {
            return true;
        }
        return false;
    }

    bool operator>=(const MultiValueType<T> &other) const { return !(*this < other); }

    bool operator<=(const MultiValueType<T> &other) const { return !(*this > other); }

#define DEFINE_UNSUPPORTED_OPERATOR(OpType, ReturnType)                                                                \
    ReturnType operator OpType(const MultiValueType<T> &other) const {                                                 \
        assert(false);                                                                                                 \
        return ReturnType();                                                                                           \
    }

    DEFINE_UNSUPPORTED_OPERATOR(&&, bool);             // std::logical_and
    DEFINE_UNSUPPORTED_OPERATOR(||, bool);             // std::logical_or
    DEFINE_UNSUPPORTED_OPERATOR(&, MultiValueType<T>); // bit_and
    DEFINE_UNSUPPORTED_OPERATOR(|, MultiValueType<T>); // bit_or
    DEFINE_UNSUPPORTED_OPERATOR(^, MultiValueType<T>); // bit_xor
    DEFINE_UNSUPPORTED_OPERATOR(+, MultiValueType<T>); // plus
    DEFINE_UNSUPPORTED_OPERATOR(-, MultiValueType<T>); // minus
    DEFINE_UNSUPPORTED_OPERATOR(*, MultiValueType<T>); // multiplies
    DEFINE_UNSUPPORTED_OPERATOR(/, MultiValueType<T>); // divide
#undef DEFINE_UNSUPPORTED_OPERATOR

    bool operator==(const std::string &fieldOfInput) const;
    bool operator!=(const std::string &fieldOfInput) const;

private:
    uint32_t getCount() const { return _format.getCount(); }

    // for fixed size MultiValue, length should not include header
    uint32_t getHeaderSize() const {
        if (unlikely(_format._data == nullptr)) {
            return 0;
        }
        return MultiValueFormatter::getEncodedCountLength(getCount());
    }

private:
    Format _format;
};

////////////////////////////////////////////////////////////////////////////////////////
template <>
inline bool MultiValueType<char>::operator==(const std::string &fieldOfInput) const {
    if (_format._data == nullptr) {
        return false;
    }
    return std::string(data(), size()) == fieldOfInput;
}

template <>
inline bool MultiValueType<char>::operator!=(const std::string &fieldOfInput) const {
    return !(*this == fieldOfInput);
}

#define MULTI_VALUE_TYPE_MACRO_HELPER(MY_MACRO)                                                                        \
    MY_MACRO(MultiValueType<bool>);                                                                                    \
    MY_MACRO(MultiValueType<int8_t>);                                                                                  \
    MY_MACRO(MultiValueType<uint8_t>);                                                                                 \
    MY_MACRO(MultiValueType<int16_t>);                                                                                 \
    MY_MACRO(MultiValueType<uint16_t>);                                                                                \
    MY_MACRO(MultiValueType<int32_t>);                                                                                 \
    MY_MACRO(MultiValueType<uint32_t>);                                                                                \
    MY_MACRO(MultiValueType<int64_t>);                                                                                 \
    MY_MACRO(MultiValueType<uint64_t>);                                                                                \
    MY_MACRO(MultiValueType<autil::uint128_t>);                                                                        \
    MY_MACRO(MultiValueType<float>);                                                                                   \
    MY_MACRO(MultiValueType<double>);                                                                                  \
    MY_MACRO(MultiValueType<char>);                                                                                    \
    MY_MACRO(MultiValueType<MultiValueType<char>>);

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
} // namespace

#define MULTI_VALUE_TYPE_MACRO_HELPER_2(MY_MACRO)                                                                      \
    MY_MACRO(MultiValueType<bool>, Bool);                                                                              \
    MY_MACRO(MultiValueType<int8_t>, Int8);                                                                            \
    MY_MACRO(MultiValueType<uint8_t>, UInt8);                                                                          \
    MY_MACRO(MultiValueType<int16_t>, Int16);                                                                          \
    MY_MACRO(MultiValueType<uint16_t>, UInt16);                                                                        \
    MY_MACRO(MultiValueType<int32_t>, Int32);                                                                          \
    MY_MACRO(MultiValueType<uint32_t>, UInt32);                                                                        \
    MY_MACRO(MultiValueType<int64_t>, Int64);                                                                          \
    MY_MACRO(MultiValueType<uint64_t>, UInt64);                                                                        \
    MY_MACRO(MultiValueType<autil::uint128_t>, UInt128);                                                               \
    MY_MACRO(MultiValueType<float>, Float);                                                                            \
    MY_MACRO(MultiValueType<double>, Double);                                                                          \
    MY_MACRO(MultiValueType<char>, Char);                                                                              \
    MY_MACRO(MultiValueType<MultiValueType<char>>, String);

#define MULTI_VALUE_TYPEDEF_1(type, suffix) typedef type Multi##suffix;
MULTI_VALUE_TYPE_MACRO_HELPER_2(MULTI_VALUE_TYPEDEF_1);
#undef MULTI_VALUE_TYPEDEF_1

#define MULTI_VALUE_OSTREAM(type) std::ostream &operator<<(std::ostream &stream, type value);
MULTI_VALUE_TYPE_MACRO_HELPER(MULTI_VALUE_OSTREAM);
#undef MULTI_VALUE_OSTREAM

template <typename T>
struct IsMultiType {
    static constexpr bool value = false;
    typedef std::false_type type;
};

#define IS_MULTI_TYPE(m_type)                                                                                          \
    template <>                                                                                                        \
    struct IsMultiType<m_type> {                                                                                       \
        static constexpr bool value = true;                                                                            \
        typedef std::true_type type;                                                                                   \
    };
MULTI_VALUE_TYPE_MACRO_HELPER(IS_MULTI_TYPE);
#undef IS_MULTI_TYPE

} // namespace autil

namespace std {
template <>
struct hash<autil::MultiChar> {
    std::size_t operator()(const autil::MultiChar &key) const {
        return autil::HashAlgorithm::hashString(key.data(), key.size(), 0);
    }
};

template <>
struct equal_to<autil::MultiChar> : public std::binary_function<autil::MultiChar, autil::MultiChar, bool> {
    bool operator()(const autil::MultiChar &left, const autil::MultiChar &right) const {
        size_t leftLen = left.size(), rightLen = right.size();
        if (leftLen != rightLen) {
            return false;
        }
        return memcmp(left.data(), right.data(), leftLen) == 0;
    }
};
} // namespace std

namespace autil {
template <typename T>
struct SpanAdapter<MultiValueType<T>, std::enable_if_t<!std::is_same_v<T, MultiChar>>> {
    [[gnu::always_inline]] inline static ConstSpan<T> from(const MultiValueType<T> &other) {
        return {other.data(), (std::size_t)other.size()};
    }

    [[gnu::always_inline]] inline static MultiValueType<T> to(const ConstSpan<T> &other) {
        return {other.data(), (uint32_t)other.size()};
    }
};

} // namespace autil
