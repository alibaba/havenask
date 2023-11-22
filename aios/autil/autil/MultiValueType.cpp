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
#include "autil/MultiValueType.h"

#include <iomanip>

#include "autil/StringTokenizer.h"

using namespace std;

namespace autil {
namespace __detail {

SimpleFormat::SimpleFormat(const SimpleFormat &other)
    : _data(other._data), _count(other._count), _countLen(other._countLen) {}

SimpleFormat &SimpleFormat::operator=(const SimpleFormat &other) {
    _data = other._data;
    _count = other._count;
    _countLen = other._countLen;
    return *this;
}

void SimpleFormat::init(const void *buffer) {
    if (buffer != nullptr) {
        _data = reinterpret_cast<const char *>(buffer);
        size_t countLen = 0;
        _count = MultiValueFormatter::decodeCount(_data, countLen);
        _data += countLen;
        _countLen = countLen;
    }
}

void SimpleFormat::init(const void *buffer, uint32_t count) {
    _data = reinterpret_cast<const char *>(buffer);
    _count = count;
    _countLen = 0;
}

MultiStringFormat::MultiStringFormat(const MultiStringFormat &other)
    : _data(other._data), _count(other._count), _countLen(other._countLen), _offsetItemLen(other._offsetItemLen) {}

MultiStringFormat &MultiStringFormat::operator=(const MultiStringFormat &other) {
    _data = other._data;
    _count = other._count;
    _countLen = other._countLen;
    _offsetItemLen = other._offsetItemLen;
    return *this;
}

void MultiStringFormat::init(const void *buffer) {
    if (buffer == nullptr) {
        return;
    }
    _data = reinterpret_cast<const char *>(buffer);
    size_t countLen = 0;
    _count = MultiValueFormatter::decodeCount(_data, countLen);
    _data += countLen;
    _countLen = countLen;
    if (_count == 0 || MultiValueFormatter::isNull(_count)) {
        return;
    }
    _offsetItemLen = *reinterpret_cast<const int8_t *>(_data);
}

void MultiStringFormat::init(const void *buffer, uint32_t count) {
    if (buffer == nullptr) {
        return;
    }
    _data = reinterpret_cast<const char *>(buffer);
    _count = count;
    _countLen = 0;
    if (MultiValueFormatter::isNull(_count)) {
        return;
    }
    _offsetItemLen = *reinterpret_cast<const int8_t *>(_data);
}

uint32_t MultiStringFormat::getDataSize() const {
    if (unlikely(_data == nullptr)) {
        return 0;
    }
    if (_count == 0) {
        return sizeof(uint8_t);
    } else if (MultiValueFormatter::isNull(_count)) {
        return 0;
    } else {
        MultiChar mc(get(_count - 1));
        return (mc.getBaseAddress() + mc.length()) - _data;
    }
}

void MultiStringFormat::serialize(DataBuffer &dataBuffer) const {
    uint32_t length = _data == nullptr ? 0 : MultiValueFormatter::getEncodedCountLength(_count) + getDataSize();
    dataBuffer.write(length);
    if (length == 0) {
        return;
    }
    if (_count == 0) {
        // see xxxx://invalid/issue/23705044
        uint16_t zero = 0;
        dataBuffer.writeBytes(&zero, sizeof(zero));
    } else {
        char header[MultiValueFormatter::VALUE_COUNT_MAX_BYTES];
        size_t countLen = MultiValueFormatter::encodeCount(_count, header, MultiValueFormatter::VALUE_COUNT_MAX_BYTES);
        dataBuffer.writeBytes(header, countLen);
        if (length > countLen) {
            dataBuffer.writeBytes(_data, length - countLen);
        }
    }
}

} // namespace __detail

template <typename T>
bool MultiValueType<T>::operator==(const std::string &fieldOfInput) const {
    autil::StringTokenizer st(fieldOfInput,
                              std::string(1, MULTI_VALUE_DELIMITER),
                              autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

    if (size() == st.getNumTokens()) {
        for (size_t i = 0; i < st.getNumTokens(); ++i) {
            T valueOfInput;
            StrToT(st[i], valueOfInput);
            if (!CheckT(valueOfInput, (*this)[i])) {
                return false;
            }
        }
        return true;
    }
    return false;
}

template <typename T>
bool MultiValueType<T>::operator!=(const std::string &fieldOfInput) const {
    return !(*this == fieldOfInput);
}

#define INST_OPERATOR(Type)                                                                                            \
    template <>                                                                                                        \
    bool Type::operator==(const std::string &fieldOfInput) const;                                                      \
    template <>                                                                                                        \
    bool Type::operator!=(const std::string &fieldOfInput) const;

MULTI_VALUE_TYPE_MACRO_HELPER(INST_OPERATOR)

std::ostream &stringMultiTypeStream(std::ostream &stream, MultiChar value) {
    stream << string(value.data(), value.size());
    return stream;
}

template <typename T>
std::ostream &normalMultiTypeStream(std::ostream &stream, T value) {
    for (size_t i = 0; i < value.size(); ++i) {
        if (i != 0) {
            stream << MULTI_VALUE_DELIMITER;
        }
        stream << value[i];
    }
    return stream;
}

template <typename T>
std::ostream &intMultiTypeStream(std::ostream &stream, T value) {
    for (size_t i = 0; i < value.size(); ++i) {
        if (i != 0) {
            stream << MULTI_VALUE_DELIMITER;
        }
        stream << (int)value[i];
    }
    return stream;
}

template <typename T>
std::ostream &floatMultiTypeStream(std::ostream &stream, T value) {
    for (size_t i = 0; i < value.size(); ++i) {
        if (i != 0) {
            stream << MULTI_VALUE_DELIMITER;
        }
        stream << setprecision(6) << value[i];
    }
    return stream;
}

template <typename T>
std::ostream &doubleMultiTypeStream(std::ostream &stream, T value) {
    for (size_t i = 0; i < value.size(); ++i) {
        if (i != 0) {
            stream << MULTI_VALUE_DELIMITER;
        }
        stream << setprecision(15) << value[i];
    }
    return stream;
}

#define STREAM_FUNC(func, type)                                                                                        \
    std::ostream &operator<<(std::ostream &stream, type value) { return func(stream, value); }

STREAM_FUNC(intMultiTypeStream, MultiUInt8);
STREAM_FUNC(intMultiTypeStream, MultiInt8);
STREAM_FUNC(intMultiTypeStream, MultiBool);
STREAM_FUNC(normalMultiTypeStream, MultiInt16);
STREAM_FUNC(normalMultiTypeStream, MultiUInt16);
STREAM_FUNC(normalMultiTypeStream, MultiInt32);
STREAM_FUNC(normalMultiTypeStream, MultiUInt32);
STREAM_FUNC(normalMultiTypeStream, MultiInt64);
STREAM_FUNC(normalMultiTypeStream, MultiUInt64);
STREAM_FUNC(normalMultiTypeStream, MultiUInt128);
STREAM_FUNC(floatMultiTypeStream, MultiFloat);
STREAM_FUNC(doubleMultiTypeStream, MultiDouble);
STREAM_FUNC(stringMultiTypeStream, MultiChar);
STREAM_FUNC(normalMultiTypeStream, MultiString);

} // namespace autil
