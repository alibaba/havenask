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

#include <stddef.h>
#include <stdint.h>
#include <limits>
#include <string>


namespace autil {

struct StringRef {
public:
    StringRef()
        : buf(NULL)
        , len(0)
    {}
    StringRef(char *buf_, size_t len_)
        : buf(buf_)
        , len(len_)
    {}
public:
    char *buf;
    size_t len;
};

class FormatInt
{
private:
    static const char DIGITS[];
public:
    FormatInt();
private:
    FormatInt(const FormatInt &);
    FormatInt& operator = (const FormatInt &);
public:
    template <typename T>
    StringRef format(T value);

    template <typename T>
    std::string toString(T v);
private:
    inline char *formatDecimal(uint64_t v) {
        char *end = _buffer + BUFFER_SIZE;
        while (v >= 100) {
            uint32_t index = static_cast<uint32_t>((v % 100) * 2);
            v /= 100;
            *--end = DIGITS[index+1];
            *--end = DIGITS[index];
        }
        if (v < 10) {
            *--end = static_cast<char>('0' + v);
            return end;
        }
        uint32_t index = static_cast<uint32_t>(v * 2);
        *--end = DIGITS[index+1];
        *--end = DIGITS[index];
        return end;
    }

    inline char *formatSigned(int64_t v) {
        uint64_t absValue = static_cast<uint64_t>(v);
        bool neg = v < 0;
        if (neg) {
            absValue = 0 - absValue;
        }
        char *str = formatDecimal(absValue);
        if (neg) {
            *--str = '-';
        }
        return str;
    }
private:
    enum {BUFFER_SIZE = std::numeric_limits<uint64_t>::digits10 + 3};
    mutable char _buffer[BUFFER_SIZE];
};

#define format_unsigned_value(T)                        \
    template <>                                         \
    inline StringRef FormatInt::format(T v) {           \
        char *str = formatDecimal(v);                   \
        size_t len = _buffer + BUFFER_SIZE - str;       \
        return StringRef(str, len);                     \
    }

#define format_signed_value(T)                          \
    template <>                                         \
    inline StringRef FormatInt::format(T v) {           \
        char *str = formatSigned(v);                    \
        size_t len = _buffer + BUFFER_SIZE - str;       \
        return StringRef(str, len);                     \
    }

format_unsigned_value(uint8_t)
format_unsigned_value(uint16_t)
format_unsigned_value(uint32_t)
format_unsigned_value(uint64_t)

format_signed_value(int8_t)
format_signed_value(int16_t)
format_signed_value(int32_t)
format_signed_value(int64_t)

template <typename T>
inline std::string FormatInt::toString(T v) {
    StringRef ref = format(v);
    return std::string(ref.buf, ref.len);
}

}

