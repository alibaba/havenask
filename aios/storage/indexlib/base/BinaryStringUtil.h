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

#include <string>

#include "autil/CommonMacros.h"

namespace indexlibv2 {

class BinaryStringUtil
{
private:
    BinaryStringUtil(const BinaryStringUtil&);
    BinaryStringUtil& operator=(const BinaryStringUtil&);

public:
    template <typename T>
    static std::string toInvertString(T t, bool isNull = false);

    template <typename T>
    static std::string toString(T t, bool isNull = false);

private:
    template <typename T>
    static std::string floatingNumToString(T t, bool isNull = false);
};

template <typename T>
inline std::string BinaryStringUtil::toInvertString(T t, bool isNull)
{
    t = ~t;
    std::string result = toString(t, true);
    // revert null
    if (isNull) {
        result[0] |= 0x02;
    }
    return result;
}

template <typename T>
inline std::string BinaryStringUtil::toString(T t, bool isNull)
{
    std::string result;
    size_t n = sizeof(T);
    result.resize(n + 1, 0);
    // use two bit to represent 2 dimension: signed bit & null
    if (t >= 0) {
        result[0] |= 0x01;
    }
    if (!isNull) {
        result[0] |= 0x02;
    }
    for (size_t i = 0; i < n; ++i) {
        result[i + 1] = *((char*)&t + n - i - 1);
    }
    return result;
}

template <>
inline std::string BinaryStringUtil::toString(double t, bool isNull)
{
    return floatingNumToString<double>(t, isNull);
}

template <>
inline std::string BinaryStringUtil::toString(float t, bool isNull)
{
    return floatingNumToString<float>(t, isNull);
}

template <>
inline std::string BinaryStringUtil::toInvertString<double>(double t, bool isNull)
{
    t = -t;
    std::string result = floatingNumToString<double>(t, true);
    if (isNull) {
        result[0] = 1;
    }
    return result;
}

template <>
inline std::string BinaryStringUtil::toInvertString<float>(float t, bool isNull)
{
    t = -t;
    std::string result = floatingNumToString<float>(t, true);
    if (isNull) {
        result[0] = 1;
    }
    return result;
}

template <typename T>
inline std::string BinaryStringUtil::floatingNumToString(T t, bool isNull)
{
    std::string result;
    size_t n = sizeof(T);
    result.resize(n + 1);
    result[0] = isNull ? 0 : 1;
    for (size_t i = 0; i < n; ++i) {
        result[i + 1] = *((char*)&t + n - i - 1);
    }

    if (unlikely(result[1] & 0x80)) {
        // negativte
        for (size_t i = 0; i < n; ++i) {
            result[i + 1] = ~result[i + 1];
        }
    } else {
        // positive
        result[1] |= 0x80;
    }
    return result;
}

} // namespace indexlibv2
