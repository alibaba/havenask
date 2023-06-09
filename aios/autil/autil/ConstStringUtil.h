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

#include <assert.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/Span.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"

namespace autil {

class ConstStringUtil {
public:
    template <typename T>
    static bool fromString(const StringView *value, T &val, mem_pool::Pool *pool);

    template <typename T>
    static bool fromString(const StringView *value, MultiValueType<T> &val, mem_pool::Pool *pool);

    template <typename T>
    static T fromString(const StringView &str);

    template <typename T>
    static bool fromString(const StringView &str, T &val);

    static std::vector<StringView> split(const StringView &text, const std::string &sepStr, bool ignoreEmpty = true);
    static std::vector<StringView> split(const StringView &text, const char &sepChar, bool ignoreEmpty = true);
    static void
    split(std::vector<StringView> &vec, const StringView &text, const std::string &sepStr, bool ignoreEmpty = true);
    static void
    split(std::vector<StringView> &vec, const StringView &text, const char &sepChar, bool ignoreEmpty = true);
};

template <typename T>
inline T ConstStringUtil::fromString(const StringView &str) {
    T value;
    fromString(str, value);
    return value;
}

template <typename T>
inline bool ConstStringUtil::fromString(const StringView &str, T &value) {
    return StringUtil::fromString(std::string(str), value);
}

template <>
inline bool ConstStringUtil::fromString(const StringView &str, std::string &value) {
    value = std::string(str);
    return true;
}

template <>
inline bool ConstStringUtil::fromString(const StringView &str, int8_t &value) {
    return StringUtil::fromString(std::string(str), value);
}

template <>
inline bool ConstStringUtil::fromString(const StringView &str, uint8_t &value) {
    return StringUtil::fromString(std::string(str), value);
}

template <>
inline bool ConstStringUtil::fromString(const StringView &str, int16_t &value) {
    return StringUtil::fromString(std::string(str), value);
}

template <>
inline bool ConstStringUtil::fromString(const StringView &str, uint16_t &value) {
    return StringUtil::fromString(std::string(str), value);
}

template <>
inline bool ConstStringUtil::fromString(const StringView &str, int32_t &value) {
    return StringUtil::fromString(std::string(str), value);
}

template <>
inline bool ConstStringUtil::fromString(const StringView &str, uint32_t &value) {
    return StringUtil::fromString(std::string(str), value);
}

template <>
inline bool ConstStringUtil::fromString(const StringView &str, int64_t &value) {
    return StringUtil::fromString(std::string(str), value);
}

template <>
inline bool ConstStringUtil::fromString(const StringView &str, uint64_t &value) {
    return StringUtil::fromString(std::string(str), value);
}

template <>
inline bool ConstStringUtil::fromString(const StringView &str, float &value) {
    return StringUtil::fromString(std::string(str), value);
}

template <>
inline bool ConstStringUtil::fromString(const StringView &str, double &value) {
    return StringUtil::fromString(std::string(str), value);
}

template <>
inline bool ConstStringUtil::fromString(const StringView &str, bool &value) {
    return StringUtil::fromString(std::string(str), value);
}

template <typename T>
inline bool ConstStringUtil::fromString(const StringView *value, T &val, mem_pool::Pool *pool) {
    assert(value);
    return StringUtil::fromString<T>(std::string(*value), val);
}

template <typename T>
inline bool ConstStringUtil::fromString(const StringView *value, MultiValueType<T> &val, mem_pool::Pool *pool) {
    assert(value);
    std::vector<T> valVec;
    StringUtil::fromString(std::string(*value), valVec, std::string(1, MULTI_VALUE_DELIMITER));
    char *buffer = MultiValueCreator::createMultiValueBuffer<T>(valVec, pool);
    if (buffer == nullptr) {
        return false;
    }
    val.init(buffer);
    return true;
}

template <>
inline bool ConstStringUtil::fromString<char>(const StringView *value, MultiChar &val, mem_pool::Pool *pool) {
    assert(value);
    char *buffer = MultiValueCreator::createMultiValueBuffer(value->data(), value->size(), pool);
    if (buffer == nullptr) {
        return false;
    }
    val.init(buffer);
    return true;
}

template <>
inline bool
ConstStringUtil::fromString<MultiChar>(const StringView *value, autil::MultiString &val, mem_pool::Pool *pool) {
    assert(value);
    char *buffer = nullptr;
    if (value->empty()) {
        std::vector<std::string> vec;
        buffer = autil::MultiValueCreator::createMultiValueBuffer(vec, pool);
        val.init(buffer);
    } else {
        std::vector<autil::StringView> vec = autil::StringTokenizer::constTokenize(
            *value, autil::MULTI_VALUE_DELIMITER, autil::StringTokenizer::TOKEN_LEAVE_AS);
        buffer = autil::MultiValueCreator::createMultiValueBuffer(vec, pool);
    }
    if (buffer == nullptr) {
        return false;
    }
    val.init(buffer);
    return true;
}

} // namespace autil
