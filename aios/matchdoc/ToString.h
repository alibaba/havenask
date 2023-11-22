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
#include <type_traits>
#include <vector>

#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"

namespace matchdoc {
namespace __detail {

template <typename T, typename = T>
struct ToStringImpl : std::false_type {
    static std::string toString(const T &x) { return ""; }
};

template <typename T>
static constexpr bool IsAtomicType =
    std::is_enum<T>::value || std::is_arithmetic<T>::value || std::is_same<T, std::string>::value;

template <typename T>
struct ToStringImpl<T, typename std::enable_if<IsAtomicType<T>, T>::type> : std::true_type {
    static std::string toString(const T &t) { return autil::StringUtil::toString(t); }
};

template <typename T>
struct ToStringImpl<T, typename std::enable_if<autil::IsMultiType<T>::value, T>::type> : std::true_type {
    static std::string toString(const T &t) { return autil::StringUtil::toString(t); }
};

template <typename T>
struct ToStringImpl<std::vector<T>,
                    typename std::enable_if<IsAtomicType<T> && std::is_same<decltype(autil::StringUtil::toString(
                                                                                std::declval<const std::vector<T> &>(),
                                                                                std::declval<const std::string &>())),
                                                                            std::string>::value,
                                            std::vector<T>>::type> : std::true_type {
    static std::string toString(const std::vector<T> &val) {
        static const std::string DELIM(1, autil::MULTI_VALUE_DELIMITER);
        return autil::StringUtil::toString(val, DELIM);
    }
};

template <typename T>
struct ToStringImpl<
    std::vector<std::vector<T>>,
    typename std::enable_if<IsAtomicType<T> && std::is_same<decltype(autil::StringUtil::toString(
                                                                std::declval<const std::vector<std::vector<T>> &>(),
                                                                std::declval<const std::string &>(),
                                                                std::declval<const std::string &>())),
                                                            std::string>::value,
                            std::vector<std::vector<T>>>::type> : std::true_type {
    static std::string toString(const std::vector<std::vector<T>> &val) {
        static const std::string DELIM1 = "\x1D";
        static const std::string DELIM2 = "\x1E";
        return autil::StringUtil::toString(val, DELIM1, DELIM2);
    }
};

// T.toString();
template <typename T>
struct ToStringImpl<
    T,
    typename std::enable_if<std::is_same<decltype(std::declval<T>().toString()), std::string>::value, T>::type>
    : std::true_type {
    static std::string toString(const T &val) { return val.toString(); }
};

// T::toString(const T &t)
template <typename T>
struct ToStringImpl<
    T,
    typename std::enable_if<std::is_same<decltype(T::toString(std::declval<const T &>())), std::string>::value,
                            T>::type> : std::true_type {
    static std::string toString(const T &val) { return T::toString(val); }
};

} // namespace __detail
} // namespace matchdoc
