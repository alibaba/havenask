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

#include <map>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "autil/LongHashValue.h"
#include "autil/Span.h"

namespace autil {

class DataBuffer;

namespace mem_pool {
class Pool;
}

namespace __detail {

template <typename T, typename = T>
struct HasSerialize : std::false_type {};

template <typename T>
struct HasSerialize<
    T,
    typename std::enable_if<
        std::is_same<decltype(std::declval<T>().serialize(std::declval<autil::DataBuffer &>())), void>::value,
        T>::type> : std::true_type {};

template <typename T, typename = T>
struct HasSimpleDeserialize : std::false_type {};

template <typename T>
struct HasSimpleDeserialize<
    T,
    typename std::enable_if<
        std::is_same<decltype(std::declval<T>().deserialize(std::declval<autil::DataBuffer &>())), void>::value,
        T>::type> : std::true_type {};

template <typename T, typename = T>
struct HasPoolDeserialize : std::false_type {};

template <typename T>
struct HasPoolDeserialize<T,
                          typename std::enable_if<std::is_same<decltype(std::declval<T>().deserialize(
                                                                   std::declval<autil::DataBuffer &>(),
                                                                   std::declval<autil::mem_pool::Pool *>())),
                                                               void>::value,
                                                  T>::type> : std::true_type {};

template <typename T, typename = T>
struct HasDeserialize : std::false_type {};

template <typename T>
struct HasDeserialize<T,
                      typename std::enable_if<HasSimpleDeserialize<T>::value || HasPoolDeserialize<T>::value, T>::type>
    : std::true_type {};

template <typename T>
static constexpr bool IsBuiltinSupportType =
    std::is_enum<T>::value || std::is_arithmetic<T>::value || std::is_same<std::string, T>::value ||
    std::is_same<StringView, T>::value || std::is_same<uint128_t, T>::value;

template <typename T, typename = T>
struct SupportSerialize : std::false_type {};

template <typename T, typename = T>
struct SupportDeserialize : std::false_type {};

// atomic types
template <typename T>
struct SupportSerialize<
    T,
    typename std::enable_if<__detail::IsBuiltinSupportType<T> || __detail::HasSerialize<T>::value, T>::type>
    : std::true_type {};

template <typename T>
struct SupportDeserialize<
    T,
    typename std::enable_if<__detail::IsBuiltinSupportType<T> || __detail::HasDeserialize<T>::value, T>::type>
    : std::true_type {};

// raw pointer
template <typename T>
struct SupportSerialize<T *, typename std::enable_if<SupportSerialize<T>::value, T *>::type> : std::true_type {};

template <typename T>
struct SupportDeserialize<T *, typename std::enable_if<SupportDeserialize<T>::value, T *>::type> : std::true_type {};

// unique_ptr
template <typename T>
struct SupportSerialize<std::unique_ptr<T>,
                        typename std::enable_if<SupportSerialize<T>::value, std::unique_ptr<T>>::type>
    : std::true_type {};

template <typename T>
struct SupportDeserialize<std::unique_ptr<T>,
                          typename std::enable_if<SupportDeserialize<T>::value, std::unique_ptr<T>>::type>
    : std::true_type {};

// shared_ptr
template <typename T>
struct SupportSerialize<std::shared_ptr<T>,
                        typename std::enable_if<SupportSerialize<T>::value, std::shared_ptr<T>>::type>
    : std::true_type {};

template <typename T>
struct SupportDeserialize<std::shared_ptr<T>,
                          typename std::enable_if<SupportDeserialize<T>::value, std::shared_ptr<T>>::type>
    : std::true_type {};

// std::pair
template <typename T>
struct IsPair : std::false_type {};

template <typename F, typename S>
struct IsPair<std::pair<F, S>> : std::true_type {};

template <typename T>
struct SupportSerialize<T,
                        typename std::enable_if<IsPair<T>::value && SupportSerialize<typename T::first_type>::value &&
                                                    SupportSerialize<typename T::second_type>::value,
                                                T>::type> : std::true_type {};

template <typename T>
struct SupportDeserialize<
    T,
    typename std::enable_if<IsPair<T>::value && SupportDeserialize<typename T::first_type>::value &&
                                SupportDeserialize<typename T::second_type>::value,
                            T>::type> : std::true_type {};

// std::vector
template <typename T>
struct SupportSerialize<std::vector<T>, typename std::enable_if<SupportSerialize<T>::value, std::vector<T>>::type>
    : std::true_type {};

template <typename T>
struct SupportDeserialize<std::vector<T>, typename std::enable_if<SupportDeserialize<T>::value, std::vector<T>>::type>
    : std::true_type {};

// set
template <typename T>
struct SupportSerialize<std::set<T>, typename std::enable_if<SupportSerialize<T>::value, std::set<T>>::type>
    : std::true_type {};

template <typename T>
struct SupportDeserialize<std::set<T>, typename std::enable_if<SupportDeserialize<T>::value, std::set<T>>::type>
    : std::true_type {};

template <typename T>
struct SupportSerialize<std::unordered_set<T>,
                        typename std::enable_if<SupportSerialize<T>::value, std::unordered_set<T>>::type>
    : std::true_type {};

template <typename T>
struct SupportDeserialize<std::unordered_set<T>,
                          typename std::enable_if<SupportDeserialize<T>::value, std::unordered_set<T>>::type>
    : std::true_type {};

// map
template <typename T>
struct IsMap : std::false_type {};

template <typename K, typename V>
struct IsMap<std::map<K, V>> : std::true_type {};

template <typename K, typename V>
struct IsMap<std::unordered_map<K, V>> : std::true_type {};

template <typename T>
struct SupportSerialize<T,
                        typename std::enable_if<IsMap<T>::value && SupportSerialize<typename T::key_type>::value &&
                                                    SupportSerialize<typename T::mapped_type>::value,
                                                T>::type> : std::true_type {};

template <typename T>
struct SupportDeserialize<T,
                          typename std::enable_if<IsMap<T>::value && SupportDeserialize<typename T::key_type>::value &&
                                                      SupportDeserialize<typename T::mapped_type>::value,
                                                  T>::type> : std::true_type {};

} // namespace __detail

template <typename T>
struct SerializeDeserializeTraits {
    static constexpr bool CAN_SERIALIZE = __detail::SupportSerialize<T>::value;
    static constexpr bool CAN_DESERIALIZE = __detail::SupportDeserialize<T>::value;
};

} // namespace autil
