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

#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <tuple>
#include <type_traits>

namespace indexlibv2 { namespace util {
template <typename Alloc>
struct AllocatorHasTrivialDeallocate : std::false_type {
};

template <typename T>
struct AllocatorHasTrivialDeallocate<std::allocator<T>> : std::false_type {
};

template <size_t... Ns>
struct Number2Type {
};

template <size_t N>
struct Number2Type<N> {
    static constexpr size_t Number = N;
};

template <typename... Ts>
struct Type2Type {
    static constexpr std::size_t length() { return sizeof...(Ts); }
};

template <typename T>
struct Type2Type<T> {
    typedef T type;
};

template <typename T>
struct StaticTypeNot : std::integral_constant<bool, !T::value> {
};

template <typename... Ts>
struct StaticTypeAnd : std::is_same<Number2Type<Ts::value...>, Number2Type<(Ts::value || true)...>> {
};

template <typename... Ts>
struct StaticTypeOr : StaticTypeNot<std::is_same<Number2Type<Ts::value...>, Number2Type<(Ts::value && false)...>>> {
};

template <typename T, typename... Ts>
struct IsOneOf : StaticTypeOr<std::is_same<T, Ts>...> {
};

template <typename T, typename = void>
struct HasTypedefIterator : std::false_type {
};
template <typename T>
struct HasTypedefIterator<T, std::void_t<typename T::iterator>> : std::true_type {
};

template <typename T, template <typename...> class Template>
struct IsInstantiationOf : std::false_type {
};

template <template <typename...> class Template, typename... Args>
struct IsInstantiationOf<Template<Args...>, Template> : std::true_type {
};

template <typename T>
using IsStdPointer = StaticTypeOr<std::is_pointer<T>, IsInstantiationOf<T, std::shared_ptr>,
                                  IsInstantiationOf<T, std::unique_ptr>, IsInstantiationOf<T, std::weak_ptr>>;

template <class T, class U, typename = void>
struct IsVirtualBaseImpl : std::true_type {
};

template <class T, class U>
struct IsVirtualBaseImpl<T, U, std::void_t<decltype((U*)(std::declval<T*>()))>> : std::false_type {
};

template <class T, class U>
using IsVirtualBase = StaticTypeAnd<IsVirtualBaseImpl<T, U>, std::is_base_of<T, U>>;

static_assert(StaticTypeAnd<std::true_type, std::true_type, std::true_type>::value);
static_assert(!StaticTypeAnd<std::true_type, std::false_type, std::true_type>::value);
static_assert(StaticTypeOr<std::true_type, std::false_type, std::true_type>::value);
static_assert(!StaticTypeOr<std::false_type>::value);
static_assert(IsOneOf<int, float, double, int>::value);
static_assert(IsInstantiationOf<std::vector<bool>, std::vector>::value);
static_assert(!IsInstantiationOf<bool, std::vector>::value);
static_assert(IsStdPointer<std::shared_ptr<bool>>::value);

}} // namespace indexlibv2::util
