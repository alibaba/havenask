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

#include <algorithm>
#include <array>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#include <type_traits>

#include <stdint.h>

namespace autil {

template <typename T, typename Enable = void>
struct SpanAdapter;
template <typename T, typename Enable = void>
struct ExplicitSpanAdapter;

namespace detail {

/* polyfill for c++20 std::span<T> */
/* use crtp for one */
template <typename T, typename Self>
class BasicSpan {
public:
    static_assert(!std::is_reference_v<T>);

    using element_type = T;
    using value_type = std::remove_cv_t<T>;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using pointer = T *;
    using const_pointer = const T *;
    using reference = T &;
    using const_reference = const T &;
    using iterator = pointer;
    using const_iterator = const_pointer;

public:
    /* default copy */
    constexpr BasicSpan(BasicSpan const &) noexcept = default;
    constexpr BasicSpan(BasicSpan &&) noexcept = default;

    BasicSpan &operator=(BasicSpan const &) noexcept = default;
    BasicSpan &operator=(BasicSpan &&) noexcept = default;

    /* default construct */
    constexpr BasicSpan() noexcept : _data { nullptr }, _size { 0 } { }

    template <typename It>
    constexpr BasicSpan(It first, size_type count) :
        _data { &*first }, _size { count } {
        /* TODO: satisfy concept `std::contiguous_iterator<T>` */
        /* https://en.cppreference.com/w/cpp/container/span/span :: (2) */
    }

    template <typename It>
    constexpr BasicSpan(It first, It last) :
        _data { &*first }, _size { size_type(last - first) } {
        /* https://en.cppreference.com/w/cpp/container/span/span :: (3) */
    }

    template <typename U,
              bool A = true,
              std::enable_if_t<(A && std::is_const_v<T>), int> = 0>
    constexpr BasicSpan(const BasicSpan<std::remove_const_t<T>, U> &other) noexcept :
        _data { other.data() }, _size { other.size() } { }

public:
    /* iterators */
    constexpr iterator begin() const noexcept { return _data; }

    constexpr iterator end() const noexcept { return _data + _size; }

public:
    /* element access */
    constexpr T &front() const { return _data[0]; }

    constexpr T &back() const { return _data[_size - 1]; }

    constexpr T &operator[](size_type idx) const { return _data[idx]; }

    constexpr T &at(size_type idx) const {
        if (idx >= _size) {
            throw std::out_of_range("index out of range");
        }
        return _data[idx];
    }

    constexpr T *data() const noexcept { return _data; }

public:
    /* observer */
    constexpr size_type size() const noexcept { return _size; }

    constexpr size_type size_bytes() const noexcept { return _size * sizeof(T); }

    [[nodiscard]] constexpr bool empty() const noexcept { return _size == 0; }

public:
    /* subviews */
    constexpr Self first(size_type count) const { return { _data, count }; }

    constexpr Self last(size_type count) const {
        return { _data + (_size - count), count };
    }

    constexpr Self subspan(size_type offset) const {
        return subspan(offset, _size - offset);
    }

    constexpr Self subspan(size_type offset, size_type count) const {
        return { _data + offset, count };
    }

public:
    /* adapter */
    template <
        typename V,
        typename U = std::decay_t<V>,
        typename = std::enable_if_t<!std::is_same_v<U, Self>>,
        typename = std::void_t<decltype(SpanAdapter<U>::from(std::declval<V &&>()))>>
    BasicSpan(V &&other) noexcept(
        noexcept(SpanAdapter<U>::from(std::declval<V &&>()))) :
        BasicSpan { SpanAdapter<U>::from(std::forward<V>(other)) } { }

    template <
        typename U,
        typename = std::void_t<decltype(SpanAdapter<U>::to(std::declval<Self>()))>>
    operator U() const noexcept(noexcept(SpanAdapter<U>::to(std::declval<Self>()))) {
        return SpanAdapter<U>::to(static_cast<const Self &>(*this));
    }

    template <typename V,
              bool A = true,
              typename U = std::decay_t<V>,
              typename = std::enable_if_t<A && !std::is_same_v<U, Self>>,
              typename = std::void_t<
                  decltype(ExplicitSpanAdapter<U>::from(std::declval<V &&>()))>>
    explicit BasicSpan(V &&other) noexcept(
        noexcept(ExplicitSpanAdapter<U>::from(std::declval<V &&>()))) :
        BasicSpan { ExplicitSpanAdapter<U>::from(std::forward<V>(other)) } { }

    template <typename U,
              bool A = true,
              typename = std::enable_if_t<A>,
              typename = std::void_t<
                  decltype(ExplicitSpanAdapter<U>::to(std::declval<Self>()))>>
    explicit operator U() const
        noexcept(noexcept(ExplicitSpanAdapter<U>::to(std::declval<Self>()))) {
        return ExplicitSpanAdapter<U>::to(static_cast<const Self &>(*this));
    }

public:
    /* instance */
    static inline const Self &empty_instance() {
        static Self self;
        return self;
    }

protected:
    T *_data;
    size_type _size;
};

template <typename CharT, typename Self>
class StringViewImpl : public BasicSpan<CharT, Self> {
private:
    using U8 = unsigned char;
    static_assert(std::is_same_v<std::decay_t<CharT>, char>);
    static_assert(sizeof(CharT) == sizeof(U8));
    using StdU8CharTraitsType = std::char_traits<U8>;
    using StdU8StringViewType = std::basic_string_view<U8>;

public:
    using size_type = typename BasicSpan<CharT, Self>::size_type;
    using BasicSpan<CharT, Self>::BasicSpan;

    explicit constexpr StringViewImpl(CharT *s) :
        BasicSpan<CharT, Self> { s, std::char_traits<char>::length(s) } {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/basic_string_view :: (4) */
    }

    explicit constexpr StringViewImpl(std::nullptr_t) = delete;
    /* https://en.cppreference.com/w/cpp/string/basic_string_view/basic_string_view :: (7) */

public:
    /* capacity */
    constexpr size_type length() const noexcept { return this->_size; }

public:
    /* constants */
    static constexpr size_type npos = std::string_view::npos;

public:
    /* operations */
    size_type copy(char *dest, size_type count, size_type pos = 0) const {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/copy */
        return u8span().copy((U8 *)dest, count, pos);
    }

    constexpr Self substr(size_type pos = 0, size_type count = npos) const {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/substr */
        auto v = u8span().substr(pos, count);
        return { (CharT *)v.data(), v.size() };
    }

    friend std::ostream &operator<<(std::ostream &os, const StringViewImpl &v) {
        return os << std::string_view { v.data(), v.size() };
    }

    /* find */
    constexpr size_type find(const StringViewImpl &v,
                             size_type pos = 0) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find :: (1) */
        return u8span().find(v.u8span(), pos);
    }

    constexpr size_type find(char ch, size_type pos = 0) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find :: (2) */
        return u8span().find(ch, pos);
    }

    constexpr size_type find(const char *s,
                             size_type pos,
                             size_type count) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find :: (3) */
        return u8span().find((const U8 *)s, pos, count);
    }

    constexpr size_type find(const char *s, size_type pos = 0) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find :: (4) */
        return u8span().find((const U8 *)s, pos);
    }

    /* rfind */
    constexpr size_type rfind(const StringViewImpl &v,
                              size_type pos = npos) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/rfind :: (1) */
        return u8span().rfind(v.u8span(), pos);
    }

    constexpr size_type rfind(char c, size_type pos = npos) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/rfind :: (2) */
        return u8span().rfind(c, pos);
    }

    constexpr size_type rfind(const char *s,
                              size_type pos,
                              size_type count) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/rfind :: (3) */
        return u8span().rfind((const U8 *)s, pos, count);
    }

    constexpr size_type rfind(const char *s, size_type pos = npos) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/rfind :: (4) */
        return u8span().rfind((const U8 *)s, pos);
    }

    /* find_first_of */
    constexpr size_type find_first_of(const StringViewImpl &v,
                                      size_type pos = 0) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find_first_of :: (1) */
        return u8span().find_first_of(v.u8span(), pos);
    }

    constexpr size_type find_first_of(char ch, size_type pos = 0) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find_first_of :: (2) */
        return u8span().find_first_of(ch, pos);
    }

    constexpr size_type find_first_of(const char *s,
                                      size_type pos,
                                      size_type count) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find_first_of :: (3) */
        return u8span().find_first_of((const U8 *)s, pos, count);
    }

    constexpr size_type find_first_of(const char *s,
                                      size_type pos = 0) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find_first_of :: (4) */
        return u8span().find_first_of((const U8 *)s, pos);
    }

    /* find_last_of */
    constexpr size_type find_last_of(const StringViewImpl &v,
                                     size_type pos = npos) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find_last_of :: (1) */
        return u8span().find_last_of(v.u8span(), pos);
    }

    constexpr size_type find_last_of(char c, size_type pos = npos) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find_last_of :: (2) */
        return u8span().find_last_of(c, pos);
    }

    constexpr size_type find_last_of(const char *s,
                                     size_type pos,
                                     size_type count) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find_last_of :: (3) */
        return u8span().find_last_of((const U8 *)s, pos, count);
    }

    constexpr size_type find_last_of(const char *s,
                                     size_type pos = npos) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find_last_of :: (4) */
        return u8span().find_last_of((const U8 *)s, pos);
    }

    /* find_first_not_of */
    constexpr size_type find_first_not_of(const StringViewImpl &v,
                                          size_type pos = 0) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find_first_not_of :: (1) */
        return u8span().find_first_not_of(v.u8span(), pos);
    }

    constexpr size_type find_first_not_of(char ch, size_type pos = 0) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find_first_not_of :: (2) */
        return u8span().find_first_not_of(ch, pos);
    }

    constexpr size_type find_first_not_of(const char *s,
                                          size_type pos,
                                          size_type count) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find_first_not_of :: (3) */
        return u8span().find_first_not_of((const U8 *)s, pos, count);
    }

    constexpr size_type find_first_not_of(const char *s,
                                          size_type pos = 0) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find_first_not_of :: (4) */
        return u8span().find_first_not_of((const U8 *)s, pos);
    }

    /* find_last_not_of */
    constexpr size_type find_last_not_of(const StringViewImpl &v,
                                         size_type pos = npos) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find_last_not_of :: (1) */
        return u8span().find_last_not_of(v.u8span(), pos);
    }

    constexpr size_type find_last_not_of(char c,
                                         size_type pos = npos) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find_last_not_of :: (2) */
        return u8span().find_last_not_of(c, pos);
    }

    constexpr size_type find_last_not_of(const char *s,
                                         size_type pos,
                                         size_type count) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find_last_not_of :: (3) */
        return u8span().find_last_not_of((const U8 *)s, pos, count);
    }

    constexpr size_type find_last_not_of(const char *s,
                                         size_type pos = npos) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/find_last_not_of :: (4) */
        return u8span().find_last_not_of((const U8 *)s, pos);
    }

public:
    /* operations (c++20) */
    constexpr bool starts_with(const StringViewImpl &sv) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/starts_with :: (1) */
        /* https://github.com/gcc-mirror/gcc/blob/27239e13b1ba383e2706231917062aa6e14150a8/libstdc%2B%2B-v3/include/std/string_view#L356 */
        return this->substr(0, sv.size()) == sv;
    }

    constexpr bool starts_with(char c) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/starts_with :: (2) */
        /* https://github.com/gcc-mirror/gcc/blob/27239e13b1ba383e2706231917062aa6e14150a8/libstdc%2B%2B-v3/include/std/string_view#L360 */
        return !this->empty() && StdU8CharTraitsType::eq((U8)this->front(), (U8)c);
    }

    constexpr bool starts_with(const char *s) const {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/starts_with :: (3) */
        /* https://github.com/gcc-mirror/gcc/blob/27239e13b1ba383e2706231917062aa6e14150a8/libstdc%2B%2B-v3/include/std/string_view#L364 */
        return starts_with(Self { s });
    }

    constexpr bool ends_with(const StringViewImpl &sv) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/ends_with :: (1) */
        /* https://github.com/gcc-mirror/gcc/blob/27239e13b1ba383e2706231917062aa6e14150a8/libstdc%2B%2B-v3/include/std/string_view#L368 */
        return this->size() >= sv.size() &&
               this->substr(this->size() - sv.size()) == sv;
    }

    constexpr bool ends_with(char c) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/ends_with :: (2) */
        /* https://github.com/gcc-mirror/gcc/blob/27239e13b1ba383e2706231917062aa6e14150a8/libstdc%2B%2B-v3/include/std/string_view#L377 */
        return !this->empty() && StdU8CharTraitsType::eq((U8)this->back(), (U8)c);
    }

    constexpr bool ends_with(const char *s) const {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/ends_with :: (3) */
        /* https://github.com/gcc-mirror/gcc/blob/27239e13b1ba383e2706231917062aa6e14150a8/libstdc%2B%2B-v3/include/std/string_view#L381 */
        return ends_with(Self { s });
    }

public:
    /* operations (c++23) */
    constexpr bool contains(const StringViewImpl &sv) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/contains :: (1) */
        /* https://github.com/gcc-mirror/gcc/blob/27239e13b1ba383e2706231917062aa6e14150a8/libstdc%2B%2B-v3/include/std/string_view#L388 */
        return this->find(sv) != npos;
    }

    constexpr bool contains(char c) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/contains :: (2) */
        /* https://github.com/gcc-mirror/gcc/blob/27239e13b1ba383e2706231917062aa6e14150a8/libstdc%2B%2B-v3/include/std/string_view#L392 */
        return this->find(c) != npos;
    }

    constexpr bool contains(const char *s) const {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/contains :: (3) */
        /* https://github.com/gcc-mirror/gcc/blob/27239e13b1ba383e2706231917062aa6e14150a8/libstdc%2B%2B-v3/include/std/string_view#L396 */
        return this->find(s) != npos;
    }

public:
    /* compare */
    inline bool operator==(const StringViewImpl &rhs) const noexcept {
        /* https://github.com/gcc-mirror/gcc/blob/27239e13b1ba383e2706231917062aa6e14150a8/libstdc%2B%2B-v3/include/std/string_view#L540 */
        return this->size() == rhs.size() && compare(rhs) == 0;
    }

    friend inline bool operator==(const StringViewImpl &self,
                                  const char *other) noexcept {
        return self == Self { other };
    }

    friend inline bool operator==(const char *self,
                                  const StringViewImpl &other) noexcept {
        return Self { self } == other;
    }

    inline bool operator!=(const StringViewImpl &rhs) const noexcept {
        /* https://github.com/gcc-mirror/gcc/blob/27239e13b1ba383e2706231917062aa6e14150a8/libstdc%2B%2B-v3/include/std/string_view#L575 */
        return !(*this == rhs);
    }

    inline bool operator<(const StringViewImpl &rhs) const noexcept {
        /* https://github.com/gcc-mirror/gcc/blob/27239e13b1ba383e2706231917062aa6e14150a8/libstdc%2B%2B-v3/include/std/string_view#L594 */
        return compare(rhs) < 0;
    }

    inline bool operator>(const StringViewImpl &rhs) const noexcept {
        /* https://github.com/gcc-mirror/gcc/blob/27239e13b1ba383e2706231917062aa6e14150a8/libstdc%2B%2B-v3/include/std/string_view#L613 */
        return compare(rhs) > 0;
    }

    inline bool operator<=(const StringViewImpl &rhs) const noexcept {
        /* https://github.com/gcc-mirror/gcc/blob/27239e13b1ba383e2706231917062aa6e14150a8/libstdc%2B%2B-v3/include/std/string_view#L632 */
        return compare(rhs) <= 0;
    }

    inline bool operator>=(const StringViewImpl &rhs) const noexcept {
        /* https://github.com/gcc-mirror/gcc/blob/27239e13b1ba383e2706231917062aa6e14150a8/libstdc%2B%2B-v3/include/std/string_view#L651 */
        return compare(rhs) >= 0;
    }

    inline int compare(const StringViewImpl &other) const noexcept {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/compare :: (1) */
        return u8span().compare(other.u8span());
    }

    inline int compare(const char *other) const {
        /* https://en.cppreference.com/w/cpp/string/basic_string_view/compare :: (4) */
        return u8span().compare(reinterpret_cast<const U8 *>(other));
    }

public:
    /* extensions */
    std::string to_string() const { return { this->data(), this->size() }; }

public:
    /* legacy StringView operations */
    [[deprecated(
        "`StringView` is never guaranteed to be zero-terminated, "
        "naming this method to `c_str()` may lead to misuse or potential error. "
        "please confirm your use case is correct, "
        "or you may use `data()` to access the underlying buffer.")]]
    CharT *c_str() const { return this->data(); }

    template <typename... Args>
    [[deprecated("please use `substr()` instead, "
                 "note that boundary check has been removed")]] Self
        subString(Args &&...args) const {
        return this->substr(std::forward<Args>(args)...);
    }

    [[deprecated(
        "please use `to_string()` or explicit type-cast instead")]] std::string
        toString() const {
        return to_string();
    }

    [[deprecated("please use lvalue assignment instead")]] void reset(
        CharT *data,
        size_type size) {
        this->_data = data;
        this->_size = size;
    }

protected:
    /* uint8 char traits adapter */
    /* FIXME: these functions ought to be constexpr, but legacy usage treated the 
       string_view as unsigned chars, but unfortunately, reinterpret_cast<> is 
       not allowed during compile time evaluation, so the constexpr usage is 
       ill formed, currently */
    [[gnu::always_inline]] constexpr const U8 *u8data() const noexcept {
        return reinterpret_cast<const U8 *>(this->data());
    }

    [[gnu::always_inline]] constexpr StdU8StringViewType u8span() const noexcept {
        return { u8data(), this->size() };
    }
};

template <typename T, typename Self>
class SpanOrStringView : public BasicSpan<T, Self> {
public:
    using BasicSpan<T, Self>::BasicSpan;

    template <std::size_t N>
    constexpr SpanOrStringView(T (&arr)[N]) noexcept : BasicSpan<T, Self> { arr, N } {
        /* https://en.cppreference.com/w/cpp/container/span/span :: (4) */
    }

public:
    /* compare */
    inline bool operator==(const SpanOrStringView &rhs) const {
        return std::equal(this->begin(), this->end(), rhs.begin(), rhs.end());
    }

    inline bool operator!=(const SpanOrStringView &rhs) const {
        return !(*this == rhs);
    }

    inline bool operator<(const SpanOrStringView &rhs) const {
        return std::lexicographical_compare(
            this->begin(), this->end(), rhs.begin(), rhs.end());
    }

    inline bool operator>(const SpanOrStringView &rhs) const {
        return std::lexicographical_compare(
            rhs.begin(), rhs.end(), this->begin(), this->end());
    }

    inline bool operator<=(const SpanOrStringView &rhs) const {
        return !(*this > rhs);
    }

    inline bool operator>=(const SpanOrStringView &rhs) const {
        return !(*this < rhs);
    }
};

template <typename Self>
class SpanOrStringView<char, Self> : public StringViewImpl<char, Self> {
public:
    using StringViewImpl<char, Self>::StringViewImpl;
};

template <typename Self>
class SpanOrStringView<const char, Self> : public StringViewImpl<const char, Self> {
public:
    using StringViewImpl<const char, Self>::StringViewImpl;
};

}    // namespace detail

template <typename T>
class Span : public detail::SpanOrStringView<T, Span<T>> {
public:
    using detail::SpanOrStringView<T, Span>::SpanOrStringView;
};

template <typename T>
using ConstSpan = Span<const T>;

using StringView = ConstSpan<char>;
using MutableStringView = Span<char>;

/* adapters */
template <typename T>
struct SpanAdapter<std::vector<T>> {
    [[gnu::always_inline]] inline static Span<T> from(
        std::vector<T> &other) noexcept {
        return { other.data(), other.size() };
    }

    [[gnu::always_inline]] inline static ConstSpan<T> from(
        const std::vector<T> &other) noexcept {
        return { other.data(), other.size() };
    }
};

template <typename T>
struct ExplicitSpanAdapter<std::vector<T>> {
    [[gnu::always_inline]] inline static std::vector<T> to(
        const Span<T> &other) noexcept {
        return { other.begin(), other.end() };
    }
};

template <typename T, std::size_t N>
struct SpanAdapter<std::array<T, N>> {
    [[gnu::always_inline]] inline static Span<T> from(
        std::array<T, N> &other) noexcept {
        return { other.data(), other.size() };
    }

    [[gnu::always_inline]] inline static ConstSpan<T> from(
        const std::array<T, N> &other) noexcept {
        return { other.data(), other.size() };
    }
};

template <>
struct SpanAdapter<std::string> {
    [[gnu::always_inline]] inline static StringView from(
        const std::string &other) noexcept {
        return { other.data(), other.size() };
    }
};

template <>
struct ExplicitSpanAdapter<std::string> {
    [[gnu::always_inline]] inline static std::string to(
        const StringView &other) noexcept {
        return { other.data(), other.size() };
    }
};

template <>
struct SpanAdapter<std::string_view> {
    [[gnu::always_inline]] inline static StringView from(
        const std::string_view &other) noexcept {
        return { other.data(), other.size() };
    }

    [[gnu::always_inline]] inline static std::string_view to(
        const StringView &other) noexcept {
        return { other.data(), other.size() };
    }
};

} // autil
