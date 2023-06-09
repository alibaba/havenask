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
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>
#include <algorithm>
#include <cctype>

#include "autil/Span.h"
#include "autil/CommonMacros.h"

namespace autil {

class StringTokenizer
{
public:
    /// ignore empty tokens
    const static int32_t TOKEN_IGNORE_EMPTY = 1;
    /// remove leading and trailing whitespace from tokens
    const static int32_t TOKEN_TRIM         = 2;
    /// leave the token as it is
    const static int32_t TOKEN_LEAVE_AS     = 4;

    typedef int32_t Option;
    typedef std::vector<std::string> TokenVector;
    typedef TokenVector::const_iterator Iterator;
public:
    StringTokenizer(const std::string& str, const std::string& sep,
                    Option opt = TOKEN_LEAVE_AS);
    StringTokenizer();
    ~StringTokenizer();
public:
    size_t tokenize(const std::string& str, const std::string& sep,
                    Option opt = TOKEN_LEAVE_AS);

    inline Iterator begin() const;
    inline Iterator end() const;

    inline const std::string& operator [] (size_t index) const;

    inline size_t getNumTokens() const;
    inline const TokenVector& getTokenVector() const;

public:
    // new implement begin
    static std::vector<StringView> constTokenize(
            const StringView& str, const std::string& sep,
            Option opt = TOKEN_IGNORE_EMPTY | TOKEN_TRIM);

    static std::vector<StringView> constTokenize(
            const StringView& str, char c,
            Option opt = TOKEN_IGNORE_EMPTY | TOKEN_TRIM);

    static std::vector<std::string> tokenize(
            const StringView& str, const std::string &sep,
            Option opt = TOKEN_IGNORE_EMPTY | TOKEN_TRIM);

    static std::vector<std::string> tokenize(
            const StringView& str, char c,
            Option opt = TOKEN_IGNORE_EMPTY | TOKEN_TRIM);

    template <typename RawStringType>    
    static std::vector<std::string_view>
    tokenizeView(const RawStringType &str, const std::string &sep, Option opt = TOKEN_IGNORE_EMPTY | TOKEN_TRIM);

    template <typename RawStringType>        
    static std::vector<std::string_view>
    tokenizeView(const RawStringType &str, char c, Option opt = TOKEN_IGNORE_EMPTY | TOKEN_TRIM);

    template <typename ResultContainerType>
    static void tokenize(const StringView &str,
                         char sep,
                         ResultContainerType &results,
                         Option opt = TOKEN_IGNORE_EMPTY | TOKEN_TRIM) {
        return tokenize(str, sep, 1, results, opt);
    }
    template <typename ResultContainerType>
    static void tokenize(const StringView &str,
                         const std::string &sep,
                         ResultContainerType &results,
                         Option opt = TOKEN_IGNORE_EMPTY | TOKEN_TRIM) {
        return tokenize(str, sep, sep.length(), results, opt);
    }

private:
    template<typename Sep, typename ResultContainerType, typename RawStringType>
    inline static void tokenize(const RawStringType& str, Sep &&sep, size_t sepLen,
                                ResultContainerType &results, Option opt) __attribute__((always_inline));
    inline static bool isSpace(char ch);

    template <typename ResultContainerType, typename ResultItemType>
    inline static void addToken(const ResultItemType& token,
                                Option opt, ResultContainerType &target) __attribute__((always_inline));
private:
    StringTokenizer(const StringTokenizer&);
    StringTokenizer& operator = (const StringTokenizer&);
private:
    TokenVector m_tokens;
};

///////////////////////////////////////////////////////////
// inlines

template <typename ResultContainerType, typename ResultItemType>
void StringTokenizer::addToken(const ResultItemType &token, Option opt, ResultContainerType &target) {
    size_t length = token.size();
    const char *data = token.data();
    if (unlikely(opt & TOKEN_LEAVE_AS)) {
        if (!(opt & TOKEN_IGNORE_EMPTY)) {
            target.push_back(ResultItemType(data, length));
        } else if (length > 0) {
            target.push_back(ResultItemType(data, length));
        }
    } else if (unlikely(opt & TOKEN_TRIM)) {
        size_t n = 0;
        while (n < length && isSpace(data[n])) {
            n++;
        }
        size_t n2 = length;
        while (n2 > n && isSpace(data[n2 - 1])) {
            n2--;
        }
        if (n2 > n) {
            target.push_back(ResultItemType(data + n, n2 - n));
        } else if (!(opt & TOKEN_IGNORE_EMPTY)) {
            target.push_back(ResultItemType()); // insert an empty token
        }
    } else if (length > 0) {
        target.push_back(ResultItemType(data, length));
    }
}

template <typename Sep, typename ResultContainerType, typename RawStringType>
inline void StringTokenizer::tokenize(
    const RawStringType &str, Sep &&sep, size_t sepLen, ResultContainerType &results, Option opt) {
    typedef typename ResultContainerType::value_type T;
    size_t n = 0, old = 0;
    while (n != std::string::npos) {
        n = str.find(sep, n);
        if (n != std::string::npos) {
            if (n != old) {
                T subStr(str.data() + old, n - old);
                addToken(subStr, opt, results);
            } else {
                T subStr("");
                addToken(subStr, opt, results);
            }

            n += sepLen;
            old = n;
        }
    }
    T subStr(str.data() + old, str.size() - old);
    addToken(subStr, opt, results);
}

inline bool StringTokenizer::isSpace(char ch)
{
    return (ch > 0 && std::isspace(ch));
}

inline StringTokenizer::Iterator StringTokenizer::begin() const
{
    return m_tokens.begin();
}

inline StringTokenizer::Iterator StringTokenizer::end() const
{
    return m_tokens.end();
}

inline const std::string& StringTokenizer::operator [] (std::size_t index) const
{
    assert(index < m_tokens.size());
    return m_tokens[index];
}

inline size_t StringTokenizer::getNumTokens() const
{
    return m_tokens.size();
}

inline const StringTokenizer::TokenVector& StringTokenizer::getTokenVector() const {
    return m_tokens;
}

template <typename RawStringType>
std::vector<std::string_view>
StringTokenizer::tokenizeView(const RawStringType &str, const std::string &sep, Option opt) {
    std::vector<std::string_view> ret;
    tokenize(str, sep, sep.length(), ret, opt);
    return ret;
}
template <typename RawStringType>
std::vector<std::string_view> StringTokenizer::tokenizeView(const RawStringType &str, char c, Option opt) {
    std::vector<std::string_view> ret;
    tokenize(str, c, 1, ret, opt);
    return ret;
}
}
