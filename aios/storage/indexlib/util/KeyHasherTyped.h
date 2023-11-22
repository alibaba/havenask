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
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/HashAlgorithm.h"
#include "autil/Log.h"
#include "autil/NumbersUtil.h"
#include "autil/Span.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlib { namespace util {

#define DECLARE_HASHER_IDENTIFIER(id)                                                                                  \
    static std::string Identifier() { return std::string(#id); }                                                       \
    static std::string GetIdentifier() { return Identifier(); }

class TextHasher
{
public:
    static bool GetHashKey(const char* word, size_t size, dictkey_t& key);
    static bool GetHashKey(const autil::StringView& word, dictkey_t& key);

    static bool IsOriginalKeyValid(const char* word, size_t size, bool hash64) { return true; }

public:
    DECLARE_HASHER_IDENTIFIER(StringHash64)
private:
    TextHasher() {}
};
typedef TextHasher DefaultHasher;

class LayerTextHasher
{
public:
    LayerTextHasher(const KeyValueMap& params)
    {
        _separator = GetValueFromKeyValueMap(params, "separator", "#");
        _prefixBits = GetTypeValueFromKeyValueMap<uint8_t>(params, "prefix_bits", 16);
        _prefixBits = std::max((uint8_t)1, std::min((uint8_t)32, _prefixBits)); // [1, 32]
        AUTIL_LOG(INFO, "layer hash use separator [%s], prefix bits [%d]", _separator.c_str(), _prefixBits);
    }

public:
    bool GetHashKey(const std::string& term, dictkey_t& key) const
    {
        size_t pos = term.find(_separator);
        uint64_t suffix = autil::HashAlgorithm::hashString64(term.data(), term.size());
        if (pos == std::string::npos || pos == 0) {
            key = suffix;
            return true;
        }
        uint64_t prefix = autil::HashAlgorithm::hashString(term.data(), pos, 0);
        key = (prefix << (64 - _prefixBits)) | (suffix >> _prefixBits);
        return true;
    }

private:
    std::string _separator;
    uint8_t _prefixBits;

private:
    AUTIL_LOG_DECLARE();
};

class MurmurHasher
{
public:
    static const uint16_t NO_SALT = 0xffff;

public:
    static bool GetHashKey(const char* word, size_t size, dictkey_t& key, uint16_t salt = NO_SALT);
    static bool GetHashKey(const autil::StringView& key, dictkey_t& hashKey, uint16_t salt = NO_SALT);

    static bool IsOriginalKeyValid(const char* word, size_t size, bool hash64) { return true; }

private:
    static uint64_t InnerGetHashKey(const void* key, int len, uint64_t salt);

public:
    DECLARE_HASHER_IDENTIFIER(MurmurHash)
private:
    MurmurHasher() {}

private:
    static const uint64_t SEED = 545609244;
};

/////////////////////////////NumberHasher///////////////////////////////
class NumberHasher
{
public:
    static const uint16_t DEFAULT_SALT = 0;
};

template <typename KeyType, bool negative>
class NumberHasherTyped
{
public:
    static bool GetHashKey(const char* word, size_t size, dictkey_t& key, uint16_t salt = 0);
    static bool IsOriginalKeyValid(const char* word, size_t size, bool hash64);
    static bool GetHashKey(const autil::StringView& key, dictkey_t& hashKey, uint16_t salt = 0);

private:
    static bool InnerGetHashKey(const char* word, size_t size, dictkey_t& key, uint16_t salt);

public:
    DECLARE_HASHER_IDENTIFIER(Number)
private:
    NumberHasherTyped() {}
};

template <typename KeyType, bool negative>
bool NumberHasherTyped<KeyType, negative>::GetHashKey(const char* word, size_t size, dictkey_t& key, uint16_t salt)
{
    return InnerGetHashKey(word, size, key, salt);
}

template <typename KeyType, bool negative>
bool NumberHasherTyped<KeyType, negative>::IsOriginalKeyValid(const char* word, size_t size, bool hash64)
{
    if (!hash64) {
        return true;
    }
    dictkey_t key;
    return GetHashKey(word, size, key);
}

template <typename KeyType, bool negative>
bool NumberHasherTyped<KeyType, negative>::GetHashKey(const autil::StringView& key, dictkey_t& hashKey, uint16_t salt)
{
    return InnerGetHashKey(key.data(), key.size(), hashKey, salt);
}

typedef NumberHasherTyped<uint8_t, true> Int8NumberHasher;
typedef NumberHasherTyped<uint8_t, false> UInt8NumberHasher;
typedef NumberHasherTyped<uint16_t, true> Int16NumberHasher;
typedef NumberHasherTyped<uint16_t, false> UInt16NumberHasher;
typedef NumberHasherTyped<uint32_t, true> Int32NumberHasher;
typedef NumberHasherTyped<uint32_t, false> UInt32NumberHasher;
typedef NumberHasherTyped<uint64_t, true> Int64NumberHasher;
typedef NumberHasherTyped<uint64_t, false> UInt64NumberHasher;

#define GETHASHKEY(RET_BITS)                                                                                           \
    template <>                                                                                                        \
    inline bool NumberHasherTyped<uint##RET_BITS##_t, false>::InnerGetHashKey(const char* word, size_t size,           \
                                                                              dictkey_t& key, uint16_t salt)           \
    {                                                                                                                  \
        uint##RET_BITS##_t var;                                                                                        \
        bool ret = autil::NumbersUtil::safe_strtouint##RET_BITS##_base(word, size, var, 10);                           \
        key = ((dictkey_t)salt << 48) | (dictkey_t)var;                                                                \
        return ret;                                                                                                    \
    }                                                                                                                  \
                                                                                                                       \
    template <>                                                                                                        \
    inline bool NumberHasherTyped<uint##RET_BITS##_t, true>::InnerGetHashKey(const char* word, size_t size,            \
                                                                             dictkey_t& key, uint16_t salt)            \
    {                                                                                                                  \
        int##RET_BITS##_t var;                                                                                         \
        bool ret = autil::NumbersUtil::safe_strtoint##RET_BITS##_base(word, size, var, 10);                            \
        key = ((dictkey_t)salt << 48) | (dictkey_t)var;                                                                \
        return ret;                                                                                                    \
    }

GETHASHKEY(8)
GETHASHKEY(16)
GETHASHKEY(32)
GETHASHKEY(64)

#undef GETHASHKEY
#undef DECLARE_HASHER_IDENTIFIER

inline bool GetHashKey(HashFunctionType hasherType, const autil::StringView& key, dictkey_t& hashKey)
{
    switch (hasherType) {
    case hft_uint64:
        return UInt64NumberHasher::GetHashKey(key, hashKey);
    case hft_int64:
        return Int64NumberHasher::GetHashKey(key, hashKey);
    case hft_murmur:
        return MurmurHasher::GetHashKey(key, hashKey);
    default:
        return false;
    }
    return false;
}

}} // namespace indexlib::util
