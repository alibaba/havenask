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

#include <stdint.h>
#include <string.h>
#include <string>
#include <functional>

#include "autil/HashAlgorithm.h"
#include "autil/LongHashValue.h"
#include "autil/StringView.h"
#include "autil/MultiValueType.h"
#include "autil/cityhash/city.h"

namespace autil {

template <class T>
class KeyHash {
};

template<>
class KeyHash<char>
{
public:
    inline uint64_t operator()(char key) const
    {
        return (uint64_t)key;
    }
};

template<>
class KeyHash<const char*>
{
public:
    uint64_t operator()(const char* key) const
    {
        return HashAlgorithm::hashString64(key);
    }
};

template<>
class KeyHash<std::string>
{
public:
    uint64_t operator()(const std::string& key) const
    {
        return HashAlgorithm::hashString64(key.c_str());
    }
};

template<>
class KeyHash<uint64_t>
{
public:
    uint64_t operator()(uint64_t key) const
    {
        return key;
    }
};

template<>
class KeyHash<int64_t>
{
public:
    uint64_t operator()(int64_t key) const
    {
        return (uint64_t)key;
    }
};

template<>
class KeyHash<uint32_t>
{
public:
    uint64_t operator()(uint32_t key) const
    {
        return (uint64_t)key;
    }
};

template<>
class KeyHash<int32_t>
{
public:
    uint64_t operator()(int32_t key) const
    {
        return (uint64_t)key;
    }
};

template<>
class KeyHash<uint16_t>
{
public:
    uint64_t operator()(uint16_t key) const
    {
        return (uint64_t)key;
    }
};

template<>
class KeyHash<int16_t>
{
public:
    uint64_t operator()(int16_t key) const
    {
        return (uint64_t)key;
    }
};

template<>
class KeyHash<uint8_t>
{
public:
    uint64_t operator()(uint8_t key) const
    {
        return (uint64_t)key;
    }
};

template<>
class KeyHash<int8_t>
{
public:
    uint64_t operator()(int8_t key) const
    {
        return (uint64_t)key;
    }
};

template<>
class KeyHash<float>
{
public:
    uint64_t operator()(float key) const
    {
        union {float k; uint64_t i;} a;
        a.k = a.i = 0;
        a.k = key;
        return a.i;
    }
};

template<>
class KeyHash<uint128_t>
{
public:
    uint64_t operator()(uint128_t key) const
    {
        return key.value[0];
    }
};

template <typename Key>
class KeyEqual {
};

template<>
class KeyEqual<char>
{
public:
    inline bool operator()(char cKey1, char cKey2) const
    {
        return cKey1 == cKey2;
    }
};

template<>
class KeyEqual<const char*>
{
public:
    inline bool operator()(const char* szKey1, const char* szKey2) const
    {
        return strcmp(szKey1,szKey2) == 0;
    }
};

template<>
class KeyEqual<std::string>
{
public:
    inline bool operator()(const std::string& key1, const std::string& key2) const
    {
        return key1 == key2;
    }
};

template<>
class KeyEqual<uint8_t>
{
public:
    inline bool operator()(uint8_t nKey1, uint8_t nKey2) const
    {
        return nKey1 == nKey2;
    }
};

template<>
class KeyEqual<int8_t>
{
public:
    inline bool operator()(int8_t nKey1, int8_t nKey2) const
    {
        return nKey1 == nKey2;
    }
};

template<>
class KeyEqual<uint16_t>
{
public:
    inline bool operator()(uint16_t nKey1, uint16_t nKey2) const
    {
        return nKey1 == nKey2;
    }
};

template<>
class KeyEqual<int16_t>
{
public:
    inline bool operator()(int16_t nKey1, int16_t nKey2) const
    {
        return nKey1 == nKey2;
    }
};

template<>
class KeyEqual<uint32_t>
{
public:
    inline bool operator()(uint32_t nKey1, uint32_t nKey2) const
    {
        return nKey1 == nKey2;
    }
};

template<>
class KeyEqual<int32_t>
{
public:
    inline bool operator()(int32_t nKey1, int32_t nKey2) const
    {
        return nKey1 == nKey2;
    }
};

template<>
class KeyEqual<uint64_t>
{
public:
    inline bool operator()(uint64_t nKey1, uint64_t nKey2) const
    {
        return nKey1 == nKey2;
    }
};

template<>
class KeyEqual<int64_t>
{
public:
    inline bool operator()(int64_t nKey1, int64_t nKey2) const
    {
        return nKey1 == nKey2;
    }
};

template<>
class KeyEqual<float>
{
public:
    inline bool operator()(float nKey1, float nKey2) const
    {
        return nKey1 == nKey2;
    }
};

template<>
class KeyEqual<uint128_t>
{
public:
    inline bool operator()(uint128_t nKey1, uint128_t nKey2) const
    {
        return nKey1 == nKey2;
    }
};

template <typename Key>
class KeyCompare
{
};

template<>
class KeyCompare<const char*>
{
public:
    inline bool operator()(const char* szKey1, const char* szKey2) const
    {
        return strcmp(szKey1,szKey2) < 0;
    }
};

template<>
class KeyCompare<uint32_t>
{
public:
    inline bool operator()(uint32_t nKey1, uint32_t nKey2) const
    {
        return nKey1 < nKey2;
    }
};

template<>
class KeyCompare<int32_t>
{
public:
    inline bool operator()(int32_t nKey1, int32_t nKey2) const
    {
        return nKey1 < nKey2;
    }
};

template<>
class KeyCompare<float>
{
public:
    inline bool operator()(float nKey1, float nKey2) const
    {
        return nKey1 < nKey2;
    }
};

template<>
class KeyCompare<uint64_t>
{
public:
    inline bool operator()(uint64_t nKey1, uint64_t nKey2) const
    {
        return nKey1 < nKey2;
    }
};

template<>
class KeyCompare<int64_t>
{
public:
    inline bool operator()(int64_t nKey1, int64_t nKey2) const
    {
        return nKey1 < nKey2;
    }
};

template<>
class KeyCompare<uint128_t>
{
public:
    inline bool operator()(uint128_t nKey1, uint128_t nKey2) const
    {
        if((nKey1.value[0] < nKey2.value[0]))
            return true;
         if((nKey1.value[0] > nKey2.value[0]))
             return false;
         return (nKey1.value[1] < nKey2.value[1]);
    }
};

template < typename Type, typename Compare = KeyCompare<Type> >
class BinarySearch
{
public:
    inline uint32_t operator()(Type *szElements, uint32_t nBegin,
                               uint32_t nEnd, Type value)
    {
        return std::lower_bound(szElements + nBegin,
                                szElements + nEnd, value, Compare()) - szElements;
    }
};


class HashUtil {
public:
    template <typename T>
    static size_t calculateHashValue(const T &data);

    template <class T>
    static void combineHash(size_t& seed, const T& v) {
        size_t seed2 = calculateHashValue(v);
        seed = Hash128to64(uint128(seed, seed2));
    }
};

template <>
inline void HashUtil::combineHash(size_t& seed, const size_t& v) {
    seed = Hash128to64(uint128(seed, v));
}

template<typename T>
size_t HashUtil::calculateHashValue(const T &data) {
    std::hash<T> hasher;
    return hasher(data);
}

template<>
inline size_t HashUtil::calculateHashValue(const autil::StringView &data) {
    return CityHash64(data.data(), data.size());
}

template<>
inline size_t HashUtil::calculateHashValue(const autil::MultiChar &data) {
    return CityHash64(data.data(), data.size());
}

template<>
inline size_t HashUtil::calculateHashValue(const std::string &data) {
    return CityHash64(data.data(), data.size());
}

#define CALCULATE_HASH_VALUE(Type)                                      \
    template<>                                                          \
    inline size_t HashUtil::calculateHashValue<Type>(const Type &data) { \
        return CityHash64((char*)&data, sizeof(Type));                  \
    }

#define CALCULATE_HASH_VALUE_SMALL_INT(Type)                            \
    template<>                                                          \
    inline size_t HashUtil::calculateHashValue<Type>(const Type &data) { \
        int64_t tmp = data;                                             \
        return CityHash64((char*)&tmp, sizeof(int64_t));               \
    }

#define CALCULATE_HASH_VALUE_SMALL_UINT(Type)                           \
    template<>                                                          \
    inline size_t HashUtil::calculateHashValue<Type>(const Type &data) { \
        uint64_t tmp = data;                                            \
        return CityHash64((char*)&tmp, sizeof(uint64_t));               \
    }

CALCULATE_HASH_VALUE_SMALL_INT(int8_t)
CALCULATE_HASH_VALUE_SMALL_INT(int16_t)
CALCULATE_HASH_VALUE_SMALL_INT(int32_t)
CALCULATE_HASH_VALUE_SMALL_UINT(uint8_t)
CALCULATE_HASH_VALUE_SMALL_UINT(uint16_t)
CALCULATE_HASH_VALUE_SMALL_UINT(uint32_t)
CALCULATE_HASH_VALUE(int64_t)
CALCULATE_HASH_VALUE(uint64_t)
CALCULATE_HASH_VALUE(float)
CALCULATE_HASH_VALUE(double)

#undef CALCULATE_HASH_VALUE
#undef CALCULATE_HASH_VALUE_SMALL_INT
#undef CALCULATE_HASH_VALUE_SMALL_UINT

#define CALCULATE_MULTI_VALUE_HASH_VALUE(Type)                          \
    template<>                                                          \
    inline size_t HashUtil::calculateHashValue<Type>(const Type &data) { \
        assert(data.isEmptyData() || data.hasEncodedCount());           \
        return CityHash64(data.getBaseAddress(), data.length());        \
    }
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiUInt8);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiInt8);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiUInt16);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiInt16);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiUInt32);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiInt32);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiUInt64);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiInt64);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiFloat);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiDouble);
CALCULATE_MULTI_VALUE_HASH_VALUE(autil::MultiString);

#undef CALCULATE_MULTI_VALUE_HASH_VALUE

}
