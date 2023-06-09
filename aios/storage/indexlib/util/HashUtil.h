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

#include <memory>

#include "autil/HashAlgorithm.h"
#include "autil/LongHashValue.h"

namespace indexlib { namespace util {

template <class T>
class KeyHash
{
};

template <>
class KeyHash<char>
{
public:
    inline uint64_t operator()(char key) const { return (uint64_t)key; }
};

template <>
class KeyHash<const char*>
{
public:
    uint64_t operator()(const char* key) const { return autil::HashAlgorithm::hashString64(key); }
};

template <>
class KeyHash<std::string>
{
public:
    uint64_t operator()(const std::string& key) const { return autil::HashAlgorithm::hashString64(key.c_str()); }
};

template <>
class KeyHash<uint64_t>
{
public:
    uint64_t operator()(uint64_t key) const { return key; }
};

template <>
class KeyHash<int64_t>
{
public:
    uint64_t operator()(int64_t key) const { return (uint64_t)key; }
};

template <>
class KeyHash<uint32_t>
{
public:
    uint64_t operator()(uint32_t key) const { return (uint64_t)key; }
};

template <>
class KeyHash<int32_t>
{
public:
    uint64_t operator()(int32_t key) const { return (uint64_t)key; }
};

template <>
class KeyHash<uint16_t>
{
public:
    uint64_t operator()(uint16_t key) const { return (uint64_t)key; }
};

template <>
class KeyHash<int16_t>
{
public:
    uint64_t operator()(int16_t key) const { return (uint64_t)key; }
};

template <>
class KeyHash<uint8_t>
{
public:
    uint64_t operator()(uint8_t key) const { return (uint64_t)key; }
};

template <>
class KeyHash<int8_t>
{
public:
    uint64_t operator()(int8_t key) const { return (uint64_t)key; }
};

template <>
class KeyHash<float>
{
public:
    uint64_t operator()(float key) const
    {
        union {
            float k;
            uint64_t i;
        } a;
        a.k = a.i = 0;
        a.k = key;
        return a.i;
    }
};

template <>
class KeyHash<double>
{
public:
    uint64_t operator()(double key) const
    {
        union {
            double k;
            uint64_t i;
        } a;
        a.k = a.i = 0;
        a.k = key;
        return a.i;
    }
};

template <>
class KeyHash<autil::uint128_t>
{
public:
    uint64_t operator()(autil::uint128_t key) const { return key.value[0]; }
};

template <typename Key>
class KeyEqual
{
};

template <>
class KeyEqual<char>
{
public:
    inline bool operator()(char cKey1, char cKey2) const { return cKey1 == cKey2; }
};

template <>
class KeyEqual<const char*>
{
public:
    inline bool operator()(const char* szKey1, const char* szKey2) const { return strcmp(szKey1, szKey2) == 0; }
};

template <>
class KeyEqual<std::string>
{
public:
    inline bool operator()(const std::string& key1, const std::string& key2) const { return key1 == key2; }
};

template <>
class KeyEqual<uint8_t>
{
public:
    inline bool operator()(uint8_t nKey1, uint8_t nKey2) const { return nKey1 == nKey2; }
};

template <>
class KeyEqual<int8_t>
{
public:
    inline bool operator()(int8_t nKey1, int8_t nKey2) const { return nKey1 == nKey2; }
};

template <>
class KeyEqual<uint16_t>
{
public:
    inline bool operator()(uint16_t nKey1, uint16_t nKey2) const { return nKey1 == nKey2; }
};

template <>
class KeyEqual<int16_t>
{
public:
    inline bool operator()(int16_t nKey1, int16_t nKey2) const { return nKey1 == nKey2; }
};

template <>
class KeyEqual<uint32_t>
{
public:
    inline bool operator()(uint32_t nKey1, uint32_t nKey2) const { return nKey1 == nKey2; }
};

template <>
class KeyEqual<int32_t>
{
public:
    inline bool operator()(int32_t nKey1, int32_t nKey2) const { return nKey1 == nKey2; }
};

template <>
class KeyEqual<uint64_t>
{
public:
    inline bool operator()(uint64_t nKey1, uint64_t nKey2) const { return nKey1 == nKey2; }
};

template <>
class KeyEqual<int64_t>
{
public:
    inline bool operator()(int64_t nKey1, int64_t nKey2) const { return nKey1 == nKey2; }
};

template <>
class KeyEqual<float>
{
public:
    inline bool operator()(float nKey1, float nKey2) const { return nKey1 == nKey2; }
};

template <>
class KeyEqual<double>
{
public:
    inline bool operator()(double nKey1, double nKey2) const { return nKey1 == nKey2; }
};

template <>
class KeyEqual<autil::uint128_t>
{
public:
    inline bool operator()(autil::uint128_t nKey1, autil::uint128_t nKey2) const { return nKey1 == nKey2; }
};

template <typename Key>
class KeyCompare
{
};

template <>
class KeyCompare<const char*>
{
public:
    inline bool operator()(const char* szKey1, const char* szKey2) const { return strcmp(szKey1, szKey2) < 0; }
};

template <>
class KeyCompare<uint32_t>
{
public:
    inline bool operator()(uint32_t nKey1, uint32_t nKey2) const { return nKey1 < nKey2; }
};

template <>
class KeyCompare<int32_t>
{
public:
    inline bool operator()(int32_t nKey1, int32_t nKey2) const { return nKey1 < nKey2; }
};

template <>
class KeyCompare<float>
{
public:
    inline bool operator()(float nKey1, float nKey2) const { return nKey1 < nKey2; }
};

template <>
class KeyCompare<double>
{
public:
    inline bool operator()(double nKey1, double nKey2) const { return nKey1 < nKey2; }
};

template <>
class KeyCompare<uint64_t>
{
public:
    inline bool operator()(uint64_t nKey1, uint64_t nKey2) const { return nKey1 < nKey2; }
};

template <>
class KeyCompare<int64_t>
{
public:
    inline bool operator()(int64_t nKey1, int64_t nKey2) const { return nKey1 < nKey2; }
};

template <>
class KeyCompare<autil::uint128_t>
{
public:
    inline bool operator()(autil::uint128_t nKey1, autil::uint128_t nKey2) const
    {
        if ((nKey1.value[0] < nKey2.value[0]))
            return true;
        if ((nKey1.value[0] > nKey2.value[0]))
            return false;
        return (nKey1.value[1] < nKey2.value[1]);
    }
};
}} // namespace indexlib::util
