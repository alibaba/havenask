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

namespace indexlib { namespace util {

class HashString
{
public:
    static inline void Hash(autil::uint128_t& t, const char* str, size_t size)
    {
        t = autil::HashAlgorithm::hashString128(str, size);
    }

    static inline void Hash(uint64_t& t, const char* str, size_t size)
    {
        t = autil::HashAlgorithm::hashString64(str, size);
    }

    static inline void Hash(uint32_t& t, const char* str, size_t size)
    {
        t = autil::HashAlgorithm::hashString(str, size, 0);
    }

    static inline uint64_t Hash(const char* data, size_t size)
    {
        uint64_t t = autil::HashAlgorithm::hashString(data, size, 0);
        t <<= 32;
        t |= autil::HashAlgorithm::hashString(data, size, 1);
        return t;
    }
};
}} // namespace indexlib::util
