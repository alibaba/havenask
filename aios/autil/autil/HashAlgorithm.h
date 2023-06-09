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

#include <stddef.h>
#include <stdint.h>

#include "autil/LongHashValue.h"

namespace autil {

class HashAlgorithm
{
public:
    HashAlgorithm();
    ~HashAlgorithm();
public:
    static uint32_t hashString(const char *str, uint32_t dwHashType);
    static uint32_t hashString(const char *str, size_t size, uint32_t dwHashType);
    static uint64_t hashString64(const char *str);
    static uint64_t hashString64(const char *str, size_t size);
    static uint64_t hashNumber64(int32_t number);
    static uint128_t hashString128(const char* str);
    static uint128_t hashString128(const char* str, size_t size);
public:
    //deprecated hash function, for it not produces hash value unifromly
    static uint64_t hashString64Deprecated(const char *str, uint32_t dwHashType);
    static uint64_t hashString64Deprecated(const char *str, size_t size, uint32_t dwHashType);
};

}

