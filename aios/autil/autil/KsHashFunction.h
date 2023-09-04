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
#include <stdint.h>
#include <string>

#include "autil/HashFunctionBase.h"

namespace autil {

class KsHashFunction : public HashFunctionBase {
public:
    KsHashFunction(const std::string &hashFunction, uint32_t partitionCout = 65536, uint32_t ksHashRange = 720)
        : HashFunctionBase(hashFunction, partitionCout), _ksHashRange(ksHashRange) {}
    virtual ~KsHashFunction() {}

public:
    virtual uint32_t getHashId(const std::string &str) const;
    uint32_t getKsHashRange() const { return _ksHashRange; }

public:
    static uint32_t getHashId(uint64_t key, uint32_t rangeBefore, uint32_t rangeAfter);
    static uint32_t getHashId(uint32_t key, uint32_t rangeBefore, uint32_t rangeAfter);

private:
    uint32_t _ksHashRange;
};

typedef std::shared_ptr<KsHashFunction> KsHashFunctionPtr;

} // namespace autil
