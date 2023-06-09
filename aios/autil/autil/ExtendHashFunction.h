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
#include <string>
#include <memory>

#include "autil/HashFunctionBase.h"
#include "autil/KeyRangeManager.h"

namespace autil {

class ExtendHashFunction : public HashFunctionBase
{
public:
    ExtendHashFunction(const std::string& hashFunction, uint32_t partitionCount)
        : HashFunctionBase(hashFunction, partitionCount), _hasher(partitionCount)
    {}
    ~ExtendHashFunction(){}
public:
    virtual uint32_t getHashId(const std::string& str) const;
public:
    static std::string hashString(const std::string& str);
protected:
    static std::string extendMd5(uint8_t md[16]);
private:
    AutoHashKeyRangeManager _hasher;
};

typedef std::shared_ptr<ExtendHashFunction> ExtendHashFunctionPtr;

}

