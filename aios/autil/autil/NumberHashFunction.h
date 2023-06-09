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

namespace autil {

class NumberHashFunction : public HashFunctionBase
{
public:
    NumberHashFunction(const std::string& hashFunction, uint32_t partitionCount)
        :HashFunctionBase(hashFunction, partitionCount) {}
    virtual ~NumberHashFunction() {}
public:
    /*override*/virtual uint32_t getHashId(const std::string& str) const;
public:
    int64_t hashString(const std::string& str) const;
};

typedef std::shared_ptr<NumberHashFunction> NumberHashFunctionPtr;
}

