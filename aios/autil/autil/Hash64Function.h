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
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/HashFunctionBase.h"

namespace autil {

class Hash64Function : public HashFunctionBase {
public:
    Hash64Function(const std::string &hashFunction, uint32_t partitionCount)
        : HashFunctionBase(hashFunction, partitionCount) {}
    virtual ~Hash64Function() {}

public:
    /*override*/ virtual uint32_t getHashId(const std::string &str) const;
    /* override */ uint32_t getHashId(const char *buf, size_t len) const;
};

typedef std::shared_ptr<Hash64Function> Hash64FunctionPtr;

} // namespace autil
