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

#include <bits/stdint-uintn.h>
#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/HashFunctionBase.h"

namespace autil {

class MurmurHash3Function : public HashFunctionBase {
public:
    static constexpr uint32_t DEFAULT_HASH_SEED = 104729;

    MurmurHash3Function(const std::string &hashFunction,
                        const std::map<std::string, std::string> &param,
                        uint32_t partitionCount);
    virtual ~MurmurHash3Function() {}

    static const char *name() { return "MURMUR_HASH3"; }

public:
    bool init() override;
    uint32_t getHashId(const std::string &str) const override;

private:
    std::map<std::string, std::string> _params;
    uint32_t _seed;
};

typedef std::shared_ptr<MurmurHash3Function> MurmurHash3FunctionPtr;

} // namespace autil
