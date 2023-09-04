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

#include <map>
#include <memory>
#include <stdint.h>
#include <string>

#include "autil/HashFunctionBase.h"

namespace autil {

class HashFuncFactory {
public:
    HashFuncFactory();
    ~HashFuncFactory();

public:
    static HashFunctionBasePtr createHashFunc(const std::string &funcName, uint32_t partitionCount = 65536);
    static HashFunctionBasePtr createHashFunc(const std::string &funcName,
                                              const std::map<std::string, std::string> &params,
                                              uint32_t partitionCount = 65536);
};

typedef std::shared_ptr<HashFuncFactory> HashFuncFactoryPtr;

} // namespace autil
