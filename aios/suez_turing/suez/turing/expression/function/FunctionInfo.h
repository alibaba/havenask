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
#include <string>

#include "autil/legacy/jsonizable.h"
#include "suez/turing/expression/common.h"

namespace suez {
namespace turing {

class FunctionInfo : public autil::legacy::Jsonizable {
public:
    FunctionInfo();
    ~FunctionInfo();

public:
    static const std::string SUB_MATCH_INFO_TYPE;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);

private:
    // for test
    bool operator==(const FunctionInfo &other) const {
        if (&other == this) {
            return true;
        }
        return funcName == other.funcName && className == other.className && moduleName == other.moduleName &&
               params == other.params && matchInfoType == other.matchInfoType &&
               isDeterministic == other.isDeterministic;
    }

public:
    std::string funcName;
    std::string className;
    std::string moduleName;
    KeyValueMap params;
    std::string matchInfoType;
    bool isDeterministic;
};

SUEZ_TYPEDEF_PTR(FunctionInfo);

} // namespace turing
} // namespace suez
