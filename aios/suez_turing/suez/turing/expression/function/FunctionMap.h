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
#include <stdint.h>
#include <string>
#include <vector>

#include "matchdoc/ValueType.h"
#include "suez/turing/expression/common.h"

namespace suez {
namespace turing {

enum FuncActionScopeType {
    FUNC_ACTION_SCOPE_MAIN_DOC,
    FUNC_ACTION_SCOPE_SUB_DOC,
    FUNC_ACTION_SCOPE_ADAPTER,
};

static const uint32_t FUNCTION_UNLIMITED_ARGUMENT_COUNT = (uint32_t)-1;

struct FunctionProtoInfo {
    uint32_t argCount;
    matchdoc::BuiltinType resultType;
    bool isMulti;
    FuncActionScopeType scopeType;
    bool isDeterministic;

    FunctionProtoInfo(uint32_t argCount_ = 0,
                      matchdoc::BuiltinType resultType_ = matchdoc::bt_unknown,
                      bool isMulti_ = false,
                      FuncActionScopeType scopeType_ = FUNC_ACTION_SCOPE_ADAPTER,
                      bool isDeterministic_ = true)
        : argCount(argCount_)
        , resultType(resultType_)
        , isMulti(isMulti_)
        , scopeType(scopeType_)
        , isDeterministic(isDeterministic_) {}
    bool operator==(const FunctionProtoInfo &other) const {
        return argCount == other.argCount && resultType == other.resultType && isMulti == other.isMulti &&
               scopeType == other.scopeType && isDeterministic == other.isDeterministic;
    }
    bool operator!=(const FunctionProtoInfo &protoInfo) const { return !(*this == protoInfo); }
};

typedef std::map<std::string, FunctionProtoInfo> FuncInfoMap;
typedef std::vector<const FuncInfoMap *> FuncInfoMaps;
typedef std::map<std::string, FuncInfoMap> ClusterFuncMap;
SUEZ_TYPEDEF_PTR(ClusterFuncMap);
SUEZ_TYPEDEF_PTR(FunctionProtoInfo);

} // namespace turing
} // namespace suez
