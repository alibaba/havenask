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
#include "suez/turing/expression/function/FunctionInfo.h"

#include <iosfwd>
#include <map>

#include "autil/legacy/jsonizable.h"
#include "suez/turing/expression/common.h"

using namespace std;

namespace suez {
namespace turing {

const std::string FunctionInfo::SUB_MATCH_INFO_TYPE = "sub";
FunctionInfo::FunctionInfo() : isDeterministic(true) {}

FunctionInfo::~FunctionInfo() {}

void FunctionInfo::Jsonize(JsonWrapper &json) {
    json.Jsonize("name", funcName, funcName);
    json.Jsonize("class_name", className, className);
    json.Jsonize("module_name", moduleName, moduleName);
    json.Jsonize("parameters", params, params);
    json.Jsonize("match_info_type", matchInfoType, matchInfoType);
    json.Jsonize("is_deterministic", isDeterministic, isDeterministic);
}

} // namespace turing
} // namespace suez
