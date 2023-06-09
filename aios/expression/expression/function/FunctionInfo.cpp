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
#include "expression/function/FunctionInfo.h"

using namespace std;

namespace expression {

AUTIL_LOG_SETUP(function, FunctionInfo);

FunctionInfo::FunctionInfo()
{ 
}

FunctionInfo::~FunctionInfo() { 
}

const KeyValueMap& FunctionInfo::getParams() const {
    return _params;
}

void FunctionInfo::setParams(const KeyValueMap &params) {
    _params = params;
}

void FunctionInfo::Jsonize(JsonWrapper& json) {
    json.Jsonize("name", _funcName);
    json.Jsonize("parameters", _params);
}

bool FunctionInfo::operator==(const FunctionInfo &other) const {
    if (&other == this) {
        return true;
    }
    return _funcName == other._funcName
        && _params == other._params;
}
}

