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

#include <vector>

#include "autil/legacy/jsonizable.h"
#include "iquan/common/Common.h"
#include "iquan/common/catalog/FunctionDef.h"
#include "iquan/common/catalog/TvfFunctionDef.h"

namespace iquan {

struct FunctionSign {
    std::string FunctionName;
    std::string FunctionType;
    bool operator==(const FunctionSign &other) const {
        return FunctionName == other.FunctionName && FunctionType == other.FunctionType;
    }
};

class FunctionModel : public autil::legacy::Jsonizable {
public:
    FunctionModel()
        : functionVersion(1)
        , isDeterministic(0) {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("function_name", functionName);
        json.Jsonize("function_type", functionType);
        json.Jsonize("function_version", functionVersion, 1L);
        json.Jsonize("is_deterministic", isDeterministic);
        json.Jsonize("function_content_version", functionContentVersion);
        if (functionType == SQL_TVF_FUNCTION_TYPE) {
            json.Jsonize("function_content", tvfFunctionDef);
        } else {
            json.Jsonize("function_content", functionDef);
        }
    }

    bool isValid() const {
        if (functionName.empty() || functionType.empty() || functionVersion <= 0
            || (isDeterministic != 0 && isDeterministic != 1) || functionContentVersion.empty()) {
            return false;
        }
        if (functionType == SQL_TVF_FUNCTION_TYPE && !tvfFunctionDef.isValid()) {
            return false;
        }
        if (functionType != SQL_TVF_FUNCTION_TYPE && !functionDef.isValid()) {
            return false;
        }
        return true;
    }

    bool operator==(const FunctionModel &other) const {
        return functionName == other.functionName && functionType == other.functionType;
    }

public:
    std::string functionName;
    std::string functionType;
    long functionVersion;
    int isDeterministic;
    std::string functionContentVersion;

    FunctionDef functionDef;
    TvfFunctionDef tvfFunctionDef;
};

} // namespace iquan
