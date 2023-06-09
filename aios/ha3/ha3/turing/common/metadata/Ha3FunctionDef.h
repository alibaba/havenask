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

#include <string>
#include <vector>
#include "autil/legacy/jsonizable.h"
#include "iquan/common/Common.h"
#include "iquan/common/catalog/FunctionDef.h"
#include "iquan/common/IquanException.h"

namespace isearch {
namespace turing {

class Ha3FunctionDef : public autil::legacy::Jsonizable {
public:
    Ha3FunctionDef()
        : isDeterministic(1) {}

    bool isValid() const {
        if (functionName.empty()
            || functionType.empty()
            || (isDeterministic != 0 && isDeterministic != 1)
            || functionContentVersion.empty()
            || !functionContent.isValid())
        {
            return false;
        }
        return true;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        if (json.GetMode() == FROM_JSON) {
            json.Jsonize("function_name", functionName);
            json.Jsonize("function_type", functionType);
            json.Jsonize("is_deterministic", isDeterministic, isDeterministic);
            json.Jsonize("function_content_version", functionContentVersion);
            json.Jsonize("function_content", functionContent);
        } else {
            throw iquan::IquanException("Ha3FunctionDef dose not support TO_JSON");
        }
    }

public:
    std::string functionName;
    std::string functionType;
    int isDeterministic;
    std::string functionContentVersion;
    iquan::FunctionDef functionContent;
};

class Ha3FunctionsDef : public autil::legacy::Jsonizable {
public:
    bool isValid() const {
        return true;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        if (json.GetMode() == FROM_JSON) {
            json.Jsonize("functions", functionDefList);
        } else {
            throw iquan::IquanException(
                    "Ha3FunctionVectorDef dose not support TO_JSON");
        }
    }

public:
    std::vector<Ha3FunctionDef> functionDefList;
};

} // namespace turing
} // namespace isearch
