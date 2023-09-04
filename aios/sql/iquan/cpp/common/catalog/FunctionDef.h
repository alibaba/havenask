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
#include <vector>

#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "iquan/common/catalog/FunctionCommonDef.h"

namespace iquan {

class PrototypeDef : public autil::legacy::Jsonizable {
public:
    PrototypeDef()
        : variableArgs(false) {}

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("returns", returnTypes);
        json.Jsonize("params", paramTypes);
        json.Jsonize("variable_args", variableArgs, variableArgs);
        if (json.GetMode() == TO_JSON) {
            if (!accTypes.empty()) {
                json.Jsonize("acc_types", accTypes);
            }
        } else {
            json.Jsonize("acc_types", accTypes, accTypes);
        }
    }

    bool isValid() const {
        if (returnTypes.empty() || paramTypes.empty()) {
            return false;
        }
        return true;
    }

public:
    bool variableArgs;
    std::vector<ParamTypeDef> accTypes;
    std::vector<ParamTypeDef> returnTypes;
    std::vector<ParamTypeDef> paramTypes;
};

class FunctionDef : public autil::legacy::Jsonizable {
public:
    bool isValid() const {
        if (prototypes.empty()) {
            return false;
        }
        return true;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("prototypes", prototypes);
        json.Jsonize("properties", properties, properties);
    }

public:
    std::vector<PrototypeDef> prototypes;
    autil::legacy::json::JsonMap properties;
};

} // namespace iquan
