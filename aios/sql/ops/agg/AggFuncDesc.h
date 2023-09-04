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

#include <algorithm>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "sql/ops/util/KernelUtil.h"

namespace sql {

class AggFuncDesc : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("type", type);
        json.Jsonize("name", funcName);
        json.Jsonize("input", inputs, inputs);
        json.Jsonize("output", outputs);
        json.Jsonize("filter_arg", filterArg, -1);
        if (json.GetMode() == autil::legacy::Jsonizable::FROM_JSON) {
            for (size_t i = 0; i < inputs.size(); ++i) {
                KernelUtil::stripName(inputs[i]);
            }
            for (size_t i = 0; i < outputs.size(); ++i) {
                KernelUtil::stripName(outputs[i]);
            }
        }
    }

public:
    std::string type;
    std::string funcName;
    std::vector<std::string> inputs;
    std::vector<std::string> outputs;
    int32_t filterArg;
};

} // namespace sql
