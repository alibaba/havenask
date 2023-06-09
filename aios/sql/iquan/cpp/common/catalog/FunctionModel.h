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
#include "iquan/common/catalog/FunctionCommonDef.h"
#include "iquan/common/catalog/FunctionDef.h"

namespace iquan {

class FunctionModel : public FunctionModelBase {
public:
    FunctionModel() {}

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        FunctionModelBase::Jsonize(json);
        json.Jsonize("function_content", functionContent);
    }

    bool isValid() const { return FunctionModelBase::isValid() && functionContent.isValid(); }

public:
    FunctionDef functionContent;
};

class FunctionModels : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override { json.Jsonize("functions", functions); }

    bool isValid() const {
        if (functions.empty()) {
            return false;
        }
        for (const auto &function : functions) {
            if (!function.isValid()) {
                return false;
            }
        }
        return true;
    }

    void merge(const FunctionModels &other) {
        functions.insert(functions.end(), other.functions.begin(), other.functions.end());
    }

    void insert(const FunctionModel &other) {
        if (!exist(other)) {
            functions.emplace_back(other);
        }
    }

    bool exist(const FunctionModel &other) {
        for (const auto &funcModel : functions) {
            if (funcModel == other) {
                return true;
            }
        }
        return false;
    }

public:
    std::vector<FunctionModel> functions;
};

} // namespace iquan
