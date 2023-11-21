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

#include "autil/legacy/any.h"
#include "autil/legacy/jsonizable.h"

namespace iquan {

enum CompareOp {
    EQUALS,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
    LESS_THAN,
    LESS_THAN_OR_EQUAL
};

class LayerTablePlanMetaDef : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("catalog_name", catalogName);
        json.Jsonize("database_name", dbName);
        json.Jsonize("layer_table_name", layerTableName);
        json.Jsonize("layer_field", fieldName);
        json.Jsonize("dynamic_params_index", dynamicParamsIndex);
        json.Jsonize("reverse", isRev);
        json.Jsonize("op", op);
        json.Jsonize("normalized_value", value);
    }

public:
    std::string catalogName;
    std::string dbName;
    std::string layerTableName;
    std::string fieldName;
    int dynamicParamsIndex;
    bool isRev;
    std::string op;
    autil::legacy::Any value;
};

class LayerTablePlanMeta {
public:
    LayerTablePlanMeta(const LayerTablePlanMetaDef &metaDef);

public:
    std::string catalogName;
    std::string dbName;
    std::string layerTableName;
    std::string fieldName;
    int dynamicParamsIndex;
    bool isRev;
    CompareOp op;
    autil::legacy::Any value;
};

} // namespace iquan
