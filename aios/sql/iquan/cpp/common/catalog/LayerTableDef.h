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
#include <unordered_map>
#include <map>

#include "autil/legacy/jsonizable.h"
#include "autil/legacy/any.h"

namespace iquan {

class Layer : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("database_name", dbName);
        json.Jsonize("table_name", tableName);
        json.Jsonize("layer_info", layerInfo);
    }

    bool isValid() const { return true; }
public:
    std::string dbName;
    std::string tableName;
    std::map<std::string, autil::legacy::Any> layerInfo;
};

class LayerFormat : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("field_name", fieldName);
        json.Jsonize("layer_method", layerMethod);
        json.Jsonize("value_type", valueType);
    }

    bool isValid() const { return true; }

public:
    std::string fieldName;
    std::string layerMethod;
    std::string valueType;
};

class LayerTableDef : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("layer_table_name", layerTableName);
        json.Jsonize("layer_format", layerFormats);
        json.Jsonize("layers", layers);
        json.Jsonize("properties", properties, properties);
    }

    bool isValid() const { return true; }
public:
    std::string layerTableName;
    std::vector<Layer> layers;
    std::vector<LayerFormat> layerFormats;
    std::map<std::string, autil::legacy::Any> properties;
};

} // namespace iquan
