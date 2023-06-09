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
#include "iquan/common/catalog/LayerTableDef.h"

namespace iquan {

class LayerTableModel : public autil::legacy::Jsonizable {
public:
    LayerTableModel() {}

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("catalog_name", catalogName);
        json.Jsonize("database_name", databaseName);
        json.Jsonize("layer_table_name", tableName);
        json.Jsonize("content", tableContent);
    }

    bool isValid() const {
        if (tableName.empty() ||!tableContent.isValid()) {
            return false;
        }
        return true;
    }

public:
    std::string catalogName;
    std::string databaseName;
    std::string tableName;
    LayerTableDef tableContent;
};

class LayerTableModels : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override{
        json.Jsonize("layer_tables", tables);
    }

    bool isValid() const {
        for (const auto &table : tables) {
            if (!table.isValid()) {
                return false;
            }
        }
        return true;
    }

    void merge(const LayerTableModels &other) { tables.insert(tables.end(), other.tables.begin(), other.tables.end()); }
    bool empty() const { return tables.empty(); }
public:
    std::vector<LayerTableModel> tables;
};

} // namespace iquan
