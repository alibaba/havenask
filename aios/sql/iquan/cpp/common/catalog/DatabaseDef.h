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
#include "iquan/common/catalog/FunctionModel.h"
#include "iquan/common/catalog/LayerTableModel.h"
#include "iquan/common/catalog/TableModel.h"

namespace iquan {

class DatabaseDef : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("database_name", dbName, dbName);
        json.Jsonize("tables", tables, tables);
        json.Jsonize("layer_tables", layerTables, layerTables);
        json.Jsonize("functions", functions, functions);
    }

    bool isValid() const {
        for (const auto &tableModel : tables) {
            if (!tableModel.isValid()) {
                return false;
            }
        }

        for (const auto &layerTable : layerTables) {
            if (!layerTable.isValid()) {
                return false;
            }
        }

        for (const auto &func : functions) {
            if (!func.isValid()) {
                return false;
            }
        }

        return !dbName.empty();
    }

    bool merge(DatabaseDef &other) {
        if (other.dbName != dbName) {
            return false;
        }
        bool ret = true;
        for (auto &table : other.tables) {
            ret = ret && addTable(table);
        }
        for (auto &table : other.layerTables) {
            ret = ret && addTable(table);
        }
        for (auto &functionModel : other.functions) {
            ret = ret && addFunction(functionModel);
        }
        return ret;
    }

    bool addFunction(const FunctionModel &funcModel) {
        if (hasFunction(funcModel.functionName, funcModel.functionType)) {
            return false;
        }
        functions.push_back(funcModel);
        return true;
    }

    bool addTable(const TableModel &tableModel) {
        if (hasTable(tableModel.tableName())) {
            return false;
        }
        tables.push_back(tableModel);
        return true;
    }

    bool addTable(const LayerTableModel &layerTableModel) {
        if (hasLayerTable(layerTableModel.tableName())) {
            return false;
        }
        layerTables.push_back(layerTableModel);
        return true;
    }

    bool hasTable(const std::string &tableName) const {
        for (auto &tableModel : tables) {
            if (tableModel.tableName() == tableName) {
                return true;
            }
        }
        return false;
    }

    bool hasLayerTable(const std::string &tableName) const {
        for (auto &tableModel : layerTables) {
            if (tableModel.tableName() == tableName) {
                return true;
            }
        }
        return false;
    }

    bool hasFunction(const std::string &funcName, const std::string &funcType) const {
        for (auto &func : functions) {
            if (func.functionName == funcName && func.functionType == funcType) {
                return true;
            }
        }
        return false;
    }

public:
    std::string dbName;
    std::vector<TableModel> tables;
    std::vector<LayerTableModel> layerTables;
    std::vector<FunctionModel> functions;
};

} // namespace iquan
