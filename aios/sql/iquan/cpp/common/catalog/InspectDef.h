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

#include "autil/legacy/jsonizable.h"
#include "iquan/common/Common.h"

namespace iquan {

class InspectTableDef : public autil::legacy::Jsonizable {
public:
    bool isValid() const {
        if (catalogName.empty() || databaseName.empty() || tableName.empty()) {
            return false;
        }
        return true;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("catalog_name", catalogName);
        json.Jsonize("database_name", databaseName);
        json.Jsonize("table_name", tableName);
    }

public:
    std::string catalogName;
    std::string databaseName;
    std::string tableName;
};

class InspectFunctionDef : public autil::legacy::Jsonizable {
public:
    bool isValid() const {
        if (catalogName.empty() || databaseName.empty() || functionName.empty()) {
            return false;
        }
        return true;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("catalog_name", catalogName);
        json.Jsonize("database_name", databaseName);
        json.Jsonize("function_name", functionName);
    }

public:
    std::string catalogName;
    std::string databaseName;
    std::string functionName;
};

class InspectDataBaseDef : public autil::legacy::Jsonizable {
public:
    bool isValid() const {
        if (catalogName.empty() || databaseName.empty()) {
            return false;
        }
        return true;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("catalog_name", catalogName);
        json.Jsonize("database_name", databaseName);
    }

public:
    std::string catalogName;
    std::string databaseName;
};

class InspectCatalogDef : public autil::legacy::Jsonizable {
public:
    bool isValid() const { return !catalogName.empty(); }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override { json.Jsonize("catalog_name", catalogName); }

public:
    std::string catalogName;
};

} // namespace iquan
