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

namespace iquan {

class TableIdentity : public autil::legacy::Jsonizable {
public:
    TableIdentity(const std::string &dbName_, const std::string &tableName_)
        : dbName(dbName_)
        , tableName(tableName_) {}
    TableIdentity() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("database_name", dbName, dbName);
        json.Jsonize("table_name", tableName, tableName);
    }

    bool isValid() const {
        return !tableName.empty() && !dbName.empty();
    }

    bool operator==(const TableIdentity &other) const {
        return dbName == other.dbName && tableName == other.tableName;
    }

public:
    std::string dbName;
    std::string tableName;
};

} // namespace iquan
