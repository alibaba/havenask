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
#include "iquan/common/catalog/TableDef.h"

namespace iquan {

class TableModel : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("table_content_type", tableContentType, tableContentType);
        json.Jsonize("table_content", tableContent);
    }

    bool isValid() const {
        if (!tableContent.isValid()) {
            return false;
        }
        return true;
    }

    const std::string &tableName() const {
        return tableContent.tableName;
    }

    const std::string &tableType() const {
        return tableContent.tableType;
    }

    bool operator==(const TableModel &other) const {
        return tableName() == other.tableName();
    }

public:
    std::string tableContentType;
    TableDef tableContent;
};

} // namespace iquan
