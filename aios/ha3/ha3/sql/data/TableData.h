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

#include <memory>
#include <utility>

#include "ha3/sql/data/TableType.h"
#include "navi/engine/Data.h"
#include "table/Table.h"

namespace isearch {
namespace sql {

class TableData : public navi::Data {
public:
    TableData(table::TablePtr table)
        : navi::Data(TableType::TYPE_ID, nullptr)
        , _table(std::move(table)) {}

    ~TableData() {}

private:
    TableData(const TableData &);
    TableData &operator=(const TableData &);

public:
    table::TablePtr &getTable() {
        return _table;
    }

private:
    table::TablePtr _table;
};

typedef std::shared_ptr<TableData> TableDataPtr;

} // namespace sql
} // namespace isearch
