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
#include <stddef.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "sql/ops/join/JoinBaseParamR.h"
#include "table/Table.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace sql {
class JoinInfo;
} // namespace sql

namespace table {
class Column;
} // namespace table

namespace sql {
class CalcTableR;

class JoinBase {
public:
    JoinBase(const JoinBaseParamR &joinBaseParam);
    virtual ~JoinBase() {}
    JoinBase(const JoinBase &) = delete;
    JoinBase &operator=(const JoinBase &) = delete;

public:
    virtual bool generateResultTable(const std::vector<size_t> &leftTableIndexes,
                                     const std::vector<size_t> &rightTableIndexes,
                                     const table::TablePtr &leftTable,
                                     const table::TablePtr &rightTable,
                                     size_t joinedRowCount,
                                     table::TablePtr &outputTable)
        = 0;
    virtual bool
    finish(const table::TablePtr &leftTable, size_t offset, table::TablePtr &outputTable) {
        return true;
    }
    virtual bool initJoinedTable(const table::TablePtr &leftTable,
                                 const table::TablePtr &rightTable,
                                 table::TablePtr &outputTable);

protected:
    bool evaluateJoinedTable(const std::vector<size_t> &leftTableIndexes,
                             const std::vector<size_t> &rightTableIndexes,
                             const table::TablePtr &leftTable,
                             const table::TablePtr &rightTable,
                             table::TablePtr &outputTable);
    bool evaluateLeftTableColumns(const std::vector<size_t> &rowIndexes,
                                  const table::TablePtr &inputTable,
                                  size_t joinIndexStart,
                                  table::TablePtr &outputTable);
    bool
    evaluateEmptyColumns(size_t fieldIndex, size_t joinIndexStart, table::TablePtr &outputTable);
    bool appendColumns(const table::TablePtr &inputTable, table::TablePtr &outputTable) const;

private:
    bool getColumnInfo(const table::TablePtr &table,
                       const std::string &field,
                       table::Column *&tableColumn,
                       table::ValueType &vt) const;
    bool declareTableColumn(const table::TablePtr &inputTable,
                            table::TablePtr &outputTable,
                            const std::string &inputField,
                            const std::string &outputField) const;
    bool evaluateTableColumn(const table::TablePtr &inputTable,
                             table::TablePtr &outputTable,
                             const std::string &inputField,
                             const std::string &outputField,
                             size_t joinIndex,
                             const std::vector<size_t> &rowIndexes);
    bool evaluateRightTableColumns(const std::vector<size_t> &rowIndexes,
                                   const table::TablePtr &inputTable,
                                   size_t joinIndexStart,
                                   table::TablePtr &outputTable);

protected:
    const JoinBaseParamR &_joinBaseParam;
};

typedef std::shared_ptr<JoinBase> JoinBasePtr;
} // namespace sql
