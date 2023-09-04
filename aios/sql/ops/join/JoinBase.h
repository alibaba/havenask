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

struct JoinBaseParam {
    JoinBaseParam() {};
    JoinBaseParam(const std::vector<std::string> &leftInputFields,
                  const std::vector<std::string> &rightInputFields,
                  const std::vector<std::string> &outputFields,
                  JoinInfo *joinInfo,
                  std::shared_ptr<autil::mem_pool::Pool> poolPtr,
                  autil::mem_pool::Pool *pool)
        : _leftInputFields(leftInputFields)
        , _rightInputFields(rightInputFields)
        , _outputFields(outputFields)
        , _joinInfo(joinInfo)
        , _poolPtr(poolPtr)
        , _pool(pool) {}
    std::vector<std::string> _leftInputFields;
    std::vector<std::string> _rightInputFields;
    std::vector<std::string> _outputFields;
    std::unordered_map<std::string, std::string> _defaultValue;
    JoinInfo *_joinInfo;
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    autil::mem_pool::Pool *_pool;
    std::shared_ptr<CalcTableR> _calcTableR;
};

class JoinBase {
public:
    JoinBase(const JoinBaseParam &joinBaseParam);
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
    const JoinBaseParam &_joinBaseParam;
};

typedef std::shared_ptr<JoinBase> JoinBasePtr;
} // namespace sql
