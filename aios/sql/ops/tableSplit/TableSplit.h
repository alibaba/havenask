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

#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/HashFunctionBase.h"
#include "autil/Log.h"
#include "autil/RangeUtil.h"
#include "sql/common/TableDistribution.h"
#include "table/Row.h"

namespace table {
class Column;
class Table;
} // namespace table

namespace sql {

class TableSplit {
public:
    void init(const TableDistribution &dist);
    bool split(table::Table &table, std::vector<std::vector<table::Row>> &partRows);

private:
    void initPartColumns(table::Table &table, std::vector<table::Column *> &partCols);
    table::Column *getColumn(const std::string &field, table::Table &table);
    bool doSplit(table::Table &table,
                 const std::vector<table::Column *> &partCols,
                 std::vector<std::vector<table::Row>> &partRows);
    bool collectHashValues(table::Column &col, std::vector<std::string> &hashValues);
    void splitByHashValues(const std::vector<std::vector<std::string>> &multiHashValues,
                           table::Table &table,
                           std::vector<std::vector<table::Row>> &partRows);
    void splitByHashRange(const std::vector<std::vector<std::string>> &multiHashValues,
                          table::Table &table,
                          std::vector<std::vector<table::Row>> &partRows);

    template <class Func>
    void splitByHashRange(const std::vector<std::vector<std::string>> &multiHashValues,
                          table::Table &table,
                          std::vector<std::vector<table::Row>> &partRows,
                          Func func);

    int getPartId(uint32_t point);
    std::pair<int, int> range2partId(const autil::PartitionRange &range);

private:
    int32_t _partCnt {0};
    HashMode _hashMode;
    autil::HashFunctionBasePtr _hashFunc;
    std::vector<uint32_t> _rangeBounds;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace sql
