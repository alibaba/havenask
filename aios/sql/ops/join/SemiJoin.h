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
#include <vector>

#include "sql/ops/join/JoinBase.h"
#include "table/Table.h"

namespace sql {

class SemiJoin : public JoinBase {
public:
    SemiJoin(const JoinBaseParam &joinBaseParam);
    ~SemiJoin() {}
    SemiJoin(const SemiJoin &) = delete;
    SemiJoin &operator=(const SemiJoin &) = delete;

public:
    bool generateResultTable(const std::vector<size_t> &leftTableIndexes,
                             const std::vector<size_t> &rightTableIndexes,
                             const table::TablePtr &leftTable,
                             const table::TablePtr &rightTable,
                             size_t joinedRowCount,
                             table::TablePtr &outputTable) override;
    bool
    finish(const table::TablePtr &leftTable, size_t offset, table::TablePtr &outputTable) override;
    bool initJoinedTable(const table::TablePtr &leftTable,
                         const table::TablePtr &rightTable,
                         table::TablePtr &outputTable) override;

private:
    std::vector<bool> _joinedFlag;
    table::TablePtr _joinedTable;
};

typedef std::shared_ptr<SemiJoin> SemiJoinPtr;
} // namespace sql
