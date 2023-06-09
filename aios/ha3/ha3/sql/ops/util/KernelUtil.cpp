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
#include "ha3/sql/ops/util/KernelUtil.h"

#include <iosfwd>

#include "ha3/sql/common/common.h"
#include "ha3/sql/data/TableData.h"
#include "navi/engine/Data.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "table/Table.h"

using namespace std;
using namespace table;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, KernelUtil);
const std::string KernelUtil::FIELD_PREIFX = string(1, SQL_COLUMN_PREFIX);

void KernelUtil::stripName(std::string &name) {
    if (!name.empty() && name[0] == SQL_COLUMN_PREFIX) {
        name.erase(0, 1);
    }
}

void KernelUtil::stripName(std::vector<std::string> &vec) {
    for (auto &v : vec) {
        stripName(v);
    }
}

table::TablePtr
KernelUtil::getTable(const navi::DataPtr &data) {
    if (!data) {
        SQL_LOG(ERROR, "invalid input table");
        return nullptr;
    }

    TableDataPtr tableData = dynamic_pointer_cast<TableData>(data);
    if (!tableData) {
        SQL_LOG(ERROR, "invalid input table");
        return nullptr;
    }

    TablePtr inputTable = tableData->getTable();
    if (!inputTable) {
        SQL_LOG(ERROR, "invalid input table");
        return nullptr;
    }
    return inputTable;
}

table::TablePtr KernelUtil::getTable(const navi::DataPtr &data,
                                     navi::GraphMemoryPoolResource *memoryPoolResource) {
    return getTable(data, memoryPoolResource, false);
}

table::TablePtr KernelUtil::getTable(const navi::DataPtr &data,
                                     navi::GraphMemoryPoolResource *memoryPoolResource,
                                     bool needClone) {
    auto inputTable = getTable(data);
    if (needClone) {
        auto tableClone = inputTable->clone(memoryPoolResource->getPool());
        if (!tableClone) {
            SQL_LOG(ERROR, "clone table error.");
            return nullptr;
        }
        inputTable = tableClone;
    }

    return inputTable;
}

} // namespace sql
} // namespace isearch
