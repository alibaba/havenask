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
#include "ha3/sql/data/TableType.h"

#include <iosfwd>

#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Data.h"
#include "navi/engine/Type.h"
#include "table/Table.h"
#include "ha3/sql/data/TableData.h"

namespace navi {
class TypeContext;
} // namespace navi

using namespace std;
using namespace table;

namespace isearch {
namespace sql {

const std::string TableType::TYPE_ID = "ha3.sql.table_type_id";

TableType::TableType()
    : Type(TYPE_ID) {}

TableType::~TableType() {}

navi::TypeErrorCode TableType::serialize(navi::TypeContext &ctx, const navi::DataPtr &data) const
{
    auto *tableData = dynamic_cast<TableData *>(data.get());
    if (!tableData) {
        return navi::TEC_NULL_DATA;
    }
    auto table = tableData->getTable();
    assert(table != nullptr);
    table->serialize(ctx.getDataBuffer());
    return navi::TEC_NONE;
}

navi::TypeErrorCode TableType::deserialize(navi::TypeContext &ctx, navi::DataPtr &data) const {
    // TODO: use own pool
    TablePtr table(new Table(ctx.getPool()));
    table->deserialize(ctx.getDataBuffer());
    TableDataPtr tableData(new TableData(table));
    data = tableData;
    return navi::TEC_NONE;
}

REGISTER_TYPE(TableType);

} // namespace sql
} // namespace isearch
