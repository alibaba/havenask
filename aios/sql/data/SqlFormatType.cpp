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
#include "sql/data/SqlFormatType.h"

#include <assert.h>
#include <engine/TypeContext.h>
#include <iosfwd>
#include <memory>

#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Data.h"
#include "navi/engine/Type.h"
#include "sql/common/Log.h"
#include "sql/data/SqlFormatData.h"

using namespace std;

namespace sql {
AUTIL_LOG_SETUP(sql, SqlFormatType);

const std::string SqlFormatType::TYPE_ID = "sql.format_type_id";

SqlFormatType::SqlFormatType()
    : navi::Type(__FILE__, TYPE_ID) {}

SqlFormatType::~SqlFormatType() {}

navi::TypeErrorCode SqlFormatType::serialize(navi::TypeContext &ctx,
                                             const navi::DataPtr &data) const {
    auto *sqlFormatData = dynamic_cast<SqlFormatData *>(data.get());
    if (!sqlFormatData) {
        return navi::TEC_NULL_DATA;
    }
    ctx.getDataBuffer().write(sqlFormatData->getSqlFormat());
    return navi::TEC_NONE;
}

navi::TypeErrorCode SqlFormatType::deserialize(navi::TypeContext &ctx, navi::DataPtr &data) const {
    std::string sqlFormat;
    ctx.getDataBuffer().read(sqlFormat);
    SqlFormatDataPtr sqlFormatData(new SqlFormatData(sqlFormat));
    data = sqlFormatData;
    return navi::TEC_NONE;
}

REGISTER_TYPE(SqlFormatType);

} // namespace sql
