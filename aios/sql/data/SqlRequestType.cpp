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
#include "sql/data/SqlRequestType.h"

#include <assert.h>
#include <engine/TypeContext.h>
#include <iosfwd>
#include <memory>

#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Data.h"
#include "navi/engine/Type.h"
#include "sql/common/Log.h"
#include "sql/data/SqlQueryRequest.h"
#include "sql/data/SqlRequestData.h"

using namespace std;

namespace sql {
AUTIL_LOG_SETUP(sql, SqlRequestType);

const std::string SqlRequestType::TYPE_ID = "sql.request_type_id";

SqlRequestType::SqlRequestType()
    : navi::Type(__FILE__, TYPE_ID) {}

SqlRequestType::~SqlRequestType() {}

navi::TypeErrorCode SqlRequestType::serialize(navi::TypeContext &ctx,
                                              const navi::DataPtr &data) const {
    auto *sqlRequestData = dynamic_cast<SqlRequestData *>(data.get());
    if (!sqlRequestData) {
        return navi::TEC_NULL_DATA;
    }
    auto sqlRequest = sqlRequestData->getSqlRequest();
    assert(sqlRequest != nullptr);
    sqlRequest->serialize(ctx.getDataBuffer());
    return navi::TEC_NONE;
}

navi::TypeErrorCode SqlRequestType::deserialize(navi::TypeContext &ctx, navi::DataPtr &data) const {
    SqlQueryRequestPtr sqlRequest(new SqlQueryRequest());
    sqlRequest->deserialize(ctx.getDataBuffer());
    if (!sqlRequest->validate()) {
        SQL_LOG(WARN, "validate sql query request failed.");
        return navi::TEC_NULL_DATA;
    }
    SqlRequestDataPtr sqlRequestData(new SqlRequestData(sqlRequest));
    data = sqlRequestData;
    return navi::TEC_NONE;
}

REGISTER_TYPE(SqlRequestType);

} // namespace sql
