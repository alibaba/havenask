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
#include "ha3/sql/data/SqlPlanType.h"
#include "ha3/sql/data/SqlPlanData.h"
#include "ha3/sql/common/Log.h"
#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"

using namespace std;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(data, SqlPlanType);

const std::string SqlPlanType::TYPE_ID = "ha3.sql.plan_type_id";

SqlPlanType::SqlPlanType()
    : Type(TYPE_ID) {}

SqlPlanType::~SqlPlanType() {}

navi::TypeErrorCode SqlPlanType::serialize(navi::TypeContext &ctx, const navi::DataPtr &data) const
{
    auto *sqlPlanData = dynamic_cast<SqlPlanData *>(data.get());
    if (!sqlPlanData) {
        SQL_LOG(ERROR, "sql plan data is empty");
        return navi::TEC_NULL_DATA;
    }
    auto sqlPlan = sqlPlanData->getSqlPlan();
    if (!sqlPlan) {
        SQL_LOG(ERROR, "sql plan is empty");
        return navi::TEC_NULL_DATA;
    }
    sqlPlan->serialize(ctx.getDataBuffer());
    return navi::TEC_NONE;
}

navi::TypeErrorCode SqlPlanType::deserialize(navi::TypeContext &ctx, navi::DataPtr &data) const {
    iquan::SqlPlanPtr sqlPlan(new iquan::SqlPlan());
    iquan::Status status = sqlPlan->deserialize(ctx.getDataBuffer());
    if (!status.ok()) {
        SQL_LOG(ERROR, "deserialize sql plan error, message [%s]", status.errorMessage().c_str());
        return navi::TEC_NULL_DATA;
    }

    SqlPlanDataPtr sqlPlanData(new SqlPlanData(iquan::IquanDqlRequestPtr(), sqlPlan));
    data = sqlPlanData;
    return navi::TEC_NONE;
}

REGISTER_TYPE(SqlPlanType);

} //end namespace data
} //end namespace isearch
