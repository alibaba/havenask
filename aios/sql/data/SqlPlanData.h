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

#include "autil/Log.h"
#include "iquan/jni/IquanDqlRequest.h"
#include "iquan/jni/SqlPlan.h"
#include "navi/engine/Data.h"
#include "sql/data/SqlPlanType.h"

namespace sql {

class SqlPlanData : public navi::Data {
public:
    SqlPlanData(iquan::IquanDqlRequestPtr iquanRequest, iquan::SqlPlanPtr sqlPlan)
        : navi::Data(SqlPlanType::TYPE_ID)
        , _iquanSqlRequest(std::move(iquanRequest))
        , _sqlPlan(std::move(sqlPlan)) {}
    ~SqlPlanData() {};

private:
    SqlPlanData(const SqlPlanData &) = delete;
    SqlPlanData &operator=(const SqlPlanData &) = delete;

public:
    iquan::SqlPlanPtr &getSqlPlan() {
        return _sqlPlan;
    }
    iquan::IquanDqlRequestPtr &getIquanSqlRequest() {
        return _iquanSqlRequest;
    }

private:
    iquan::IquanDqlRequestPtr _iquanSqlRequest;
    iquan::SqlPlanPtr _sqlPlan;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SqlPlanData> SqlPlanDataPtr;

} // namespace sql
