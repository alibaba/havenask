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

#include "ha3/sql/data/SqlQueryRequest.h"
#include "ha3/sql/data/SqlRequestType.h"
#include "navi/engine/Data.h"

namespace isearch {
namespace sql {

class SqlRequestData : public navi::Data {
public:
    SqlRequestData(SqlQueryRequestPtr query)
        : navi::Data(SqlRequestType::TYPE_ID, nullptr)
        , _sqlQuery(std::move(query)) {}
    ~SqlRequestData() {}

private:
    SqlRequestData(const SqlRequestData &) = delete;
    SqlRequestData &operator=(const SqlRequestData &) = delete;

public:
    SqlQueryRequestPtr &getSqlRequest() {
        return _sqlQuery;
    }

private:
    SqlQueryRequestPtr _sqlQuery;
};

typedef std::shared_ptr<SqlRequestData> SqlRequestDataPtr;

} // namespace sql
} // end namespace isearch
