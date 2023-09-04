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
#include "sql/ops/scan/udf_to_query/InUdfToQuery.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/TimeUtility.h"
#include "autil/legacy/RapidJsonHelper.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/NumberTerm.h"
#include "ha3/common/Query.h"
#include "ha3/common/Term.h"
#include "ha3/isearch.h"
#include "rapidjson/document.h"
#include "sql/common/IndexInfo.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/ops/condition/SqlJsonUtil.h"
#include "sql/ops/scan/udf_to_query/UdfToQuery.h"

using namespace std;
using namespace autil;

using namespace isearch::common;

namespace sql {
AUTIL_LOG_SETUP(sql, InUdfToQuery);

Query *InUdfToQuery::toQuery(const SimpleValue &condition, const UdfToQueryParam &condParam) {
    const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
    if (!param.IsArray() || param.Size() < 2) {
        SQL_LOG(DEBUG, "param size %u < 2", param.Size());
        return nullptr;
    }
    if (!SqlJsonUtil::isColumn(param[0])) {
        SQL_LOG(DEBUG, "[%s] not column", string(param[0].GetString()).c_str());
        return nullptr;
    }
    return genQuery(condParam, param);
}

Query *InUdfToQuery::genQuery(const UdfToQueryParam &condParam, const SimpleValue &param) {
    const string &columnName = SqlJsonUtil::getColumnName(param[0]);
    auto iter = condParam.indexInfo->find(columnName);
    if (iter == condParam.indexInfo->end()) {
        SQL_LOG(DEBUG, "[%s] not index", columnName.c_str());
        return nullptr;
    }
    const string &indexName = iter->second.name;
    const string &indexType = iter->second.type;
    unique_ptr<MultiTermQuery> query(new MultiTermQuery("", OP_OR));
    if (indexType == "number") {
        int64_t numVal = 0;
        for (size_t i = 1; i < param.Size(); ++i) {
            const SimpleValue &value = param[i];
            if (value.IsInt64()) {
                numVal = value.GetInt64();
                TermPtr term(new NumberTerm(numVal, indexName.c_str(), RequiredFields()));
                query->addTerm(term);
            } else {
                SQL_LOG(DEBUG,
                        "parse param[%lu]:`%s` to number failed",
                        i,
                        RapidJsonHelper::SimpleValue2Str(value).c_str());
                return nullptr;
            }
        }
    } else if (indexType == "string") {
        string strVal;
        for (size_t i = 1; i < param.Size(); ++i) {
            const SimpleValue &value = param[i];
            if (value.IsString()) {
                strVal = string(value.GetString());
                TermPtr term(new Term(strVal, indexName.c_str(), RequiredFields()));
                query->addTerm(term);
            } else {
                SQL_LOG(DEBUG,
                        "parse param[%lu]:`%s` to string failed",
                        i,
                        RapidJsonHelper::SimpleValue2Str(value).c_str());
                return nullptr;
            }
        }
    } else {
        return nullptr;
    }
    return query.release();
}

} // namespace sql
