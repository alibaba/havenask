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
#include "ha3/sql/ops/scan/builtin/QueryUdfToQuery.h"

#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "ha3/common/QueryInfo.h"
#include "ha3/common/Term.h"
#include "ha3/isearch.h"
#include "ha3/queryparser/ParserContext.h"
#include "ha3/queryparser/QueryParser.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/condition/SqlJsonUtil.h"
#include "ha3/sql/ops/scan/Ha3ScanConditionVisitorParam.h"
#include "rapidjson/document.h"

using namespace std;
using namespace autil;

using namespace isearch::common;
using namespace isearch::queryparser;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, QueryUdfToQuery);

Query *QueryUdfToQuery::toQuery(const SimpleValue &condition,
                                const Ha3ScanConditionVisitorParam &condParam) {
    if (!condition.IsObject() || !condition.HasMember(SQL_CONDITION_OPERATOR)
        || !condition.HasMember(SQL_CONDITION_PARAMETER)) {
        return nullptr;
    }
    string op(condition[SQL_CONDITION_OPERATOR].GetString());
    if (op != SQL_UDF_QUERY_OP) {
        return nullptr;
    }
    const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
    if (!param.IsArray() || param.Size() == 0) {
        return nullptr;
    }
    string queryStr;
    QueryOperator defaultOp = condParam.queryInfo->getDefaultOperator();
    string defaultIndex = condParam.queryInfo->getDefaultIndexName();
    unordered_set<string> noTokenIndexs;
    string globalAnalyzer;
    unordered_map<string, string> indexAnalyzerMap;
    bool tokenQuery = true;
    bool removeStopWords = true;
    if (param.Size() == 1) {
        queryStr = param[0].GetString();
    } else if (param.Size() == 2) {
        if (SqlJsonUtil::isColumn(param[0])) {
            defaultIndex = SqlJsonUtil::getColumnName(param[0]);
        } else {
            defaultIndex = param[0].GetString();
        }
        queryStr = param[1].GetString();
    } else {
        if (SqlJsonUtil::isColumn(param[0])) {
            defaultIndex = SqlJsonUtil::getColumnName(param[0]);
        } else {
            defaultIndex = param[0].GetString();
        }
        queryStr = param[1].GetString();
        string defaultOpStr;
        parseKvPairInfo(param[2].GetString(),
                        indexAnalyzerMap,
                        globalAnalyzer,
                        defaultOpStr,
                        noTokenIndexs,
                        tokenQuery,
                        removeStopWords);
        if (defaultOpStr == "AND" || defaultOpStr == "and") {
            defaultOp = OP_AND;
        } else if (defaultOpStr == "OR" || defaultOpStr == "or") {
            defaultOp = OP_OR;
        }
    }
    queryparser::QueryParser queryParser(defaultIndex.c_str(), defaultOp, false);
    SQL_LOG(DEBUG, "QueryText: [%s]", queryStr.c_str());
    unique_ptr<queryparser::ParserContext> context(queryParser.parse(queryStr.c_str()));
    if (queryparser::ParserContext::OK != context->getStatus()) {
        SQL_LOG(WARN, "parse query failed: [%s], error msg: [%s]", queryStr.c_str(),
                context->getStatusMsg().c_str());
        return nullptr;
    }

    vector<Query *> querys = context->stealQuerys();
    return postProcessQuery(tokenQuery || condParam.analyzerFactory,
                            removeStopWords,
                            defaultOp,
                            queryStr,
                            globalAnalyzer,
                            noTokenIndexs,
                            indexAnalyzerMap,
                            condParam.indexInfos,
                            condParam.analyzerFactory,
                            reserveFirstQuery(querys));
}

} // namespace sql
} // namespace isearch
