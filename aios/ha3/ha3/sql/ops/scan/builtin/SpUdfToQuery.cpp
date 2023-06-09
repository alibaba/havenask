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
#include "ha3/sql/ops/scan/builtin/SpUdfToQuery.h"

#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "alog/Logger.h"
#include "ha3/common/QueryInfo.h"
#include "ha3/common/Term.h"
#include "ha3/isearch.h"
#include "ha3/queryparser/ParserContext.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/scan/Ha3ScanConditionVisitorParam.h"
#include "ha3/sql/ops/scan/builtin/SpQueryUdf.h"
#include "rapidjson/document.h"

using namespace std;

using namespace isearch::common;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, SpUdfToQuery);

Query *SpUdfToQuery::toQuery(const autil::SimpleValue &condition,
                             const Ha3ScanConditionVisitorParam &condParam) {
    if (!condition.IsObject() || !condition.HasMember(SQL_CONDITION_OPERATOR)
        || !condition.HasMember(SQL_CONDITION_PARAMETER)) {
        return nullptr;
    }
    string op(condition[SQL_CONDITION_OPERATOR].GetString());
    if (op != SQL_UDF_SP_QUERY_MATCH_OP) {
        return nullptr;
    }
    const auto &param = condition[SQL_CONDITION_PARAMETER];
    if ((!param.IsArray())) {
        return nullptr;
    }

    string queryStr = param[0].GetString();
    QueryOperator defaultOp = condParam.queryInfo->getDefaultOperator();
    string defaultIndex = condParam.queryInfo->getDefaultIndexName();
    unordered_set<string> noTokenIndexs;
    string globalAnalyzer;
    unordered_map<string, string> indexAnalyzerMap;
    bool tokenQuery = true;
    bool removeStopWords = true;

    SPQueryUdf queryParser(defaultIndex.c_str());
    SQL_LOG(DEBUG, "QueryText: [%s]", queryStr.c_str());
    unique_ptr<queryparser::ParserContext> context(queryParser.parse(queryStr.c_str()));
    if (queryparser::ParserContext::OK != context->getStatus()) {
        SQL_LOG(WARN, "parse query failed: [%s], error msg: [%s]", queryStr.c_str(),
                context->getStatusMsg().c_str());
        return nullptr;
    }

    vector<Query *> querys = context->stealQuerys();
    return postProcessQuery(tokenQuery,
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
