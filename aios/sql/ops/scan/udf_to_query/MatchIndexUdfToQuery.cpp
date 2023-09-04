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
#include "sql/ops/scan/udf_to_query/MatchIndexUdfToQuery.h"

#include <iosfwd>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "ha3/common/Query.h"
#include "ha3/common/QueryInfo.h"
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"
#include "ha3/isearch.h"
#include "rapidjson/document.h"
#include "sql/common/common.h"
#include "sql/ops/condition/SqlJsonUtil.h"
#include "sql/ops/scan/udf_to_query/UdfToQuery.h"

using namespace std;

using namespace isearch::common;

namespace sql {
AUTIL_LOG_SETUP(sql, MatchIndexUdfToQuery);

Query *MatchIndexUdfToQuery::toQuery(const autil::SimpleValue &condition,
                                     const UdfToQueryParam &condParam) {
    if (!condition.IsObject() || !condition.HasMember(SQL_CONDITION_OPERATOR)
        || !condition.HasMember(SQL_CONDITION_PARAMETER)) {
        return nullptr;
    }
    string op(condition[SQL_CONDITION_OPERATOR].GetString());
    if (op != SQL_UDF_MATCH_OP) {
        return nullptr;
    }
    const auto &param = condition[SQL_CONDITION_PARAMETER];
    if ((!param.IsArray()) || param.Size() == 0) {
        return nullptr;
    }
    string queryStr;
    QueryOperator defaultOp = condParam.queryInfo->getDefaultOperator();
    string defaultIndex = condParam.queryInfo->getDefaultIndexName();
    unordered_set<string> noTokenIndexs;
    unordered_map<string, string> indexAnalyzerMap;
    string globalAnalyzer;
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
    Term term;
    term.setIndexName(defaultIndex.c_str());
    term.setWord(queryStr.c_str());
    Query *query = new TermQuery(term, "");
    return postProcessQuery(tokenQuery,
                            removeStopWords,
                            defaultOp,
                            queryStr,
                            globalAnalyzer,
                            noTokenIndexs,
                            indexAnalyzerMap,
                            condParam.indexInfos,
                            condParam.analyzerFactory,
                            query);
}

} // namespace sql
