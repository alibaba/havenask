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
#include "sql/ops/scan/udf_to_query/UdfToQuery.h"

#include <cstddef>
#include <map>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryTokenizer.h"
#include "ha3/common/QueryValidator.h"
#include "ha3/common/StopWordsCleaner.h"
#include "ha3/search/PartitionInfoWrapper.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"

namespace suez {
namespace turing {
class IndexInfos;
} // namespace turing
} // namespace suez

using namespace std;
using namespace autil;
using namespace suez::turing;

using namespace isearch::common;
using namespace isearch::search;

namespace sql {
AUTIL_LOG_SETUP(sql, UdfToQuery);

void UdfToQuery::parseKvPairInfo(const string &kvStr,
                                 unordered_map<string, string> &indexAnalyzerMap,
                                 string &globalAnalyzerName,
                                 string &defaultOpStr,
                                 unordered_set<string> &noTokenIndexes,
                                 bool &tokenizeQuery,
                                 bool &removeStopWords) {
    map<string, string> kvPairs;
    vector<string> kvVec;
    StringUtil::split(kvVec, kvStr, SQL_FUNC_KVPAIR_SEP);
    vector<string> valueVec;
    for (auto kv : kvVec) {
        valueVec.clear();
        StringUtil::split(valueVec, kv, SQL_FUNC_KVPAIR_KV_SEP);
        if (valueVec.size() != 2) {
            continue;
        }
        kvPairs[valueVec[0]] = valueVec[1];
    }
    globalAnalyzerName = kvPairs["global_analyzer"];
    string specialAnalyzerNames = kvPairs["specific_index_analyzer"];
    if (!specialAnalyzerNames.empty()) {
        valueVec.clear();
        StringUtil::split(valueVec, specialAnalyzerNames, SQL_FUNC_KVPAIR_VALUE_SEP);
        vector<string> valueValueVec;
        for (auto kv : valueVec) {
            valueValueVec.clear();
            StringUtil::split(valueValueVec, kv, SQL_FUNC_KVPAIR_VALUE_VALUE_SEP);
            if (valueValueVec.size() != 2) {
                continue;
            }
            indexAnalyzerMap[valueValueVec[0]] = valueValueVec[1];
        }
    }
    string noTokenStr = kvPairs["no_token_indexes"];
    if (!noTokenStr.empty()) {
        valueVec.clear();
        StringUtil::split(valueVec, noTokenStr, SQL_FUNC_KVPAIR_VALUE_SEP);
        noTokenIndexes.insert(valueVec.begin(), valueVec.end());
    }
    string tokenizeQueryStr = kvPairs["tokenize_query"];
    if (!tokenizeQueryStr.empty()) {
        StringUtil::parseTrueFalse(tokenizeQueryStr, tokenizeQuery);
    }
    string removeStopWordsStr = kvPairs["remove_stopwords"];
    if (!removeStopWordsStr.empty()) {
        StringUtil::parseTrueFalse(removeStopWordsStr, removeStopWords);
    }
    defaultOpStr = kvPairs["default_op"];
}

Query *UdfToQuery::postProcessQuery(bool tokenQuery,
                                    bool removeStopWords,
                                    QueryOperator op,
                                    const string &queryStr,
                                    const string &globalAnalyzer,
                                    const unordered_set<string> &noTokenIndexs,
                                    const unordered_map<string, string> &indexAnalyzerMap,
                                    const suez::turing::IndexInfos *indexInfos,
                                    const build_service::analyzer::AnalyzerFactory *analyzerFactory,
                                    Query *query) {

    Query *resultQuery = nullptr;
    if (tokenQuery) {
        resultQuery = tokenizeQuery(query,
                                    op,
                                    noTokenIndexs,
                                    globalAnalyzer,
                                    indexAnalyzerMap,
                                    removeStopWords,
                                    indexInfos,
                                    analyzerFactory);
    } else if (removeStopWords) {
        resultQuery = cleanStopWords(query);
    }
    DELETE_AND_SET_NULL(query);
    query = resultQuery;

    if (!validateQuery(query, indexInfos)) {
        SQL_LOG(WARN, "validate query failed: [%s]", queryStr.c_str());
        DELETE_AND_SET_NULL(query);
        return nullptr;
    }
    return query;
}

Query *UdfToQuery::reserveFirstQuery(vector<Query *> &querys) {
    Query *query = nullptr;
    if (querys.size() == 0) {
        return nullptr;
    } else {
        query = querys[0];
        for (size_t i = 1; i < querys.size(); i++) {
            delete querys[i];
        }
        SQL_LOG(DEBUG, "frist query : [%s]", query->toString().c_str());
    }
    return query;
}

Query *
UdfToQuery::tokenizeQuery(isearch::common::Query *query,
                          QueryOperator qp,
                          const std::unordered_set<std::string> &noTokenIndexes,
                          const std::string &globalAnalyzer,
                          const std::unordered_map<std::string, std::string> &indexAnalyzerMap,
                          bool removeStopWords,
                          const suez::turing::IndexInfos *indexInfos,
                          const build_service::analyzer::AnalyzerFactory *analyzerFactory) {
    if (query == nullptr) {
        return nullptr;
    }
    isearch::common::QueryTokenizer queryTokenizer(analyzerFactory);
    queryTokenizer.setNoTokenizeIndexes(noTokenIndexes);
    queryTokenizer.setGlobalAnalyzerName(globalAnalyzer);
    queryTokenizer.setIndexAnalyzerNameMap(indexAnalyzerMap);
    SQL_LOG(DEBUG, "before tokenize query:%s", query->toString().c_str());
    if (!queryTokenizer.tokenizeQuery(query, indexInfos, qp, removeStopWords)) {
        auto errorResult = queryTokenizer.getErrorResult();
        SQL_LOG(WARN,
                "tokenize query failed [%s], detail: %s",
                query->toString().c_str(),
                errorResult.getErrorDescription().c_str());
        return nullptr;
    }
    isearch::common::Query *tokenizedQuery = queryTokenizer.stealQuery();
    if (tokenizedQuery) {
        SQL_LOG(DEBUG, "after tokenized query:%s", tokenizedQuery->toString().c_str());
    }
    return tokenizedQuery;
}

Query *UdfToQuery::cleanStopWords(isearch::common::Query *query) {
    if (query == nullptr) {
        return nullptr;
    }
    isearch::qrs::StopWordsCleaner stopWordsCleaner;
    SQL_LOG(DEBUG, "cleanStopWords query:%s", query->toString().c_str());
    bool ret = stopWordsCleaner.clean(query);
    if (!ret) {
        SQL_LOG(WARN,
                "clean query failed [%s]",
                isearch::haErrorCodeToString(stopWordsCleaner.getErrorCode()).c_str());
        return nullptr;
    }
    return stopWordsCleaner.stealQuery();
}

bool UdfToQuery::validateQuery(Query *query, const suez::turing::IndexInfos *indexInfos) {
    if (!query) {
        SQL_LOG(WARN, "validate query failed, error msg [Query is null]");
        return false;
    }
    isearch::qrs::QueryValidator queryValidator(indexInfos);
    query->accept(&queryValidator);
    auto errorCode = queryValidator.getErrorCode();
    if (errorCode != isearch::ERROR_NONE) {
        SQL_LOG(
            WARN, "validate query failed, error msg [%s]", queryValidator.getErrorMsg().c_str());
        return false;
    }
    return true;
}

} // namespace sql
