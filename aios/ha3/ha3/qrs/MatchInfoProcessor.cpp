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
#include "ha3/qrs/MatchInfoProcessor.h"

#include <stddef.h>
#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/analyzer/Analyzer.h"
#include "build_service/analyzer/Token.h"
#include "ha3/common/AttributeItem.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/Hits.h"
#include "ha3/common/OrQuery.h"
#include "ha3/common/PBResultFormatter.h"
#include "ha3/common/PKFilterClause.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryClause.h"
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/qrs/CheckMatchVisitor.h"
#include "ha3/qrs/IndexLimitQueryVisitor.h"
#include "ha3/qrs/ParsedQueryVisitor.h"
#include "ha3/qrs/QrsProcessor.h"
#include "ha3/qrs/TransAnd2OrVisitor.h"
#include "suez/turing/expression/util/FieldInfos.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "suez/turing/expression/util/SummaryInfo.h"
#include "suez/turing/expression/util/TableInfo.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace suez::turing;
using namespace build_service::analyzer;
using namespace indexlibv2::analyzer;

using namespace isearch::common;
using namespace isearch::config;

namespace isearch {
namespace qrs {
AUTIL_LOG_SETUP(ha3, MatchInfoProcessor);

QrsProcessorPtr MatchInfoProcessor::clone() {
    QrsProcessorPtr ptr(new MatchInfoProcessor(*this));
    return ptr;
}

bool MatchInfoProcessor::init(const KeyValueMap &keyValues,
           config::ResourceReader *resourceReader)
{
    if(!QrsProcessor::init(keyValues, resourceReader)){
        return false;
    }
    string queryIndexsStr =  getParam("query_indexs");
    const vector<string> &queryIndexVec = StringUtil::split(queryIndexsStr, ",", true);
    _queryIndexs.insert(queryIndexVec.begin(), queryIndexVec.end());

    string summaryFieldsStr = getParam("summary_fields");
    const vector<string> &summaryFieldVec = StringUtil::split(summaryFieldsStr,",",true);
    for(size_t i = 0; i < summaryFieldVec.size(); i++){
        const vector<string> &indexVec = StringUtil::split(summaryFieldVec[i],":",false);
        if(indexVec.size() == 2){
            _summaryFields[indexVec[0]] = indexVec[1];
        }
    }
    return true;
}

void MatchInfoProcessor::process(RequestPtr &requestPtr,
                                 ResultPtr &resultPtr)
{
    QueryClause *queryClause = requestPtr->getQueryClause();
    Query *originalQuery = queryClause->getRootQuery();
    Query* cloneOriQuery = originalQuery->clone();
    if(!rewriteQuery(requestPtr)){
        return;
    }
    QrsProcessor::process(requestPtr, resultPtr);
    traceQuery("flat query", queryClause->getRootQuery());
    // revert to orignal query for summary
    queryClause->setRootQuery(cloneOriQuery);
}

bool MatchInfoProcessor::rewriteQuery(RequestPtr &requestPtr){
    QueryClause *queryClause = requestPtr->getQueryClause();
    Query *originalQuery = queryClause->getRootQuery();
    ConfigClause *configClause = requestPtr->getConfigClause();
    string clusterName = configClause->getClusterName();
    TableInfoPtr tableInfoPtr = getTableInfo(clusterName);
    if (!tableInfoPtr) {
        return false;
    }
    string pkIndexName = tableInfoPtr->getPrimaryKeyIndexInfo()->getIndexName();
    traceQuery("raw query", originalQuery);
    TransAnd2OrVisitor transVisitor;
    originalQuery->accept(&transVisitor);
    Query *transQuery = transVisitor.stealQuery();
    traceQuery("trans query", transQuery);
    string primaryKey = configClause->getDebugQueryKey();
    if (!primaryKey.empty()) {
        PKFilterClause *pkClause = new PKFilterClause();
        pkClause->setOriginalString(primaryKey);
        requestPtr->setPKFilterClause(pkClause);
        OrQuery *orQuery = new OrQuery("");
        orQuery->addQuery(QueryPtr(transQuery));
        QueryPtr pkQuery(new TermQuery(primaryKey.c_str(),
                        pkIndexName.c_str(),RequiredFields(), ""));
        orQuery->addQuery(pkQuery);
        queryClause->setRootQuery(orQuery);
        //using default RANK sort clause
        requestPtr->setRankClause(NULL);
        requestPtr->setRankSortClause(NULL);
        requestPtr->setFilterClause(NULL);
    }else{
        queryClause->setRootQuery(transQuery);
    }
    return true;
}

void MatchInfoProcessor::fillSummary(const RequestPtr &requestPtr,
                                     const ResultPtr &resultPtr){
    QrsProcessor::fillSummary(requestPtr, resultPtr);
    analyzeMatchInfo(requestPtr, resultPtr);
}

void MatchInfoProcessor::analyzeMatchInfo(const RequestPtr &requestPtr,
        const ResultPtr &resultPtr)
{
    Hits *hits = resultPtr->getHits();
    if (hits == nullptr || hits->size() == 0){
        REQUEST_TRACE(INFO, "###check_result:{}");
        return;
    }
    const string &clusterName = requestPtr->getConfigClause()->getClusterName();
    TableInfoPtr tableInfoPtr = getTableInfo(clusterName);
    if (!tableInfoPtr) {
        return;
    }
    HitPtr hit = hits->getHit(0);
    Tracer *tracer = hit->getTracer();
    const string &traceInfoStr = tracer ? tracer->getTraceInfo() : "";
    CheckMatchVisitor checkVisitor(traceInfoStr);
    Query* originalQuery = requestPtr->getQueryClause()->getRootQuery();
    traceQuery("original query", originalQuery);
    originalQuery->accept(&checkVisitor);
    CheckResultInfo resInfo = checkVisitor.getCheckResult();

    map<string, string> summaryInfo;
    getSummaryInfo(tableInfoPtr, summaryInfo);
    map<string, string> summaryFieldMap = getSummaryFields(hit, summaryInfo);
    if (!resInfo.errorState) {
        const map<string, string> &phraseMap = checkVisitor.getPhraseQuerys();
        checkPhraseError(phraseMap, summaryFieldMap, resInfo);
    }
    CheckResult checkResult;
    if (resInfo.errorState) {
        CheckQueryResult& queryRes = checkResult._checkQueryResult;
        IndexLimitQueryVisitor queryVisitor(resInfo.indexStr, resInfo.wordStr);
        originalQuery->accept(&queryVisitor);
        queryRes._queryStr = queryVisitor.getQueryStr();
        queryRes._errorSubQueryStr = queryVisitor.getSubQueryStr();
        tokenSummary(summaryFieldMap, summaryInfo);
        queryRes._summaryMap = summaryFieldMap;
        queryRes._errorType = CheckResultInfo::getErrorType(resInfo.errorType);
        queryRes._errorWord = resInfo.indexStr + ":" + resInfo.wordStr;
        queryRes._isTokenizePart = findIndex(tableInfoPtr, resInfo.indexStr);
    }
    getAttributes(checkResult._checkFilterResult._filterValMap, hit);
    reportMatchResult(checkResult);
}

bool MatchInfoProcessor::findIndex(const TableInfoPtr& tableInfoPtr,
                                   const string &indexName)
{
    if (!_queryIndexs.empty()) {
        return _queryIndexs.count(indexName);
    }
    const IndexInfos *indexInfos = tableInfoPtr->getIndexInfos();
    for (auto iter = indexInfos->begin(); iter < indexInfos->end(); iter++) {
        if (indexName == (*iter)->getIndexName()) {
            return indexNeedTokenize((*iter)->getIndexType());
        }
    }
    return false;
}

void MatchInfoProcessor::reportMatchResult(const CheckResult& checkResult)
{
    const Any &any = ToJson(checkResult);
    const string &jsonString = ToString(any);
    REQUEST_TRACE(INFO, "###check_result:%s", jsonString.c_str());
}

void MatchInfoProcessor::getSummaryInfo(const TableInfoPtr& tableInfoPtr,
                                        map<string, string>& summaryInfoMap)
{
    if (!_summaryFields.empty()) {
        summaryInfoMap = _summaryFields;
        return;
    }
    const FieldInfos *fieldInfos = tableInfoPtr-> getFieldInfos();
    const SummaryInfo *summaryInfo = tableInfoPtr->getSummaryInfo();
    size_t fieldCount = summaryInfo->getFieldCount();
    for (size_t i = 0; i < fieldCount; i++){
        const string &fieldName = summaryInfo->getFieldName(i);
        const FieldInfo *fieldInfo = fieldInfos->getFieldInfo(fieldName.c_str());
        if (ft_text == fieldInfo->fieldType) {
            summaryInfoMap[fieldName] = fieldInfo->analyzerName;
        }
    }
}

map<string, string> MatchInfoProcessor::getSummaryFields(const HitPtr& hitPtr,
        const map<string, string>&summaryInfo)
{
    map<string, string> res;
    if (hitPtr == NULL) {
        return res;
    }
    string summaryStr;
    map<string, string>::const_iterator iter = summaryInfo.begin();
    for (; iter != summaryInfo.end(); iter++) {
        summaryStr = hitPtr->getSummaryValue(iter->first);
        removeSummaryTag(summaryStr);
        if(summaryStr != ""){
            res[iter->first] = summaryStr;
        }
    }
    return res;
}

void MatchInfoProcessor::tokenSummary(map<string, string>& summaryFieldMap,
                                      const map<string, string>& summaryInfo)
{
    for (auto iter = summaryFieldMap.begin(); iter != summaryFieldMap.end(); iter++) {
        string tokenStr;
        const string &analyzerName = getAnalyzerName(iter->first, summaryInfo);
        bool isTokenized = false;
        if (_analyzerFactoryPtr) {
            Analyzer *analyzer = _analyzerFactoryPtr->createAnalyzer(analyzerName);
            if(analyzer != nullptr) {
                analyzer->tokenize(iter->second.data(), iter->second.size());
                Token token;
                while (analyzer->next(token)) {
                    tokenStr.append(token.getText().c_str());
                    tokenStr.append(" ");
                }
                isTokenized = true;
                DELETE_AND_SET_NULL(analyzer);
            }
        }
        if (!isTokenized) {
            tokenStr = iter->second;
        }
        iter->second = tokenStr;
    }
}

string MatchInfoProcessor::getAnalyzerName(const string& fieldName,
        const map<string, string>&summaryInfo)
{
    map<string, string>::const_iterator iter = summaryInfo.find(fieldName);
    if (iter != summaryInfo.end()) {
        return iter->second;
    }
    return "";
}

void MatchInfoProcessor::replaceAll(string& str, const string& from, const string& to)
{
    if (from.empty()) {
        return;
    }
    size_t start_pos = 0;
    while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length();
    }
}

void MatchInfoProcessor::removeSummaryTag(std::string& titleStr){
    size_t posBeg = titleStr.find('<');
    size_t posEnd = titleStr.find('>', posBeg + 1);
    while (posBeg != string::npos && posEnd != string::npos){
        titleStr.erase(posBeg, posEnd - posBeg + 1);
        posBeg = titleStr.find('<', posBeg);
        if (posBeg != string::npos){
            posEnd = titleStr.find('>', posBeg + 1);
        }
    }
}

void MatchInfoProcessor::getAttributes(map<string, string>& attributeMap,
                                       const common::HitPtr& hitPtr){
    if (hitPtr == NULL) {
        return ;
    }
    AttributeMap& attriMap = hitPtr->getAttributeMap();
    AttributeMap::iterator iter = attriMap.begin();
    for (;iter != attriMap.end(); iter++){
        attributeMap[iter->first] = iter->second->toString();
    }
}

void MatchInfoProcessor::checkPhraseError(const map<string, string>& phraseMap,
        const map<string, string>& summaryMap, CheckResultInfo& resInfo)
{
    for (auto phraseIt = phraseMap.begin(); phraseIt != phraseMap.end(); phraseIt++) {
        bool isFind = false;
        for (auto iter = summaryMap.begin(); iter != summaryMap.end(); iter++) {
            if (iter->second.find(phraseIt->first) != string::npos) {
                isFind = true;
                break;
            }
        }
        if (!isFind) {
            resInfo.errorState = true;
            resInfo.errorType = MCE_PHRASE_QUERY;
            resInfo.wordStr = "\"" + phraseIt->first + "\"";
            resInfo.indexStr = phraseIt->second;
            break;
        }
    }
}

TableInfoPtr MatchInfoProcessor::getTableInfo(const string& clusterName) const {
    ClusterTableInfoMap::const_iterator iter = _clusterTableInfoMapPtr->find(clusterName);
    if(iter != _clusterTableInfoMapPtr->end()){
        return iter->second;
    }
    return TableInfoPtr();
}

void MatchInfoProcessor::traceQuery(const string& prefix, const Query *query){
    if (query != nullptr) {
        ParsedQueryVisitor visitor;
        query->accept(&visitor);
        const string &parsedQueryStr = visitor.getParsedQueryStr();
        REQUEST_TRACE(DEBUG, " ||| %s: ### %s", prefix.c_str(), parsedQueryStr.c_str());
    }
}

void CheckQueryResult::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json){
    json.Jsonize("summary", _summaryMap);
    json.Jsonize("query", _queryStr);
    json.Jsonize("not_matched_sub_query", _errorSubQueryStr);
    json.Jsonize("is_tokenize_part", _isTokenizePart);
    json.Jsonize("type", _errorType);
    json.Jsonize("term", _errorWord);
}

void CheckFilterResult::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json){
    json.Jsonize("filter", _filterValMap);
}

void CheckResult::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json){
    json.Jsonize("query_result", _checkQueryResult);
    json.Jsonize("filter_result", _checkFilterResult);
}

} // namespace qrs
} // namespace isearch
