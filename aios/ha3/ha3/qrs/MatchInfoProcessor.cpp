#include <ha3/qrs/MatchInfoProcessor.h>
#include <sstream>
#include <string>
#include <assert.h>
#include <autil/StringUtil.h>
#include <algorithm>
#include <ha3/common/QueryTermVisitor.h>
#include <build_service/analyzer/Token.h>
#include <ha3/qrs/ParsedQueryVisitor.h>
#include <ha3/qrs/TransAnd2OrVisitor.h>
#include <ha3/qrs/IndexLimitQueryVisitor.h>
#include <build_service/analyzer/Analyzer.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace suez::turing;
using namespace build_service::analyzer;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, MatchInfoProcessor);

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
        const ResultPtr &resultPtr){
    Hits *hits = resultPtr->getHits();
    if(hits == NULL || hits->size() == 0){
        REQUEST_TRACE(INFO, "###check_result:{}");
        return;
    }
    map<string, bool> termMatchMap;
    HitPtr hit = hits->getHit(0);
    parseMatchInfo(hit, termMatchMap);
    string clusterName = requestPtr->getConfigClause()->getClusterName();
    TableInfoPtr tableInfoPtr = getTableInfo(clusterName);
    if(!tableInfoPtr) {
        return;
    }
    CheckMatchVisitor checkVisitor(termMatchMap);
    Query* originalQuery = requestPtr->getQueryClause()->getRootQuery();
    traceQuery("original query", originalQuery);
    originalQuery->accept(&checkVisitor);
    CheckResultInfo resInfo = checkVisitor.getCheckResult();
    
    map<string, string> summaryInfo;
    getSummaryInfo(tableInfoPtr, summaryInfo);
    map<string, string> summaryFieldMap = getSummaryFields(hit, summaryInfo);
    if(!resInfo.errorState){
        map<string, string> phraseMap = checkVisitor.getPhraseQuerys();
        checkPhraseError(phraseMap, summaryFieldMap, resInfo);
    }
    CheckResult checkResult;
    if(resInfo.errorState){
        CheckQueryResult& queryRes = checkResult._checkQueryResult;
        set<string> queryIndexs;
        getQueryIndexs(tableInfoPtr, queryIndexs);
        IndexLimitQueryVisitor queryVisitor(resInfo.indexStr, resInfo.wordStr);
        originalQuery->accept(&queryVisitor);

        queryRes._queryStr = queryVisitor.getQueryStr();
        queryRes._errorSubQueryStr = queryVisitor.getSubQueryStr();
        tokenSummary(summaryFieldMap, summaryInfo);
        queryRes._summaryMap = summaryFieldMap;
        queryRes._errorType = CheckResultInfo::getErrorType(resInfo.errorType); 
        queryRes._errorWord = resInfo.indexStr +":"+resInfo.wordStr;
        queryRes._isTokenizePart = queryIndexs.count(resInfo.indexStr);
    }
    getAttributes(checkResult._checkFilterResult._filterValMap, hit);
    reportMatchResult(checkResult);
}

void MatchInfoProcessor::getQueryIndexs(const TableInfoPtr& tableInfoPtr, 
                                        set<string>& queryIndexs)
{
    if(_queryIndexs.size() > 0){
        queryIndexs = _queryIndexs;
        return;
    }
    const IndexInfos *indexInfos = tableInfoPtr->getIndexInfos();
    IndexInfos::ConstIterator iter = indexInfos->begin();
    IndexType indexType;
    for(; iter < indexInfos->end(); iter++){
        indexType = (*iter)->getIndexType();
        if(it_pack == indexType ||
           it_expack == indexType ||
           it_text == indexType)
        {
            queryIndexs.insert((*iter)->getIndexName());
        }
    }
}

void MatchInfoProcessor::reportMatchResult(const CheckResult& checkResult)
{
    Any any = ToJson(checkResult);
    string jsonString = ToString(any);
    REQUEST_TRACE(INFO, "###check_result:%s",jsonString.c_str());
}

void MatchInfoProcessor::getSummaryInfo(const TableInfoPtr& tableInfoPtr, 
                                        map<string, string>& summaryInfoMap)
{
    if(_summaryFields.size() > 0){
        summaryInfoMap = _summaryFields;
        return;
    }
    const FieldInfos *fieldInfos = tableInfoPtr-> getFieldInfos();
    const SummaryInfo *summaryInfo = tableInfoPtr->getSummaryInfo();
    size_t fieldCount = summaryInfo->getFieldCount();
    string fieldName;
    for(size_t i =0; i < fieldCount; i++){
        fieldName = summaryInfo->getFieldName(i);
        const FieldInfo *fieldInfo = fieldInfos->getFieldInfo(fieldName.c_str());
        if(ft_text == fieldInfo->fieldType){
            summaryInfoMap[fieldName] = fieldInfo->analyzerName;
        }
    }
}

map<string, string> MatchInfoProcessor::getSummaryFields(const HitPtr& hitPtr, 
        const map<string, string>&summaryInfo)
{
    map<string, string> res;
    if(hitPtr == NULL){
        return res;
    }
    string summaryStr;
    map<string, string>::const_iterator iter = summaryInfo.begin();
    for(; iter != summaryInfo.end(); iter++){
        summaryStr = hitPtr->getSummaryValue(iter->first);
        removeSummaryTag(summaryStr);
        if(summaryStr != ""){
            res[iter->first] = summaryStr;
        }
    }
    return res;
}

void MatchInfoProcessor::tokenSummary(map<string, string>& summaryFieldMap, 
        const map<string, string>& summaryInfo){
    map<string, string>::iterator iter = summaryFieldMap.begin();
    Token token;
    Analyzer* analyzer = NULL; 
    string analyzerName,tokenStr;
    for(; iter != summaryFieldMap.end(); iter++){
        tokenStr.clear();
        analyzerName = getAnalyzerName(iter->first, summaryInfo);
        bool isTokenized = false;
        if(_analyzerFactoryPtr){
            analyzer = _analyzerFactoryPtr->createAnalyzer(analyzerName);
            if(analyzer != NULL) {
                analyzer->tokenize(iter->second.data(), iter->second.size());
                while (analyzer->next(token)) {
                    tokenStr.append(token.getText().c_str());
                    tokenStr.append(" ");
                }
                isTokenized = true;
                DELETE_AND_SET_NULL(analyzer);
            }
        }
        if(!isTokenized) {
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
    if(from.empty())
        return;
    size_t start_pos = 0;
    while((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length();
    }
}

void MatchInfoProcessor::removeSummaryTag(std::string& titleStr){
    size_t posBeg, posEnd;
    posBeg = titleStr.find('<');
    posEnd = titleStr.find('>', posBeg + 1);
    while(posBeg != string::npos && posEnd != string::npos){
        titleStr.erase(posBeg, posEnd - posBeg + 1);
        posBeg = titleStr.find('<', posBeg);
        if(posBeg != string::npos){
            posEnd = titleStr.find('>', posBeg + 1);
        }
    }
}

void MatchInfoProcessor::getAttributes(map<string, string>& attributeMap,
                                       const common::HitPtr& hitPtr){
    if(hitPtr == NULL){
        return ;
    }
    AttributeMap& attriMap = hitPtr->getAttributeMap();
    AttributeMap::iterator iter = attriMap.begin();
    for(;iter != attriMap.end(); iter++){
        attributeMap[iter->first] = iter->second->toString();
    }
}

void MatchInfoProcessor::parseMatchInfo(const HitPtr& hitPtr, map<string, bool>& matchTermMap){
    Tracer *tracer = hitPtr->getTracer();
    if (!tracer) {
        return;
    }
    string traceInfoStr = tracer->getTraceInfo();
    static const string matchSepStr = "###||###";
    static const string sepStr = "|||";
    static const char sep = ':';
    size_t posBeg = traceInfoStr.find(matchSepStr);
    size_t posEnd = traceInfoStr.rfind(matchSepStr);
    string allMatchStr;
    if(posBeg != string::npos && posEnd !=string::npos){
        allMatchStr = traceInfoStr.substr(posBeg + matchSepStr.size(),
                posEnd - posBeg - matchSepStr.size());
    }
    const vector<string> &matchInfoVec = StringUtil::split(allMatchStr, sepStr, true);
    string termStr, matchStr;
    for(size_t i = 0; i < matchInfoVec.size(); i++){
        const string &str = matchInfoVec[i];
        size_t pos = str.rfind(sep);
        if(pos != string::npos){
            termStr = str.substr(0, pos);
            matchStr = str.substr(pos + 1);
            if(matchStr.size() > 0 && matchStr[0] == '1'){
                matchTermMap[termStr] = true;
            }else{
                matchTermMap[termStr] = false;
            }
        }
    }
}

void MatchInfoProcessor::checkPhraseError(const map<string, string>& phraseMap, 
        const map<string, string>& summaryMap, CheckResultInfo& resInfo){
    if(resInfo.errorState){
        return;
     }
    map<string, string>::const_iterator phrase_iter = phraseMap.begin();
    map<string, string>::const_iterator iter;
    for(; phrase_iter != phraseMap.end(); phrase_iter++){
        bool isFind = false;
        for(iter = summaryMap.begin(); iter != summaryMap.end(); iter++){
            if(iter->second.find(phrase_iter->first) != string::npos){
                isFind = true;
                break;
            }
        }
        if(!isFind){
            resInfo.errorState = true;
            resInfo.errorType = MCE_PHRASE_QUERY;
            resInfo.wordStr = "\""+phrase_iter->first+"\"";
            resInfo.indexStr = phrase_iter->second;
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
    string parsedQueryStr;
    if(query != NULL){
        ParsedQueryVisitor visitor;
        query->accept(&visitor);
        parsedQueryStr = visitor.getParsedQueryStr();
    }
    REQUEST_TRACE(DEBUG, " ||| %s: ### %s", prefix.c_str(), parsedQueryStr.c_str());
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

END_HA3_NAMESPACE(qrs);

