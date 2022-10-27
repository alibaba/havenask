#include <ha3/queryparser/RequestParser.h>
#include <ha3/queryparser/QueryParser.h>
#include <ha3/queryparser/ParserContext.h>
#include <autil/StringTokenizer.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>
#include <ha3/queryparser/SyntaxExprParser.h>
#include <suez/turing/expression/syntax/RankSyntaxExpr.h>
#include <autil/HashFuncFactory.h>
#include <autil/StringUtil.h>
#include <assert.h>
#include <algorithm>
#include <ha3/queryparser/ClauseParserContext.h>
#include <ha3/queryparser/RequestSymbolDefine.h>
#include <ha3/common/PBResultFormatter.h>

using namespace std;
using namespace autil;
using namespace suez::turing;

USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, RequestParser);

RequestParser::RequestParser() {
}

RequestParser::~RequestParser() {
}

bool RequestParser::parseRequest(RequestPtr &requestPtr,
                                 const ClusterConfigMapPtr &clusterConfigMapPtr)
{
    if (!validateOriginalRequest(requestPtr)) {
        return false;
    }

    ConfigClause* configClause = requestPtr->getConfigClause();
    if (parseConfigClause(requestPtr) == false) {
        HA3_LOG(WARN, "parse config clause failed. request string[%s]",
            configClause->getOriginalString().c_str());
        return false;
    }
    HA3_LOG(DEBUG, "parseConfigClause success!");

    if (requestPtr->getClusterClause()) {
        if (parseClusterClause(requestPtr, clusterConfigMapPtr) == false) {
            return false;
        }

        ClusterClause *clusterClause = requestPtr->getClusterClause();

        assert(clusterClause);
        const vector<string> &clusterNameVec = clusterClause->getClusterNameVector();
        vector<string>::const_iterator it = clusterNameVec.begin();
        configClause->clearClusterName();
        for(; it != clusterNameVec.end(); ++it) {
            configClause->addClusterName(*it);
        }
    }

    string clusterName = configClause->getClusterName();
    ClusterConfigMap::const_iterator it = clusterConfigMapPtr->find(clusterName);
    if (clusterName == "" || it == clusterConfigMapPtr->end()) {
        _errorResult.resetError(ERROR_CLUSTER_NAME_NOT_EXIST,
                string("ClusterName:") + clusterName);
        return false;
    }

    if (requestPtr->getQueryClause()) {
        const QueryInfo &queryInfo = it->second.getQueryInfo();
        if (parseQueryClause(requestPtr, queryInfo) == false) {
                return false;
        }
    }

    FilterClause* filterClause = requestPtr->getFilterClause();
    if (filterClause) {
        if (parseFilterClause(filterClause) == false) {
            return false;
        }
    }

    if (requestPtr->getSortClause()) {
        if (parseSortClause(requestPtr) == false) {
            return false;
        }
    }

    if (requestPtr->getDistinctClause()) {
        if (parseDistinctClause(requestPtr) == false) {
            return false;
        }
    }

    if (requestPtr->getAggregateClause()) {
        if (parseAggregateClause(requestPtr) == false) {
            return false;
        }
    }

    if (requestPtr->getRankClause()) {
        if (parseRankClause(requestPtr) == false) {
            return false;
        }
    }

    if (requestPtr->getHealthCheckClause()) {
        if (parseHealthCheckClause(requestPtr) == false) {
            return false;
        }
    }

    if (requestPtr->getAttributeClause()) {
        if (parseAttributeClause(requestPtr) == false) {
            return false;
        }
    }

    if (requestPtr->getVirtualAttributeClause()) {
        if (parseVirtualAttributeClause(requestPtr) == false) {
            return false;
        }
    }

    if (requestPtr->getFetchSummaryClause()) {
        if (parseFetchSummaryClause(requestPtr) == false) {
            return false;
        }
    }

    if (requestPtr->getKVPairClause()) {
        if (parseKVPairClause(requestPtr) == false) {
            return false;
        }
    }

    if (requestPtr->getQueryLayerClause()) {
        if (parseQueryLayerClause(requestPtr) == false) {
            return false;
        }
    }

    if (requestPtr->getSearcherCacheClause()) {
        if (parseSearcherCacheClause(requestPtr) == false) {
            return false;
        }
    }

    if (requestPtr->getOptimizerClause()) {
        if (parseOptimizerClause(requestPtr) == false) {
            return false;
        }
    }

    if (requestPtr->getRankSortClause()) {
        if (parseRankSortClause(requestPtr) == false) {
            return false;
        }
    }

    if (requestPtr->getFinalSortClause()) {
        if (parseFinalSortClause(requestPtr) == false) {
            return false;
        }
    }

    if (requestPtr->getLevelClause()) {
        if (parseLevelClause(requestPtr) == false) {
            return false;
        }
    }

    if (requestPtr->getAnalyzerClause()) {
        if (!parseAnalyzerClause(requestPtr)) {
            return false;
        }
    }

    if (requestPtr->getAuxQueryClause()) {
        if (parseAuxQueryClause(requestPtr) == false) {
            return false;
        }
    }
    
    AuxFilterClause* auxFilterClause = requestPtr->getAuxFilterClause();
    if (auxFilterClause) {
        if (parseAuxFilterClause(auxFilterClause) == false) {
            return false;
        }
    }

    return true;
}

bool RequestParser::parseConfigClause(RequestPtr &requestPtr) {
    ConfigClause *configClause = requestPtr->getConfigClause();
    if (!configClause) {
        HA3_LOG(WARN, "failed to parseConfigClause: configClause is null");
        _errorResult.resetError(ERROR_NO_CONFIG_CLAUSE, "");
        return false;
    }
    const string &configClauseStr = configClause->getOriginalString();
    if (configClauseStr.empty()) {
        HA3_LOG(WARN, "Empty Config Clause Original String");
        return true;
    }

    configClause->clear();
    StringTokenizer st(configClauseStr, CONFIG_KV_PAIR_SEPERATOR,
                       StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        StringTokenizer st2(*it, CONFIG_KV_SEPERATOR,
                            StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
        HA3_LOG(DEBUG, "Token num[%zd]", st2.getNumTokens());
        if (st2.getNumTokens() != 2) {
            HA3_LOG(WARN, "Failed to parse ConfigClause:[%s]", configClauseStr.c_str());
            _errorResult.resetError(ERROR_KEY_VALUE_PAIR,
                    string("Error KV Pair in configClause: ") + *it);
            return false;
        }

        const string &key = st2[0];
        const string &value = st2[1];
        if (key == CONFIG_CLAUSE_KEY_CLUSTER) {
            if (!parseConfigClauseClusterNameVector(configClause, value)) {
                return false;
            }
        } else if (key == CONFIG_CLAUSE_KEY_START) {
            uint32_t start = 0;
            bool ret = StringUtil::strToUInt32(value.c_str(), start);
            if (!ret) {
                HA3_LOG(WARN, "Failed to parse ConfigClause.start:%s", value.c_str());
                _errorResult.resetError(ERROR_CONFIG_CLAUSE_START_VALUE);
                return false;
            }
            configClause->setStartOffset(start);
        } else if (key == CONFIG_CLAUSE_KEY_HIT) {
            uint32_t hitCount = 0;
            bool ret = StringUtil::strToUInt32(value.c_str(), hitCount);
            if (!ret) {
                HA3_LOG(WARN, "Failed to parse ConfigClause.hits:%s", value.c_str());
                _errorResult.resetError(ERROR_CONFIG_CLAUSE_HIT_VALUE);
                return false;
            }
            configClause->setHitCount(hitCount);
        } else if (key == CONFIG_CLAUSE_KEY_SEARCHER_RETURN_HITS) {
            uint32_t searcherReturnHits = 0;
            bool ret = StringUtil::strToUInt32(value.c_str(), searcherReturnHits);
            if (!ret) {
                HA3_LOG(WARN, "Failed to parse ConfigClause.searcher_return_hits:%s",
                        value.c_str());
                _errorResult.resetError(ERROR_CONFIG_CLAUSE_HIT_VALUE);
                return false;
            }
            configClause->setSearcherReturnHits(searcherReturnHits);
        } else if (key == CONFIG_CLAUSE_KEY_FORMAT) {
            configClause->setResultFormatSetting(value);
        } else if (key == CONFIG_CLAUSE_KEY_TRACE) {
            configClause->setTrace(value);
        } else if (key == CONFIG_CLAUSE_KEY_RANKTRACE) {
            configClause->setRankTrace(value);
        } else if (key == CONFIG_CLAUSE_KEY_METRICS_SRC) {
            configClause->setMetricsSrc(value);
        } else if (key == CONFIG_CLAUSE_KEY_QRS_CHAIN) {
            configClause->setQrsChainName(value);
        } else if (key == CONFIG_CLAUSE_KEY_SUMMARY_PROFILE) {
            configClause->setSummaryProfileName(value);
        } else if (key == CONFIG_CLAUSE_KEY_DEFAULT_OPERATOR) {
            configClause->setDefaultOP(value);
        } else if (key == CONFIG_CLAUSE_KEY_DEFAULT_INDEX) {
            configClause->setDefaultIndexName(value);
        } else if (key == CONFIG_CLAUSE_KEY_USE_CACHE) {
            configClause->setUseCacheConfig(value);
        } else if (key == CONFIG_CLAUSE_KEY_ALLOW_LACK_SUMMARY) {
            configClause->setAllowLackSummaryConfig(value);
        } else if (key == CONFIG_CLAUSE_KEY_RESULT_COMPRESS) {
            configClause->setCompressType(value);
        } else if (key == CONFIG_CLAUSE_KEY_INNER_RESULT_COMPRESS) {
            configClause->setInnerResultCompressType(value);
        } else if (key == CONFIG_CLAUSE_KEY_NO_SUMMARY) {
            configClause->setNoSummary(value);
        } else if (key == CONFIG_CLAUSE_KEY_DEDUP) {
            configClause->setDoDedup(value);
        } else if (key == CONFIG_CLAUSE_KEY_FETCH_SUMMARY_CLUSTER){
            configClause->setFetchSummaryClusterName(value);
        } else if (key == CONFIG_CLAUSE_KEY_KVPAIRS) {
            bool succ = parseConfigClauseKVPairs(configClause, value);
            if (!succ) {
                return false;
            }
        } else if (key == CONFIG_CLAUSE_KEY_TIMEOUT) {
            uint32_t timeOut = 0;
            bool ret = StringUtil::strToUInt32(value.c_str(), timeOut);
            if (!ret) {
                string errorMsg = "Failed to parse ConfigClause.timeout:" + value;
                HA3_LOG(WARN, "%s", errorMsg.c_str());
                _errorResult.resetError(ERROR_CONFIG_CLAUSE_TIMEOUT, errorMsg);
                return false;
            }
            configClause->setRpcTimeOut(timeOut);
        } else if (key == CONFIG_CLAUSE_KEY_SEEK_TIMEOUT) {
            uint32_t timeOut = 0;
            bool ret = StringUtil::strToUInt32(value.c_str(), timeOut);
            if (!ret) {
                string errorMsg = "Failed to parse ConfigClause.seek_timeout:" + value;
                HA3_LOG(WARN, "%s", errorMsg.c_str());
                _errorResult.resetError(ERROR_CONFIG_CLAUSE_TIMEOUT, errorMsg);
                return false;
            }
            configClause->setSeekTimeOut(timeOut);
        } else if (key == CONFIG_CLAUSE_KEY_RANKSIZE) {
            uint32_t rankSize = 0;
            bool ret = StringUtil::strToUInt32(value.c_str(), rankSize);
            if (!ret) {
                HA3_LOG(WARN, "Failed to parse ConfigClause.rankSize:%s", value.c_str());
                _errorResult.resetError(ERROR_CONFIG_CLAUSE_RANKSIZE);
                return false;
            }
            configClause->setRankSize(rankSize);
        } else if (key == CONFIG_CLAUSE_KEY_TOTALRANKSIZE) {
            uint32_t totalRankSize = 0;
            bool ret = StringUtil::strToUInt32(value.c_str(), totalRankSize);
            if (!ret) {
                HA3_LOG(WARN, "Failed to parse ConfigClause.totalRankSize:%s", value.c_str());
                _errorResult.resetError(ERROR_CONFIG_CLAUSE_TOTALRANKSIZE);
                return false;
            }
            configClause->setTotalRankSize(totalRankSize);
        } else if (key == CONFIG_CLAUSE_KEY_RERANKSIZE) {
            uint32_t rerankSize = 0;
            bool ret = StringUtil::strToUInt32(value.c_str(), rerankSize);
            if (!ret) {
                HA3_LOG(WARN, "Failed to parse ConfigClause.rerankSize:%s", value.c_str());
                _errorResult.resetError(ERROR_CONFIG_CLAUSE_RERANKSIZE);
                return false;
            }
            configClause->setRerankSize(rerankSize);
        } else if (key == CONFIG_CLAUSE_KEY_TOTALRERANKSIZE) {
            uint32_t totalRerankSize = 0;
            bool ret = StringUtil::strToUInt32(value.c_str(), totalRerankSize);
            if (!ret) {
                HA3_LOG(WARN, "Failed to parse ConfigClause.totalRerankSize:%s", value.c_str());
                _errorResult.resetError(ERROR_CONFIG_CLAUSE_TOTALRERANKSIZE);
                return false;
            }
            configClause->setTotalRerankSize(totalRerankSize);
        } else if (key == CONFIG_CLAUSE_KEY_ANALYZER) {
            configClause->setAnalyzerName(value);
        } else if (key == CONFIG_CLAUSE_KEY_CACHE_INFO) {
            configClause->setDisplayCacheInfo(value);
        } else if (key == CONFIG_CLAUSE_RERANK_HINT) {
            configClause->setNeedRerank(value);
        } else if (key == CONFIG_CLAUSE_KEY_SOURCEID) {
            configClause->setSourceId(value);
        } else if (key == CONFIG_CLAUSE_PHASE_ONE_TASK_QUEUE) {
            configClause->setPhaseOneTaskQueueName(value);
        } else if (key == CONFIG_CLAUSE_PHASE_TWO_TASK_QUEUE) {
            configClause->setPhaseTwoTaskQueueName(value);
        } else if (key == CONFIG_CLAUSE_KEY_BATCH_SCORE) {
            configClause->setBatchScore(value);
        } else if (key == CONFIG_CLAUSE_KEY_OPTIMIZE_RANK) {
            configClause->setOptimizeRank(value);
        } else if (key == CONFIG_CLAUSE_KEY_OPTIMIZE_COMPARATOR) {
            configClause->setOptimizeComparator(value);
        } else if (key == CONFIG_CLAUSE_KEY_IGNORE_DELETE) {
            configClause->setIgnoreDelete(value);
        } else if (key == CONFIG_CLAUSE_KEY_DEBUG_QUERY_KEY){
            configClause->setDebugQueryKey(value);
        } else if (key == CONFIG_CLAUSE_REQUEST_TRACE_TYPE){
            configClause->setTraceType(value);
        } else if (key == CONFIG_CLAUSE_KEY_JOIN_TYPE) {
            if (value == CONFIG_CLAUSE_KEY_JOIN_WEAK_TYPE) {
                configClause->setJoinType(WEAK_JOIN);
            } else if (value == CONFIG_CLAUSE_KEY_JOIN_STRONG_TYPE) {
                configClause->setJoinType(STRONG_JOIN);
            } else {
                HA3_LOG(WARN, "Failed to parse ConfigClause. "
                        "unknown join type:%s", value.c_str());
                _errorResult.resetError(ERROR_CONFIG_CLAUSE_JOIN_TYPE);
                return false;
            }
        } else if (key == CONFIG_CLAUSE_KEY_FETCH_SUMMARY_TYPE) {
            if (value == CONFIG_CLAUSE_KEY_FETCH_SUMMARY_BY_PK) {
                configClause->setFetchSummaryType(BY_PK);
            } else if (value == CONFIG_CLAUSE_KEY_FETCH_SUMMARY_BY_DOCID) {
                configClause->setFetchSummaryType(BY_DOCID);
            } else if (value == CONFIG_CLAUSE_KEY_FETCH_SUMMARY_BY_RAWPK) {
                configClause->setFetchSummaryType(BY_RAWPK);
            } else {
                HA3_LOG(WARN, "Failed to parse ConfigClause. "
                        "unknown fetch summary type:%s", value.c_str());
                _errorResult.resetError(ERROR_CONFIG_CLAUSE_FETCH_SUMMARY_TYPE);
                return false;
            }
        } else if (key == CONFIG_CLAUSE_KEY_FETCH_SUMMARY_GROUP) {
            // split in search
            if (!value.empty()) {
                configClause->setFetchSummaryGroup(value);
            } else {
                HA3_LOG(WARN, "Failed to parse ConfigClause. "
                        "fetch summary group empty string");
                _errorResult.resetError(ERROR_CONFIG_CLAUSE_FETCH_SUMMARY_GROUP);
                return false;
            }
        } else if (key == CONFIG_CLAUSE_KEY_PROTO_FORMAT_OPTION) {
            if (!parseConfigClauseProtoFormatOption(configClause, value)) {
                HA3_LOG(WARN, "Failed to parse ConfigClause. "
                        "unknown protobuf format option:%s", value.c_str());
                _errorResult.resetError(ERROR_CONFIG_CLAUSE_PROTO_FORMAT_OPTION);
                return false;
            }
        } else if (key == CONFIG_CLAUSE_KEY_ACTUAL_HITS_LIMIT) {
            uint32_t actualHitsLimit = 0;
            bool ret = StringUtil::strToUInt32(value.c_str(), actualHitsLimit);
            if (!ret) {
                HA3_LOG(WARN, "Failed to parse ConfigClause.actualHitsLimit:%s", value.c_str());
                _errorResult.resetError(ERROR_CONFIG_CLAUSE_HIT_VALUE);
                return false;
            }
            configClause->setActualHitsLimit(actualHitsLimit);
        } else if (key == CONFIG_CLAUSE_NO_TOKENIZE_INDEXES) {
            StringTokenizer valueSt(value, NO_TOKENIZE_INDEXES_SEPERATOR,
                    StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
            for (StringTokenizer::Iterator vIt = valueSt.begin();
                 vIt != valueSt.end(); vIt++)
            {
                configClause->addNoTokenizeIndex(*vIt);
            }
        } else if (key == CONFIG_CLAUSE_RESEARCH_THRESHOLD) {
            uint32_t threshold = 0;
            bool ret = StringUtil::strToUInt32(value.c_str(), threshold);
            if (!ret) {
                HA3_LOG(WARN, "Failed to parse ConfigClause.researchThreshold:%s", value.c_str());
                _errorResult.setErrorCode(ERROR_CONFIG_CLAUSE_RESEARCH_THRESHOLD);
                return false;
            }
            configClause->setResearchThreshold(threshold);
        } else if (key == CONFIG_CLAUSE_SUB_DOC_DISPLAY_TYPE) {
            if (value == CONFIG_CLAUSE_KEY_SUB_DOC_DISPLAY_NO_TYPE) {
                configClause->setSubDocDisplayType(SUB_DOC_DISPLAY_NO);
            } else if (value == CONFIG_CLAUSE_KEY_SUB_DOC_DISPLAY_GROUP_TYPE) {
                configClause->setSubDocDisplayType(SUB_DOC_DISPLAY_GROUP);
            } else if (value == CONFIG_CLAUSE_KEY_SUB_DOC_DISPLAY_FLAT_TYPE) {
                configClause->setSubDocDisplayType(SUB_DOC_DISPLAY_FLAT);
            } else {
                HA3_LOG(WARN, "Failed to parse ConfigClause. "
                        "unknown subDoc display type:%s", value.c_str());
                _errorResult.resetError(ERROR_CONFIG_CLAUSE_SUB_DOC_DISPLAY_TYPE);
                return false;
            }
        } else if (key == CONFIG_CLAUSE_KEY_VERSION) {
            int64_t version = -1;
            bool ret = StringUtil::strToInt64(value.c_str(), version);
            if (!ret) {
                HA3_LOG(WARN, "Failed to parse ConfigClause.version:%s", value.c_str());
                _errorResult.resetError(ERROR_CONFIG_CLAUSE_VERSION);
                return false;
            } else {
                configClause->setVersion(version);
            }
        } else if (key == CONFIG_CLAUSE_KEY_HIT_SUMMARY_SCHEMA_CACHE_KEY) {
            if (!value.empty()) {
                configClause->setHitSummarySchemaCacheKey(value);
            } else {
                HA3_LOG(WARN, "Failed to parse ConfigClause. "
                        "hit summary schema cache key empty string");
                _errorResult.resetError(ERROR_CONFIG_CLAUSE_HIT_SUMMARY_SCHEMA_CACHE_KEY);
                return false;
            }
        } else if (key == CONFIG_CLAUSE_KEY_GET_ALL_SUB_DOC) {
          configClause->setGetAllSubDoc(value);
        }
    }

    if (!configClause->getDebugQueryKey().empty()){ //debug query need trace info
        string traceLevel = configClause->getTrace();
        if (traceLevel.empty() || traceLevel == "FATAL" || traceLevel == "ERROR"
            || traceLevel == "WARN")
        {
            configClause->setTrace("INFO");
        }
        string rankTraceLevel = configClause->getRankTrace();
        if (rankTraceLevel.empty() || rankTraceLevel == "FATAL" || rankTraceLevel == "ERROR"
            || rankTraceLevel == "WARN")
        {
            configClause->setRankTrace("INFO");
        }
        if (configClause->isNoSummary()) {
            configClause->setNoSummary("no");
        }
        if (configClause->getStartOffset() > 0) {
            configClause->setStartOffset(0);
        }
        if (configClause->useCache()) {
            configClause->setUseCacheConfig("no");
        }
    }
    return true;
}

bool RequestParser::parseConfigClauseClusterNameVector(ConfigClause* configClause,
        const string& clusterNameVectorStr)
{
    StringTokenizer st(clusterNameVectorStr, CLAUSE_MULTIVALUE_SEPERATOR,
                       StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    if (st.getNumTokens() == 0) {
        string errorMsg = "Error cluster name. oriString[" + clusterNameVectorStr + "]";
        HA3_LOG(WARN, "%s", errorMsg.c_str());
        _errorResult.resetError(ERROR_CLUSTER_NAME_NOT_EXIST, errorMsg);
        return false;
    }
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        configClause->addClusterName(*it);
    }
    return true;
}

bool RequestParser::parseConfigClauseKVPairs(ConfigClause* configClause,
        const string& configClauseKVPairsStr)
{
    StringTokenizer st(configClauseKVPairsStr, CONFIG_EXTRA_KV_PAIR_SEPERATOR,
                       StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        StringTokenizer st2(*it, CONFIG_EXTRA_KV_SEPERATOR,
                            StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
        if (st2.getNumTokens() != 2) {
            _errorResult.resetError(ERROR_KEY_VALUE_PAIR,
                    string(" Failed to extract Key-Value pairs [" + configClauseKVPairsStr+ "]"));
            return false;
        }
        configClause->addKVPair(st2[0], st2[1]);
    }
    return true;
}

bool RequestParser::parseQueryClause(RequestPtr &requestPtr,
        const QueryInfo &queryInfo)
{
    ConfigClause *configClause = requestPtr->getConfigClause();
    if (!configClause) {
        HA3_LOG(WARN, "failed to parseQueryClause: configClause is null");
        return false;
    }
    QueryClause* queryClause = requestPtr->getQueryClause();
    if (!queryClause) {
        HA3_LOG(WARN, "failed to parseQueryClause: queryClause is null");
        return false;
    }
    const string &queryClauseStr = queryClause->getOriginalString();

    QueryOperator defaultOP = configClause->getDefaultOP();
    string defaultIndexName = configClause->getDefaultIndexName();
    if (defaultIndexName.empty()) {
        defaultIndexName = queryInfo.getDefaultIndexName();
    }
    if (OP_UNKNOWN == defaultOP) {
        defaultOP = queryInfo.getDefaultOperator();
    }

    QueryParser queryParser(defaultIndexName.c_str(), defaultOP,
                            queryInfo.getDefaultMultiTermOptimizeFlag());

    HA3_LOG(TRACE2, "QueryText: [%s]", queryClauseStr.c_str());

    ParserContext *context = queryParser.parse(queryClauseStr.c_str());

    if(ParserContext::OK != context->getStatus()){
        _errorResult.resetError(ERROR_QUERY_CLAUSE,
                           string("Parse query failed! error description: ") +
                           context->getStatusMsg() + ", queryClauseStr:" + queryClauseStr);
        delete context;
        return false;
    }

    vector<Query*> querys = context->stealQuerys();
    for (uint32_t i = 0; i < querys.size(); ++i) {
        queryClause->setRootQuery(querys[i], i);
    }

    delete context;
    return true;
}

bool RequestParser::parseFilterClause(FilterClause *filterClause) {
    if (!filterClause) {
        HA3_LOG(WARN, "failed to parseFilterClause: filterClause is null");
        return false;
    }
    const string &filterClauseStr = filterClause->getOriginalString();
    ClauseParserContext ctx;
    if (!ctx.parseSyntaxExpr(filterClauseStr.c_str())) {
        _errorResult.resetError(ERROR_FILTER_CLAUSE, string("original string:") + filterClauseStr);
        return false;
    }
    SyntaxExpr *syntaxExpr = ctx.stealSyntaxExpr();
    assert(syntaxExpr);
    filterClause->setRootSyntaxExpr(syntaxExpr);
    return true;
}


bool RequestParser::parseAggregateClause(RequestPtr &requestPtr) {
    HA3_LOG(DEBUG, "parse aggregate clause");
    AggregateClause *aggClause = requestPtr->getAggregateClause();
    if (!aggClause) {
        HA3_LOG(WARN, "failed to parseAggregateClause: aggClause is null");
        return false;
    }
    const string &aggClauseStr = aggClause->getOriginalString();

    StringTokenizer st(aggClauseStr, AGGREGATE_CLAUSE_SEPERATOR,
                       StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        ClauseParserContext ctx;
        if (!ctx.parseAggClause(it->c_str())) {
            HA3_LOG(WARN, "parse aggregate clause failed");
            _errorResult = ctx.getErrorResult();
            return false;
        }
        AggregateDescription* aggDes = ctx.stealAggDescription();
        assert(aggDes);
        if (NULL == aggDes->getGroupKeyExpr()
            || aggDes->getAggFunDescriptions().size() == 0)
        {
            HA3_LOG(WARN, "illegal aggregate desription: [%s]", it->c_str());
            if (aggDes) {
                delete aggDes;
            }
            break;
        }
        HA3_LOG(TRACE3, "add Aggregate description: %s", it->c_str());
        aggClause->addAggDescription(aggDes);
    }

    if (aggClause->getAggDescriptions().empty() ||
        aggClause->getAggDescriptions().size() < st.getNumTokens())
    {
        _errorResult.resetError(ERROR_AGGREGATE_CLAUSE,
                                string("original string:") + aggClauseStr);
        return false;
    }

    return true;
}

bool RequestParser::parseRankFieldBoostConfig(const string& oriStr,
        string& packageIndexName,
        string& fieldName,
        int32_t& fieldBoost)
{
    size_t pos = oriStr.find(RANK_FIELDBOOST_CONFIG_SEPERATOR);
    if (pos == string::npos) {
        _errorResult.resetError(ERROR_NO_DOT_FIELD_BOOST,
                           string("Miss '.' in  fieldboost config: [") + oriStr + "]");
        return false;
    }
    string key = oriStr.substr(0, pos);
    string value = oriStr.substr(pos + strlen(RANK_FIELDBOOST_CONFIG_SEPERATOR));
    StringUtil::trim(key);
    StringUtil::trim(value);

    packageIndexName = key;

    size_t pos1 = value.find(RANK_FIELDBOOST_CONFIG_OPEN_PAREN);
    size_t pos2 = value.rfind(RANK_FIELDBOOST_CONFIG_CLOSE_PAREN);

    if (pos1 == string::npos
        || pos2 == string::npos
        || pos1 + strlen(RANK_FIELDBOOST_CONFIG_OPEN_PAREN) >= pos2)
    {
        _errorResult.resetError(ERROR_FIELD_BOOST_CONFIG,
                           string("illegal fieldboost config: [") + oriStr + "]");
        return false;
    }

    fieldName = value.substr(0, pos1);
    StringUtil::trim(fieldName);

    string boostStr = value.substr(pos1 + strlen(RANK_FIELDBOOST_CONFIG_OPEN_PAREN),
                                   pos2 - pos1 - strlen(RANK_FIELDBOOST_CONFIG_OPEN_PAREN));
    if (!StringUtil::strToInt32(boostStr.c_str(), fieldBoost)) {
        _errorResult.resetError(ERROR_FIELD_BOOST_NUMBER,
                           string("illegal field boost number: [") + boostStr + "]");
        return false;
    }

    return true;
}

bool RequestParser::parseRankClause(RequestPtr &requestPtr) {
    HA3_LOG(TRACE3, "parse rank clause");
    RankClause *rankClause = requestPtr->getRankClause();
    if (!rankClause) {
        HA3_LOG(WARN, "failed to parseRankClause: rankClause is null");
        return false;
    }
    const string &rankClauseStr = rankClause->getOriginalString();

    StringTokenizer st(rankClauseStr, RANK_CLAUSE_SEPERATOR,
                       StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        size_t pos = it->find(RANK_KV_SEPERATOR);
        if (pos == string::npos) {
            _errorResult.resetError(ERROR_KEY_VALUE_PAIR,
                    string("Missing ':' in kv-pair[") + *it + "] in [" + rankClauseStr + "]");
            return false;
        }
        string key = it->substr(0, pos);
        string value = it->substr(pos + strlen(RANK_KV_SEPERATOR));
        StringUtil::trim(key);
        StringUtil::trim(value);

        if (key == RANK_RANK_PROFILE) {
            rankClause->setRankProfileName(value);
        } else if (key == RANK_FIELDBOOST) {
            FieldBoostDescription des;
            StringTokenizer st2(value, RANK_FIELDBOOST_DESCRIPTION_SEPERATOR,
                    StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
            for (StringTokenizer::Iterator it2 = st2.begin(); it2 != st2.end(); ++it2) {
                string packageIndexName;
                string fieldName;
                int32_t fieldBoost;
                if (!parseRankFieldBoostConfig(*it2, packageIndexName, fieldName, fieldBoost)) {
                    return false;
                } else {
                    des[packageIndexName][fieldName] = fieldBoost;
                }
            }
            rankClause->setFieldBoostDescription(des);
        } else if (key == RANK_HINT) {
            RankHint rankHint;
            if (!parseRankHint(value, rankHint)) {
                _errorResult.resetError(ERROR_PARSE_RANK_HINT,
                        "RankClause: Invalid rank hint:" + value);
                return false;
            }
            rankClause->setRankHint(new RankHint(rankHint));
        } else {
            _errorResult.resetError(ERROR_UNKNOWN_KEY,
                    string("Unknown key: ") + key + " in: " + rankClauseStr);
            return false;
        }
    }

    return true;
}

bool RequestParser::parseSortClause(RequestPtr &requestPtr) {
    HA3_LOG(TRACE3, "parse sort clause");
    SortClause *sortClause = requestPtr->getSortClause();
    if (!sortClause) {
        HA3_LOG(WARN, "failed to parseSortClause: sortClause is null");
        return false;
    }
    const string &sortClauseStr = sortClause->getOriginalString();

    StringTokenizer st(sortClauseStr, SORT_CLAUSE_SEPERATOR,
                       StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    if ((size_t)0 == st.getNumTokens()) {
        _errorResult.resetError(ERROR_SORT_NO_DESCRIPTION,
                           "empty error sort description: ");
        return false;
    }
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        const string &originalString = *it;
        unique_ptr<SortDescription> sortDescriptionPtr(new SortDescription(originalString));
        if (originalString.size() < 1) {
            _errorResult.resetError(ERROR_SORT_NO_DESCRIPTION,
                    string("error sort description: ") + originalString);
            return false;
        }

        string trimedString = originalString;
        if (originalString[0] == SORT_CLAUSE_DESCEND) {
            trimedString = originalString.substr(1);
            sortDescriptionPtr->setSortAscendFlag(false);
        } else if(originalString[0] == SORT_CLAUSE_ASCEND) {
            trimedString = originalString.substr(1);
            sortDescriptionPtr->setSortAscendFlag(true);
        }

        if (trimedString != SORT_CLAUSE_RANK) {
            ClauseParserContext ctx;
            if (!ctx.parseSyntaxExpr(trimedString.c_str())) {
                _errorResult.resetError(ERROR_SORT_CLAUSE,
                        string("trimedString: ") + trimedString);
                return false;
            }
            SyntaxExpr* expr = ctx.stealSyntaxExpr();
            assert(expr);
            sortDescriptionPtr->setRootSyntaxExpr(expr);
            sortDescriptionPtr->setExpressionType(SortDescription::RS_NORMAL);
        } else {
            sortDescriptionPtr->setRootSyntaxExpr(new RankSyntaxExpr());
            sortDescriptionPtr->setExpressionType(SortDescription::RS_RANK);
        }
        sortClause->addSortDescription(sortDescriptionPtr.release());
    }

    return true;
}

bool RequestParser::parseDistinctClause(RequestPtr &requestPtr)
{
    HA3_LOG(TRACE3, "parse distinct clauses");
    DistinctClause *distinctClause = requestPtr->getDistinctClause();
    if (!distinctClause) {
        HA3_LOG(WARN, "failed to parseDistinctClause: distClause is null");
        return false;
    }

    const string &distClauseStr = distinctClause->getOriginalString();
    StringTokenizer st(distClauseStr, DISTINCT_CLAUSE_SEPERATOR,
                       StringTokenizer::TOKEN_TRIM |
                       StringTokenizer::TOKEN_IGNORE_EMPTY);
    if (st.getNumTokens() == 0) {
        _errorResult.resetError(ERROR_DISTINCT_CLAUSE,
                                "empty distinct clause");
        return false;
    }

    for (StringTokenizer::Iterator it = st.begin();
         it != st.end(); ++ it)
    {
        if (*it == DISTINCT_NONE_DIST) {
            distinctClause->addDistinctDescription(NULL);
            continue;
        }

        ClauseParserContext ctx;
        if (!ctx.parseDistClause(it->c_str())) {
            HA3_LOG(WARN, "parse distinct clause failed");
            _errorResult = ctx.getErrorResult();
            return false;
        }
        DistinctDescription* distDes = ctx.stealDistDescription();
        assert(distDes);
        if (NULL == distDes->getRootSyntaxExpr()) {
            _errorResult.resetError(ERROR_DISTINCT_MISS_KEY,
                    string("distinct miss key."));
            delete distDes;
            return false;
        }
        distinctClause->addDistinctDescription(distDes);
    }

    return true;
}

bool RequestParser::validateOriginalRequest(RequestPtr &requestPtr)
{
    string oriStr = requestPtr->getOriginalString();
    HA3_LOG(DEBUG, "validate original request. request string[%s]", oriStr.c_str());

    if (oriStr == "") {
        _errorResult.resetError(ERROR_EMPTY_QUERY_STRING, "");
        return false;
    }

    ConfigClause* configClause = requestPtr->getConfigClause();
    if (!configClause) {
        requestPtr->setConfigClause(new ConfigClause());
    }

    vector<string> unknownClause = requestPtr->getUnknownClause();
    for (vector<string>::const_iterator it = unknownClause.begin();
         it != unknownClause.end(); ++it)
    {
        size_t equalPos = (*it).find(CLAUSE_KV_SEPERATOR);
        if (equalPos == string::npos){
            _errorResult.resetError(ERROR_KEY_VALUE_PAIR,
                    string("Missing '=' in sub-clause[")  + (*it) + "] in [" + oriStr + "]");
        }else{
            _errorResult.resetError(ERROR_UNKNOWN_CLAUSE,
                    string("Unknown clause ") + (*it));
        }
        return false;
    }
    return true;
}

bool RequestParser::parseClusterClause(RequestPtr &requestPtr,
                                       const ClusterConfigMapPtr &clusterConfigMapPtr)
{
    HA3_LOG(TRACE3, "parse cluster clause");
    ClusterClause *clusterClause = requestPtr->getClusterClause();
    if (!clusterClause) {
        HA3_LOG(WARN, "failed to parseClusterClause: clusterClause is null");
        return false;
    }

    const string &clusterClauseStr = clusterClause->getOriginalString();

    HA3_LOG(TRACE3, "parseClusterCaluse [%s]", clusterClauseStr.c_str());

    StringTokenizer st(clusterClauseStr, CONFIG_KV_PAIR_SEPERATOR,
                       StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    if (st.getNumTokens() == 0) {
        string errorMsg = "Error cluster clause. oriString[" + clusterClauseStr + "]";
        HA3_LOG(WARN, "%s", errorMsg.c_str());
        _errorResult.resetError(ERROR_CLUSTER_CLAUSE, errorMsg);
        return false;
    }
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        StringTokenizer st1(*it, CONFIG_KV_SEPERATOR,
                           StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
        string clusterName = "";
        vector<pair<hashid_t, hashid_t> > targetParts;
        if (st1.getNumTokens() <= 0 || st1.getNumTokens() > 2) {
            string errorMsg = "Error cluster clause. oriString[" + *it + "]";
            HA3_LOG(WARN, "%s", errorMsg.c_str());
            _errorResult.resetError(ERROR_CLUSTER_CLAUSE, errorMsg);
            return false;
        }
        clusterName = ClauseBase::cluster2ZoneName(st1[0]);
        ClusterConfigMap::const_iterator it1 = clusterConfigMapPtr->find(clusterName);
        if (it1 == clusterConfigMapPtr->end()) {
            _errorResult.resetError(ERROR_CLUSTER_NAME_NOT_EXIST_IN_CLUSTER_CLAUSE,
                    string("ClusterName:") + st1[0]);
            return false;
        }
        if (st1.getNumTokens() == 2) {
            if (!parseTargetPartitions(it1->second, st1[1], targetParts)) {
                return false;
            }
        }

        clusterClause->addClusterPartIds(clusterName, targetParts);
    }
    return true;
}

bool RequestParser::parseHealthCheckClause(common::RequestPtr &requestPtr) {
    HealthCheckClause *healthCheckClause = requestPtr->getHealthCheckClause();
    if (!healthCheckClause) {
        HA3_LOG(WARN, "healthCheckClause is null");
        return false;
    }
    const string &healthCheckString = healthCheckClause->getOriginalString();
    StringTokenizer st(healthCheckString, HEALTHCHECK_CLAUSE_SEPERATOR,
                       StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    if (st.getNumTokens() == (size_t)0) {
        _errorResult.resetError(ERROR_HEALTHCHECK_CLAUSE,
                           string("Error health check clause: empty"));
        return false;
    }
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        StringTokenizer kv(*it, HEALTHCHECK_KV_SEPERATOR,
                           StringTokenizer::TOKEN_TRIM
                           | StringTokenizer::TOKEN_IGNORE_EMPTY);
        if (kv.getNumTokens() != 2) {
            _errorResult.resetError(ERROR_HEALTHCHECK_CLAUSE,
                    string("error health check clause: ") + *it);
            return false;
        }
        string key = kv[0];
        string value = kv[1];
        if (key == HEALTHCHECK_CHECK_FLAG) {
            StringUtil::toLowerCase(value);
            bool check = false;
            if (value == HEALTHCHECK_CHECK_FLAG_TRUE) {
                check = true;
            } else if (value == HEALTHCHECK_CHECK_FLAG_FALSE) {
                check = false;
            } else {
                HA3_LOG(TRACE3, "unexpected value[%s]", value.c_str());
                _errorResult.resetError(ERROR_HEALTHCHECK_CHECK_FLAG,
                        string("health check flag: ") + value);
                return false;
            }
            healthCheckClause->setCheck(check);
        } else if (key == HEALTHCHECK_CHECK_TIMES) {
            int32_t intValue = 0;
            if (!StringUtil::strToInt32(value.c_str(), intValue)
                || intValue < 0)
            {
                _errorResult.resetError(ERROR_HEALTHCHECK_CHECK_TIMES,
                        string("this value is invalid: ")+ value);
                return false;
            }
            healthCheckClause->setCheckTimes(intValue);
        } else if (key == HEALTHCHECK_RECOVER_TIME) {
            int32_t intValue = 0;
            if (!StringUtil::strToInt32(value.c_str(), intValue)
                || intValue < 0)
            {
                _errorResult.resetError(ERROR_HEALTHCHECK_RECOVER_TIME,
                        string("this value is invalid: ")+ value);
                return false;
            }
            healthCheckClause->setRecoverTime(intValue * 1000000ll);
        } else {
            _errorResult.resetError(ERROR_HEALTHCHECK_CLAUSE,
                    string("error health check clause: ") + *it);
            return false;
        }
    }
    return true;
}

bool RequestParser::parseAttributeClause(common::RequestPtr &requestPtr)
{
    AttributeClause *attributeClause = requestPtr->getAttributeClause();
    if (!attributeClause) {
        HA3_LOG(WARN, "attributeClause is null");
        return false;
    }
    const string &attributeString = attributeClause->getOriginalString();
    StringTokenizer st(attributeString, ATTRIBUTE_CLAUSE_SEPERATOR,
                       StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    attributeClause->setAttributeNames(st.begin(), st.end());
    return true;
}

bool RequestParser::parseVirtualAttributeClause(common::RequestPtr &requestPtr)
{
    VirtualAttributeClause *virtualAttributeClause =
        requestPtr->getVirtualAttributeClause();
    if (!virtualAttributeClause) {
        HA3_LOG(WARN, "virtualAttributeClause is null");
        return false;
    }

    string originalString = virtualAttributeClause->getOriginalString();
    ClauseParserContext ctx;
    if (!ctx.parseVirtualAttributeClause(originalString.c_str())) {
        HA3_LOG(WARN, "parse virtualAttribute clause failed");
        _errorResult = ctx.getErrorResult();
        return false;
    }
    VirtualAttributeClause* newVirtualAttributeClause =
        ctx.stealVirtualAttributeClause();
    if (NULL == newVirtualAttributeClause
        || (size_t)0 == newVirtualAttributeClause->getVirtualAttributeSize()) {
        _errorResult.resetError(ERROR_VIRTUALATTRIBUTE_CLAUSE,
                                string("none virtualAttribute pair after parse "
                                        "virtualAttribute clause."));
        delete newVirtualAttributeClause;
        return false;
    }
    requestPtr->setVirtualAttributeClause(newVirtualAttributeClause);
    return true;
}

bool RequestParser::parseTargetPartitions(const ClusterConfigInfo& clusterConfigInfo,
        const string& oriPartStr, vector<pair<hashid_t, hashid_t> >& targetParts)
{
    targetParts.clear();
    StringTokenizer st(oriPartStr, CLAUSE_KV_SEPERATOR,
                       StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    if (st.getNumTokens() != 2) {
        string errorMsg = "Error partition mode config. oriString[" + oriPartStr + "]";
        HA3_LOG(WARN, "%s", errorMsg.c_str());
        _errorResult.resetError(ERROR_CLUSTER_PART_MODE_CONFIG, errorMsg);
        return false;
    }

    string partMode = st[0];
    StringTokenizer tokens(st[1], CLAUSE_SUB_MULTIVALUE_SEPERATOR,
                           StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    HashFunctionBasePtr hashFunc = clusterConfigInfo.getHashFunction();
    if (hashFunc == nullptr) {
        string errorMsg = "Create hash function error. Hash funtion name["
                          + clusterConfigInfo._hashMode._hashFunction + "]";
        HA3_LOG(WARN, "%s", errorMsg.c_str());
        _errorResult.resetError(ERROR_HASH_FUNCTION, errorMsg);
        return false;
    }
    if (partMode == CLUSTER_CLAUSE_KEY_PART_IDS) {
        bool ret = parsePartitionIds(tokens.getTokenVector(), hashFunc, targetParts);
        return ret;
    } else if (partMode == CLUSTER_CLAUSE_KEY_HASH_FIELD) {
        bool ret = parseHashField(tokens.getTokenVector(), hashFunc, targetParts);
        return ret;
    } else {
        string errorMsg = "Error partition mode name. oriString[" + st[0] + "]";
        HA3_LOG(WARN, "%s", errorMsg.c_str());
        _errorResult.resetError(ERROR_CLUSTER_PART_MODE_NAME, errorMsg);
        return false;
    }
    return true;
}

bool RequestParser::parsePartitionIds(const StringTokenizer::TokenVector &tokens,
                                      const autil::HashFunctionBasePtr& hashFunc,
                                      vector<pair<hashid_t, hashid_t> > &partIds)
{
    if (tokens.empty()) {
        string errorMsg = "Parse hash range failed, token is empty.";
        HA3_LOG(WARN, "%s", errorMsg.c_str());
        _errorResult.resetError(ERROR_PART_MODE_INVALID_PARTID, errorMsg);
        return false;
    }
    for (StringTokenizer::Iterator it = tokens.begin(); it != tokens.end(); it++) {
        StringTokenizer st(*it, CONFIG_EXTRA_KV_SEPERATOR,
                           StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
        if (st.getNumTokens() > 2) {
            string errorMsg = "Error partition id format. oriString[" + *it + "]";
            HA3_LOG(WARN, "%s", errorMsg.c_str());
            _errorResult.resetError(ERROR_HASH_MODE_INVALID_FIELD, errorMsg);
            return false;
        }
        uint32_t partId = 0;
        vector<pair<uint32_t, uint32_t> > ranges;
        if (!StringUtil::strToUInt32((st[0]).c_str(), partId)) {
            string errorMsg = "Error partition id string. oriString[" + *it + "]";
            HA3_LOG(WARN, "%s", errorMsg.c_str());
            _errorResult.resetError(ERROR_PART_MODE_INVALID_PARTID, errorMsg);
            return false;
        }
        if (partId >= hashFunc->getHashSize()) {
            string errorMsg = "Error partition id string. oriString[" + *it + "]";
            HA3_LOG(WARN, "%s", errorMsg.c_str());
            _errorResult.resetError(ERROR_PART_MODE_INVALID_PARTID, errorMsg);
            return false;
        }
        
        if (st.getNumTokens() == 1) {
            ranges = hashFunc->getHashRange(partId);
        } else if (st.getNumTokens() == 2) {
            float routingRatio = 0;
            if (!StringUtil::fromString(st[1], routingRatio)) {
                string errorMsg = "parse ratio [" + st[1] +"] failed";
                HA3_LOG(WARN, "%s", errorMsg.c_str());
                _errorResult.resetError(ERROR_HASH_MODE_INVALID_FIELD, errorMsg);
                return false;
            }
            if (routingRatio < 0 || routingRatio > 1) {
                string errorMsg = "parse ratio [" + st[1] +"] failed";
                HA3_LOG(WARN, "%s", errorMsg.c_str());
                _errorResult.resetError(ERROR_HASH_MODE_INVALID_FIELD, errorMsg);
                return false;
            }
            ranges = hashFunc->getHashRange(partId, routingRatio);
        }
        if (ranges.empty()) {
            string errorMsg = "Generate hash range [" + *it+ "] fail, range is empty.";
            HA3_LOG(WARN, "%s", errorMsg.c_str());
            _errorResult.resetError(ERROR_HASH_FUNCTION, errorMsg);
            return false;
        }
        for (auto range : ranges) {
            partIds.emplace_back(make_pair<hashid_t, hashid_t>(range.first, range.second));
        }
    }
    return true;
}

bool RequestParser::parseHashField(const StringTokenizer::TokenVector &tokens,
                                   const autil::HashFunctionBasePtr& hashFunc,
                                   vector<pair<hashid_t, hashid_t> > &partIds)
{
    if (tokens.empty()) {
        string errorMsg = "parse hash range failed, token is empty.";
        HA3_LOG(WARN, "%s", errorMsg.c_str());
        _errorResult.resetError(ERROR_HASH_MODE_EMPTY_FIELD, errorMsg);
        return false;
    }
    StringTokenizer::Iterator it = tokens.begin();
    for (; it != tokens.end(); it++) {
        StringTokenizer st(*it, CONFIG_EXTRA_KV_SEPERATOR,
                           StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
        if (st.getNumTokens() > 2) {
            string errorMsg = "Error hashField format. oriString[" + *it + "]";
            HA3_LOG(WARN, "%s", errorMsg.c_str());
            _errorResult.resetError(ERROR_HASH_MODE_INVALID_FIELD, errorMsg);
            return false;
        }
        const vector<string>& strVec = st.getTokenVector();;
        vector<pair<uint32_t, uint32_t> > ranges = hashFunc->getHashRange(strVec);
        if (ranges.empty()) {
            string errorMsg = "Generate hash range [" + *it + "], range is empty.";
            HA3_LOG(WARN, "%s", errorMsg.c_str());
            _errorResult.resetError(ERROR_HASH_FUNCTION, errorMsg);
            return false;
        }
        for (auto range : ranges) {
            partIds.emplace_back(make_pair<hashid_t, hashid_t>(range.first, range.second));
        }
    }
    return true;
}

bool RequestParser::parseFetchSummaryClause(RequestPtr &requestPtr) {
    FetchSummaryClause *fetchSummaryClause = requestPtr->getFetchSummaryClause();
    assert(fetchSummaryClause);

    string originalStr = fetchSummaryClause->getOriginalString();
    if (originalStr.empty()) {
        string errorMsg = "FetchSummaryClause is empty";
        HA3_LOG(WARN, "%s", errorMsg.c_str());
        _errorResult.resetError(ERROR_FETCH_SUMMARY_CLAUSE, errorMsg);
        return false;
    }

    ConfigClause *configClause = requestPtr->getConfigClause();
    assert(configClause);

    uint32_t fetchSummaryType = configClause->getFetchSummaryType();
    if (fetchSummaryType == BY_DOCID || fetchSummaryType == BY_PK) {
        StringTokenizer st(originalStr, FETCHSUMMARY_CLAUSE_SEPERATOR,
                           StringTokenizer::TOKEN_IGNORE_EMPTY |
                           StringTokenizer::TOKEN_TRIM);
        for (StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
            string clusterName;
            GlobalIdentifier gid;
            if (!parseGid(*it, clusterName, gid)) {
                string errorMsg = "parse gid error. gid[" + *it + "]";
                HA3_LOG(WARN, "%s", errorMsg.c_str());
                _errorResult.resetError(ERROR_FETCH_SUMMARY_CLAUSE, errorMsg);
                return false;
            }
            fetchSummaryClause->addGid(clusterName, gid);
        }
    } else if (fetchSummaryType == BY_RAWPK) {
        size_t startPos = 0;
        while (startPos < originalStr.length()) {
            string clusterName;
            vector<string> rawPkVec;
            if (!parseRawPk(originalStr, startPos, clusterName, rawPkVec)) {
                string errorMsg = "parse raw pk error. original str:" +
                                  originalStr;
                HA3_LOG(WARN, "%s", errorMsg.c_str());
                _errorResult.resetError(ERROR_FETCH_SUMMARY_CLAUSE, errorMsg);
                return false;
            }
            for (vector<string>::const_iterator it = rawPkVec.begin();
                 it != rawPkVec.end(); ++it)
            {
                fetchSummaryClause->addRawPk(clusterName, *it);
            }
        }
    }
    return true;
}

bool RequestParser::parseKVPairClause(RequestPtr &requestPtr) {
    KVPairClause *clause = requestPtr->getKVPairClause();
    assert(clause);

    const string &originalStr = clause->getOriginalString();
    if (originalStr.empty()) {
        return true;
    }

    ConfigClause *configClause = requestPtr->getConfigClause();
    assert(configClause);

    size_t start = 0;
    while (start < originalStr.length()) {
        string key = getNextTerm(originalStr, KVPAIR_CLAUSE_KV_SEPERATOR, start);
        assert(start > 0);
        if (start >= originalStr.length()) {
            if (!key.empty()) {
                string errorMsg = "Parse kvpair clause failed, Invalid kvpairs: [" + originalStr + "]";
                HA3_LOG(WARN, "%s", errorMsg.c_str());
                _errorResult.resetError(ERROR_PARSE_KVPAIR_CLAUSE, errorMsg);
                return false;
            } else {
                break;
            }
        }
        string value = getNextTerm(originalStr, KVPAIR_CLAUSE_KV_PAIR_SEPERATOR, start);
        configClause->addKVPair(key, value);
    }
    return true;
}

bool RequestParser::parseQueryLayerClause(RequestPtr &requestPtr) {
    HA3_LOG(DEBUG, "parse query layer clause");
    QueryLayerClause *layerClause = requestPtr->getQueryLayerClause();
    if (!layerClause) {
        HA3_LOG(WARN, "failed to parseQueryLayerClause: layerClause is null");
        return false;
    }
    const string &layerClauseStr = layerClause->getOriginalString();

    ClauseParserContext ctx;
    if (!ctx.parseLayerClause(layerClauseStr.c_str())) {
        HA3_LOG(WARN, "parse query layer clause failed");
        _errorResult = ctx.getErrorResult();
        return false;
    }
    layerClause = ctx.stealLayerClause();
    assert(layerClause);

    requestPtr->setQueryLayerClause(layerClause);
    return true;
}

bool RequestParser::parseSearcherCacheClause(RequestPtr & requestPtr) {
    HA3_LOG(DEBUG, "parse searcher cache clause");

    SearcherCacheClause* cacheClause = requestPtr->getSearcherCacheClause();
    if (!cacheClause) {
        HA3_LOG(WARN,
                "failed to parseSearcherCacheClause: cacheClause is null");
        return false;
    }
    const string & cacheClauseStr = cacheClause->getOriginalString();

    ClauseParserContext ctx;
    if (!ctx.parseSearcherCacheClause(cacheClauseStr.c_str())) {
        HA3_LOG(WARN, "parse searcher cache clause failed");
        _errorResult = ctx.getErrorResult();
        return false;
    }
    cacheClause = ctx.stealSearcherCacheClause();
    assert(cacheClause);

    requestPtr->setSearcherCacheClause(cacheClause);
    return true;
}

bool RequestParser::parseOptimizerClause(common::RequestPtr &request) {
    OptimizerClause* optimizerClause = request->getOptimizerClause();
    if (!optimizerClause) {
        HA3_LOG(WARN, "failed to parseOptimizerClause: cacheClause is null");
        return false;
    }
    const string &originalStr = optimizerClause->getOriginalString();
    StringTokenizer st(originalStr, ";",
                       StringTokenizer::TOKEN_IGNORE_EMPTY |
                       StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        StringTokenizer st1(st[i], ",",
                            StringTokenizer::TOKEN_IGNORE_EMPTY |
                            StringTokenizer::TOKEN_TRIM);
        for (size_t j = 0; j < st1.getNumTokens(); ++j) {
            StringTokenizer st2(st1[j], ":",
                    StringTokenizer::TOKEN_IGNORE_EMPTY |
                    StringTokenizer::TOKEN_TRIM);

            if (st2.getNumTokens() != 2) {
                HA3_LOG(WARN, "Invalid optimizer[%s]", st1[j].c_str());
                _errorResult.setErrorCode(ERROR_OPTIMIZER_CLAUSE);
                return false;
            }
            if (st2[0] == "name") {
                optimizerClause->addOptimizerName(st2[1]);
            } else if (st2[0] == "option") {
                optimizerClause->addOptimizerOption(st2[1]);
            } else {
                HA3_LOG(WARN, "Invalid optimize key[%s]", st2[0].c_str());
                _errorResult.setErrorCode(ERROR_OPTIMIZER_CLAUSE);
                return false;
            }
        }
    }
    return true;
}

bool RequestParser::parseGid(const string &gidStr, string &clusterName,
                             GlobalIdentifier &gid)
{
    if (gidStr.empty()) {
        return false;
    }

    size_t begin = gidStr.size();
    uint32_t fullIndexVersion = 0;
    int32_t versionId = 0;
    hashid_t hashId = 0;
    docid_t docId = 0;
    primarykey_t pk = 0;
    uint32_t ip = 0;

    if (gidStr.find(FETCHSUMMARY_GID_SEPERATOR) != string::npos) {
        if (!getValueFromGid(gidStr, FETCHSUMMARY_GID_SEPERATOR, begin, ip) ||
            !getValueFromGid(gidStr, FETCHSUMMARY_GID_SEPERATOR, begin, pk) ||
            !getValueFromGid(gidStr, FETCHSUMMARY_GID_SEPERATOR, begin, docId) ||
            !getValueFromGid(gidStr, FETCHSUMMARY_GID_SEPERATOR, begin, hashId) ||
            !getValueFromGid(gidStr, FETCHSUMMARY_GID_SEPERATOR, begin, versionId) ||
            !getValueFromGid(gidStr, FETCHSUMMARY_GID_SEPERATOR, begin, fullIndexVersion))
        {
            return false;
        }
    } else {
        if (!getValueFromGid(gidStr, FETCHSUMMARY_GID_SEPERATOR_DEPRECATED, begin, docId) ||
            !getValueFromGid(gidStr, FETCHSUMMARY_GID_SEPERATOR_DEPRECATED, begin, hashId) ||
            !getValueFromGid(gidStr, FETCHSUMMARY_GID_SEPERATOR_DEPRECATED, begin, versionId) ||
            !getValueFromGid(gidStr, FETCHSUMMARY_GID_SEPERATOR_DEPRECATED, begin, fullIndexVersion))
        {
            return false;
        }

    }

    string clusterNameTemp = gidStr.substr(0, begin);
    if (clusterNameTemp.empty()) {
        return false;
    }

    clusterName = clusterNameTemp;
    gid.setFullIndexVersion(fullIndexVersion);
    gid.setIndexVersion(versionId);
    gid.setHashId(hashId);
    gid.setDocId(docId);
    gid.setPrimaryKey(pk);
    gid.setIp(ip);
    return true;
}

bool RequestParser::getValueStrFromGid(const string& gidStr, const string& sep,
                                       size_t &begin, string &value)
{
    size_t end = begin - 1;
    begin = gidStr.rfind(sep, end);
    if (begin == std::string::npos || begin == 0) {
        return false;
    }
    value = gidStr.substr(begin + sep.length(), end - begin);
    return true;
}

bool RequestParser::parseRawPk(const string &str, size_t &pos,
                               string &clusterName, vector<string> &rawPkVec)
{
    clusterName = "";
    rawPkVec.clear();

    bool meetsKvSep = false;
    string temp = "";
    size_t length = str.length();
    while (pos < length) {
        if (str[pos] == '\\') {
            if (pos + 1 < length) {
                temp += str[pos + 1];
            } else {
                HA3_LOG(WARN, "raw pk ends with \\, which need be escaped");
                return false;
            }
            pos += 2;
        } else if (str[pos] == FETCHSUMMARY_CLUSTER_SEPERATOR) {
            pos++;
            break;
        } else if (str[pos] == FETCHSUMMARY_KV_SEPERATOR) {
            if (meetsKvSep) {
                HA3_LOG(WARN, "invalid fetch summary clause, "
                        "multi kv seprators");
                return false;
            }
            meetsKvSep = true;
            autil::StringUtil::trim(temp);
            if (temp.empty()) {
                HA3_LOG(WARN, "invalid fetch summary clause, "
                        "empty cluster name");
                return false;
            }
            clusterName = temp;
            temp = "";
            pos++;
        } else if (str[pos] == FETCHSUMMARY_RAWPK_SEPERATOR) {
            if (!meetsKvSep) {
                HA3_LOG(WARN, "invalid fetch summary clause, "
                        "invalid cluster name");
                return false;
            }
            autil::StringUtil::trim(temp);
            if (temp.empty()) {
                HA3_LOG(WARN, "invalid fetch summary clause, empty raw pk");
                return false;
            }
            rawPkVec.push_back(temp);
            temp = "";
            pos++;
        } else {
            temp += str[pos++];
        }
    }
    if (!meetsKvSep || clusterName.empty()) {
        HA3_LOG(WARN, "parse cluster name from raw pk failed");
        return false;
    }
    autil::StringUtil::trim(temp);
    if (!temp.empty()) {
        rawPkVec.push_back(temp);
    }
    if (rawPkVec.empty()) {
        HA3_LOG(WARN, "empty raw pk");
        return false;
    }
    return true;
}

bool RequestParser::parseConfigClauseProtoFormatOption(
        ConfigClause *configClause, const string &value)
{
    StringTokenizer st(value, CLAUSE_MULTIVALUE_SEPERATOR,
                       StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        const string &option = *it;
        if (option == "summary_in_bytes") {
            configClause->addProtoFormatOption(
                    PBResultFormatter::SUMMARY_IN_BYTES);
        } else if (option == "pb_matchdoc_format") {
            configClause->addProtoFormatOption(
                    PBResultFormatter::PB_MATCHDOC_FORMAT);
        } else {
            return false;
        }
    }
    return true;
}

string RequestParser::getNextTerm(const std::string &inputStr, char sep, size_t &start) {
    string ret;
    ret.reserve(64);
    while (start < inputStr.length()) {
        char c = inputStr[start];
        ++start;
        if (c == sep) {
            break;
        } else if (c == '\\') {
            if (start < inputStr.length()) {
                ret += inputStr[start];
                ++start;
            }
            continue;
        }
        ret += c;
    }
    autil::StringUtil::trim(ret);
    return ret;
}

bool RequestParser::parseRankHint(const std::string &rankHintStr, RankHint &rankHint) {
    if (rankHintStr.empty()) {
        return false;
    }
    if (rankHintStr[0] == '+') {
        rankHint.sortPattern = sp_asc;
    } else if (rankHintStr[0] == '-') {
        rankHint.sortPattern = sp_desc;
    } else {
        return false;
    }
    rankHint.sortField.assign(rankHintStr, 1, rankHintStr.length() -1);
    return true;

}

bool RequestParser::parseRankSortClause(RequestPtr &requestPtr) {
    HA3_LOG(DEBUG, "parse rank sort clause");

    RankSortClause* rankSortClause = requestPtr->getRankSortClause();
    if (!rankSortClause) {
        HA3_LOG(WARN, "failed to parseRankSortClause: "
                "rankSortClause is null");
        return false;
    }
    const string &rankSortClauseStr = rankSortClause->getOriginalString();

    ClauseParserContext ctx;
    if (!ctx.parseRankSortClause(rankSortClauseStr.c_str())) {
        HA3_LOG(WARN, "parse rank sort clause failed");
        _errorResult = ctx.getErrorResult();
        return false;
    }
    rankSortClause = ctx.stealRankSortClause();
    assert(rankSortClause);

    requestPtr->setRankSortClause(rankSortClause);

    return true;
}

bool RequestParser::parseFinalSortClause(RequestPtr &requestPtr) {
    HA3_LOG(DEBUG, "parse rank sort clause");
    FinalSortClause* finalSortClause = requestPtr->getFinalSortClause();
    if (!finalSortClause) {
        HA3_LOG(WARN, "failed to parseFinalSortClause: "
                "finalSortClause is null");
        return false;
    }

    const string &finalSortClauseStr = finalSortClause->getOriginalString();
    StringTokenizer st(finalSortClauseStr, ";",
                       StringTokenizer::TOKEN_IGNORE_EMPTY |
                       StringTokenizer::TOKEN_TRIM);
    if (st.getNumTokens() != 1 && st.getNumTokens() != 2) {
        HA3_LOG(WARN, "split finalSortClause failed, split num is wrong!");
        string errorMsg = "split finalSortClause failed, split num is wrong: ["
                          + finalSortClauseStr + "]";
        _errorResult.resetError(ERROR_FINAL_SORT_PARSE_FAILED, errorMsg);
        return false;
    }

    const string &sortNameStr = st[0];
    StringTokenizer sortNameSt(sortNameStr, ":",
                               StringTokenizer::TOKEN_IGNORE_EMPTY |
                               StringTokenizer::TOKEN_TRIM);
    if (sortNameSt.getNumTokens() != 2) {
        HA3_LOG(WARN, "parse sortName[%s] failed in finalSortClause",
                sortNameStr.c_str());
        string errorMsg = "parse sortName[" + sortNameStr+ "] failed in finalSortClause: ["
                          + finalSortClauseStr + "]";
        _errorResult.resetError(ERROR_FINAL_SORT_PARSE_FAILED, errorMsg);
        return false;
    }
    if (sortNameSt[0] != FINALSORT_SORT_NAME) {
        HA3_LOG(WARN, "not specify sorter name in finalSortClause");
        string errorMsg = "not specify sorter name in finalSortClause: ["
                          + finalSortClauseStr + "]";
        _errorResult.resetError(ERROR_FINAL_SORT_PARSE_FAILED, errorMsg);
        return false;
    }
    finalSortClause->setSortName(sortNameSt[1]);
    if (1 == st.getNumTokens()) {
        return true;
    }

    const string &sortParamStr = st[1];
    StringTokenizer sortParamSt(sortParamStr, ":",
                                StringTokenizer::TOKEN_IGNORE_EMPTY |
                                StringTokenizer::TOKEN_TRIM);
    if (sortParamSt.getNumTokens() != 2) {
        HA3_LOG(WARN, "parse sortParam[%s] failed in finalSortClause",
                sortParamStr.c_str());
        string errorMsg = "parse sortParam[" + sortParamStr
                          + "] failed in finalSortClause: ["
                          + finalSortClauseStr + "]";
        _errorResult.resetError(ERROR_FINAL_SORT_PARSE_FAILED, errorMsg);
        return false;
    }
    if (sortParamSt[0] != FINALSORT_SORT_PARAM) {
        HA3_LOG(WARN, "parse sortParam[%s] failed in finalSortClause",
                sortParamStr.c_str());
        string errorMsg = "parse sortParam[" + sortParamStr
                          + "] failed in finalSortClause: ["
                          + finalSortClauseStr + "]";
        _errorResult.resetError(ERROR_FINAL_SORT_PARSE_FAILED, errorMsg);
        return false;
    }

    string sortParamContent = sortParamSt[1];
    sortParamSt.tokenize(sortParamContent, ",",
                         StringTokenizer::TOKEN_IGNORE_EMPTY |
                         StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < sortParamSt.getNumTokens(); ++i) {
        StringTokenizer kvSt(sortParamSt[i], "#",
                             StringTokenizer::TOKEN_IGNORE_EMPTY |
                             StringTokenizer::TOKEN_TRIM);
        if (kvSt.getNumTokens() != 2) {
            HA3_LOG(WARN, "parse kv pair[%s] failed in finalSortClause",
                    sortParamSt[i].c_str());
            string errorMsg = "parse kv pair[" + string(sortParamSt[i].c_str())
                              + "] failed in finalSortClause: ["
                              + finalSortClauseStr + "]";
            _errorResult.resetError(ERROR_FINAL_SORT_PARSE_FAILED, errorMsg);
            return false;
        }
        finalSortClause->addParam(kvSt[0], kvSt[1]);
    }
    return true;
}

bool RequestParser::parseLevelClause(RequestPtr &requestPtr) {
    LevelClause *levelClause = requestPtr->getLevelClause();
    assert(levelClause);

    const string &levelClauseStr = levelClause->getOriginalString();
    if (levelClauseStr.empty()) {
        HA3_LOG(DEBUG, "Empty Level Clause Original String");
        return true;
    }
    StringTokenizer st(levelClauseStr, LEVEL_CLAUSE_KV_PAIR_SEPERATOR,
                       StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);

    bool hasLevelClusters = false;
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        StringTokenizer st2(*it, LEVEL_CLAUSE_KV_SEPERATOR,
                            StringTokenizer::TOKEN_IGNORE_EMPTY
                            | StringTokenizer::TOKEN_TRIM);
        if (st2.getNumTokens() != 2) {
            HA3_LOG(WARN, "Failed to parse LevelClause:[%s]",
                    levelClauseStr.c_str());
            _errorResult.resetError(ERROR_KEY_VALUE_PAIR,
                    string("Error KV Pair in levelClause: ") + *it);
            return false;
        }

        const string &key = st2[0];
        const string &value = st2[1];
        if (key == LEVEL_CLAUSE_KEY_SECOND_LEVEL_CLUSTER) {
            if (hasLevelClusters) {
                HA3_LOG(WARN, "Failed to parse LevelClause:[%s]",
                        levelClauseStr.c_str());
                _errorResult.resetError(ERROR_LEVEL_CLAUSE,
                        string("Can't use both second_level_cluster and level_clusters in level clause"));
                return false;
            }
            hasLevelClusters = true;
            if (value.empty()) {
                HA3_LOG(WARN, "Failed to parse LevelClause:[%s]",
                        levelClauseStr.c_str());
                _errorResult.resetError(ERROR_LEVEL_CLAUSE,
                        string("Empty cluster name in level clause"));
                return false;
            }
            levelClause->setSecondLevelCluster(value);
        } else if (key == LEVEL_CLAUSE_KEY_LEVEL_CLUSTERS) {
            if(hasLevelClusters) {
                HA3_LOG(WARN, "Failed to parse LevelClause:[%s]",
                        levelClauseStr.c_str());
                _errorResult.resetError(ERROR_LEVEL_CLAUSE,
                        string("Can't use both second_level_cluster and level_clusters in level clause"));
                return false;
            }
            hasLevelClusters = true;
            ClusterLists clusterLists;
            StringTokenizer ss(value, LEVEL_CLAUSE_CLUSTER_LIST_SEPERATOR,
                    StringTokenizer::TOKEN_IGNORE_EMPTY
                    | StringTokenizer::TOKEN_TRIM);
            for (StringTokenizer::Iterator it = ss.begin(); it != ss.end(); it++) {
                if (it->empty()) {
                    continue;
                }
                vector<string> clusterList;
                StringTokenizer sb(*it, LEVEL_CLAUSE_CLUSTER_SEPERATOR,
                        StringTokenizer::TOKEN_IGNORE_EMPTY
                        | StringTokenizer::TOKEN_TRIM);
                for (StringTokenizer::Iterator it1 = sb.begin(); it1 != sb.end(); it1++) {
                    if (it1->empty()) {
                        continue;
                    }
                    clusterList.push_back(*it1);
                }
                if (!clusterList.empty()) {
                    clusterLists.push_back(clusterList);
                }
            }

            if (clusterLists.empty()) {
                HA3_LOG(WARN, "Failed to parse LevelClause:[%s]",
                        levelClauseStr.c_str());
                _errorResult.resetError(ERROR_LEVEL_CLAUSE,
                        string("Empty cluster lists in level clause"));
                return false;
            }

            levelClause->setLevelClusters(clusterLists);
        } else if (key == LEVEL_CLAUSE_KEY_USE_LEVEL) {
            if (value == "no") {
                levelClause->setUseLevel(false);
            }
        } else if (key == LEVEL_CLAUSE_KEY_LEVEL_TYPE) {
            if (value == LEVEL_CLAUSE_VALUE_LEVEL_TYPE_BOTH) {
                levelClause->setLevelQueryType(BOTH_LEVEL);
            } else if (value == LEVEL_CLAUSE_VALUE_LEVEL_TYPE_GOOD) {
                levelClause->setLevelQueryType(ONLY_FIRST_LEVEL);
            }
        } else if (key == LEVEL_CLAUSE_KEY_MIN_DOCS) {
            uint32_t minDocs;
            if (!StringUtil::strToUInt32(value.c_str(), minDocs)) {
                HA3_LOG(WARN, "Failed to parse LevelClause:[%s]",
                        levelClauseStr.c_str());
                _errorResult.resetError(ERROR_LEVEL_CLAUSE,
                        string("Error min docs in level clause: ") + value);
                return false;
            }
            levelClause->setMinDocs(minDocs);
        } else {
            HA3_LOG(WARN, "Unknown key/value in LevelClause:[%s:%s]",
                    key.c_str(), value.c_str());
        }
    }
    return true;
}

bool RequestParser::parseAnalyzerClause(RequestPtr &requestPtr) {
    AnalyzerClause* analyzerClause = requestPtr->getAnalyzerClause();
    const string& analyzerClauseString = analyzerClause->getOriginalString();

    StringTokenizer st(analyzerClauseString, ANALYZER_CLAUSE_SECTION_KV_PAIR_SEPERATOR,
                       StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        StringTokenizer st2(*it, ANALYZER_CLAUSE_SECTION_KV_SEPERATOR,
                            StringTokenizer::TOKEN_IGNORE_EMPTY
                            | StringTokenizer::TOKEN_TRIM);
        HA3_LOG(DEBUG, "Analyzer Clause Token num[%zd]", st2.getNumTokens());
        if (st2.getNumTokens() != 2) {
            HA3_LOG(WARN, "Failed to parse AnalyzerClause:[%s]",
                    analyzerClauseString.c_str());
            _errorResult.resetError(ERROR_INVALID_ANALYZER_GRAMMAR,
                    string("Error KV Pair in analyzerClause: ") + *it);
            return false;
        }

        const string &key = st2[0];
        const string &value = st2[1];
        ConfigClause *configClause = requestPtr->getConfigClause();
        assert(configClause);
        if (ANALYZER_CLAUSE_GLOBAL_ANALYZER == key) {
            configClause->setAnalyzerName(value);
        } else if (ANALYZER_CLAUSE_NO_TOKENIZE == key) {
            if (!parseNoTokenizeIndexSection(value, configClause)) {
                return false;
            }
        } else if (ANALYZER_CLAUSE_SPECIFIC_INDEX_ANALYZER == key) {
            if (!parseSpecificIndexAnalyzerSection(value, configClause)) {
                return false;
            }
        }
    }

    return true;
}

bool RequestParser::parseAuxQueryClause(RequestPtr &requestPtr) {
    AuxQueryClause* auxQueryClause = requestPtr->getAuxQueryClause();
    if (!auxQueryClause) {
        HA3_LOG(WARN, "failed to parseAuxQueryClause: auxQueryClause is null");
        return false;
    }
    const string &auxQueryClauseStr = auxQueryClause->getOriginalString();

    QueryOperator defaultOP = OP_AND;
    QueryParser queryParser("", defaultOP, false);

    HA3_LOG(TRACE2, "QueryText: [%s]", auxQueryClauseStr.c_str());

    ParserContext *context = queryParser.parse(auxQueryClauseStr.c_str());

    if(ParserContext::OK != context->getStatus()){
        _errorResult.resetError(ERROR_QUERY_CLAUSE,
                           string("Parse aux query failed! error description: ") +
                           context->getStatusMsg() + ", auxQueryClauseStr:" + auxQueryClauseStr);
        delete context;
        return false;
    }

    vector<Query*> querys = context->stealQuerys();
    for (uint32_t i = 0; i < querys.size(); ++i) {
        auxQueryClause->setRootQuery(querys[i], i);
    }

    delete context;
    return true;
}

bool RequestParser::parseAuxFilterClause(AuxFilterClause *auxFilterClause) {
    if (!auxFilterClause) {
        HA3_LOG(WARN, "failed to parseAuxFilterClause: auxFilterClause is null");
        return false;
    }
    const string &auxFilterClauseStr = auxFilterClause->getOriginalString();
    ClauseParserContext ctx;
    if (!ctx.parseSyntaxExpr(auxFilterClauseStr.c_str())) {
        _errorResult.resetError(ERROR_AUX_FILTER_CLAUSE, string("original string:") + auxFilterClauseStr);
        return false;
    }
    SyntaxExpr *syntaxExpr = ctx.stealSyntaxExpr();
    assert(syntaxExpr);
    auxFilterClause->setRootSyntaxExpr(syntaxExpr);
    return true;
}

bool RequestParser::parseNoTokenizeIndexSection(const string& originalStr,
        ConfigClause *configClause)
{
    assert(configClause);
    StringTokenizer st(originalStr, ANALYZER_CLAUSE_NO_TOKENIZE_SEPERATOR,
                       StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        configClause->addNoTokenizeIndex(*it);
    }

    return true;
}

bool RequestParser::parseSpecificIndexAnalyzerSection(const string& originalStr,
        ConfigClause *configClause)
{
    assert(configClause);
    StringTokenizer st(originalStr, ANALYZER_CLAUSE_INDEX_KV_SEPERATOR,
                       StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        StringTokenizer st2(*it, ANALYZER_CLAUSE_INDEX_KV_PAIR_SEPERATOR,
                            StringTokenizer::TOKEN_IGNORE_EMPTY
                            | StringTokenizer::TOKEN_TRIM);
        HA3_LOG(DEBUG, "Analyzer Clause specific Index Analyzer Token num[%zd]",
                st2.getNumTokens());
        if (st2.getNumTokens() != 2) {
            HA3_LOG(WARN, "Failed to parse AnalyzerClause "
                    "SpecificIndexAnalyzer part:[%s]", originalStr.c_str());
            _errorResult.resetError(ERROR_INVALID_ANALYZER_GRAMMAR,
                    string("Error SpecificIndexAnalyzer in analyzerClause: ") + *it);
            return false;
        }

        const string &key = st2[0];
        const string &value = st2[1];
        configClause->addIndexAnalyzerName(key, value);
    }
    return true;

}

END_HA3_NAMESPACE(queryparser);

