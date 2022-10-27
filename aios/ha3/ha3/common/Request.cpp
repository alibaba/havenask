#include <ha3/common/Request.h>
#include <autil/StringTokenizer.h>
#include <ha3/queryparser/RequestSymbolDefine.h>
#include <multi_call/common/ControllerParam.h>

using namespace autil;
using namespace autil::legacy;
using namespace std;

#define AUX_QUERY_DATABUFFER_SECTION "AUX_QUERY_DATABUFFER_SECTION"

USE_HA3_NAMESPACE(util);
BEGIN_HA3_NAMESPACE(common);

HA3_LOG_SETUP(common, Request);

Request::Request(autil::mem_pool::Pool *pool) {
    _configClause = NULL;
    _queryClause = NULL;
    _filterClause = NULL;
    _pkFilterClause = NULL;
    _sortClause = NULL;
    _distinctClause = NULL;
    _aggregateClause = NULL;    
    _rankClause = NULL;
    _docIdClause = NULL;
    _clusterClause = NULL;
    _healthCheckClause = NULL;
    _attributeClause = NULL;
    _virtualAttributeClause = NULL;
    _fetchSummaryClause = NULL;
    _kvPairClause = NULL;
    _queryLayerClause = NULL;
    _searcherCacheClause = NULL;
    _optimizerClause = NULL;
    _rankSortClause = NULL;
    _finalSortClause = NULL;
    _levelClause = NULL;
    _analyzerClause = NULL;
    _auxQueryClause = NULL;
    _auxFilterClause = NULL;
    _partitionMode = DOCID_PARTITION_MODE;
    _rowKey = 0;
    _pool = pool;
    _degradeLevel = multi_call::MIN_PERCENT;
    _rankSize = 0;
    _rerankSize = 0;
    _useGrpc = false;
}

Request::~Request() {
    release();
    // do not set pool NULL in release!!
    _pool = NULL;
}

void Request::release() {
    DELETE_AND_SET_NULL(_configClause);
    DELETE_AND_SET_NULL(_queryClause);
    DELETE_AND_SET_NULL(_filterClause);
    DELETE_AND_SET_NULL(_pkFilterClause);
    DELETE_AND_SET_NULL(_sortClause);
    DELETE_AND_SET_NULL(_distinctClause);
    DELETE_AND_SET_NULL(_aggregateClause);    
    DELETE_AND_SET_NULL(_rankClause);
    DELETE_AND_SET_NULL(_docIdClause);
    DELETE_AND_SET_NULL(_clusterClause);
    DELETE_AND_SET_NULL(_healthCheckClause);
    DELETE_AND_SET_NULL(_attributeClause);
    DELETE_AND_SET_NULL(_virtualAttributeClause);
    DELETE_AND_SET_NULL(_fetchSummaryClause);
    DELETE_AND_SET_NULL(_kvPairClause);
    DELETE_AND_SET_NULL(_queryLayerClause);
    DELETE_AND_SET_NULL(_searcherCacheClause);
    DELETE_AND_SET_NULL(_optimizerClause);
    DELETE_AND_SET_NULL(_rankSortClause);
    DELETE_AND_SET_NULL(_finalSortClause);
    DELETE_AND_SET_NULL(_levelClause);
    DELETE_AND_SET_NULL(_analyzerClause);
    DELETE_AND_SET_NULL(_auxQueryClause);
    DELETE_AND_SET_NULL(_auxFilterClause);

    _unknownClause.clear();
    _partitionMode.clear();
    _curClusterName.clear();
}

#define SET_AND_GET_CLAUSE_HELPER(className, memberName)                \
    void Request::set##className(className *clause) {                   \
        if (clause == memberName) {                                     \
            return ;                                                    \
        }                                                               \
        if (memberName != NULL) {                                       \
            delete memberName;                                          \
        }                                                               \
        memberName = clause;                                            \
    }                                                                   \
    className *Request::get##className() const {                        \
        return memberName;                                              \
    }                                                                   \
    className *Request::steal##className() {                            \
        className *tmp = memberName;                                    \
        memberName = NULL;                                              \
        return tmp;                                                     \
    }

SET_AND_GET_CLAUSE_HELPER(ConfigClause, _configClause);
SET_AND_GET_CLAUSE_HELPER(QueryClause, _queryClause);
SET_AND_GET_CLAUSE_HELPER(FilterClause, _filterClause);
SET_AND_GET_CLAUSE_HELPER(PKFilterClause, _pkFilterClause);
SET_AND_GET_CLAUSE_HELPER(SortClause, _sortClause);
SET_AND_GET_CLAUSE_HELPER(DistinctClause, _distinctClause);
SET_AND_GET_CLAUSE_HELPER(AggregateClause, _aggregateClause);
SET_AND_GET_CLAUSE_HELPER(RankClause, _rankClause);
SET_AND_GET_CLAUSE_HELPER(DocIdClause, _docIdClause);
SET_AND_GET_CLAUSE_HELPER(ClusterClause, _clusterClause);
SET_AND_GET_CLAUSE_HELPER(HealthCheckClause, _healthCheckClause);
SET_AND_GET_CLAUSE_HELPER(AttributeClause, _attributeClause);
SET_AND_GET_CLAUSE_HELPER(VirtualAttributeClause, _virtualAttributeClause);
SET_AND_GET_CLAUSE_HELPER(FetchSummaryClause, _fetchSummaryClause);
SET_AND_GET_CLAUSE_HELPER(KVPairClause, _kvPairClause);
SET_AND_GET_CLAUSE_HELPER(QueryLayerClause, _queryLayerClause);
SET_AND_GET_CLAUSE_HELPER(SearcherCacheClause, _searcherCacheClause);
SET_AND_GET_CLAUSE_HELPER(OptimizerClause, _optimizerClause);
SET_AND_GET_CLAUSE_HELPER(RankSortClause, _rankSortClause);
SET_AND_GET_CLAUSE_HELPER(FinalSortClause, _finalSortClause);
SET_AND_GET_CLAUSE_HELPER(LevelClause, _levelClause);
SET_AND_GET_CLAUSE_HELPER(AnalyzerClause, _analyzerClause);
SET_AND_GET_CLAUSE_HELPER(AuxQueryClause, _auxQueryClause);
SET_AND_GET_CLAUSE_HELPER(AuxFilterClause, _auxFilterClause);

#define CLONE_CLAUSE_HELPER(className, memberName)                      \
    className *Request::clone##className() {                            \
        if (memberName == NULL) {                                       \
            return NULL;                                                \
        }                                                               \
        autil::DataBuffer dataBuffer(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, _pool); \
        memberName->serialize(dataBuffer);                              \
        className *clause = new className();                            \
        clause->deserialize(dataBuffer);                                \
        return clause;                                                  \
    }                                                                   \

CLONE_CLAUSE_HELPER(ConfigClause, _configClause);
CLONE_CLAUSE_HELPER(QueryClause, _queryClause);
CLONE_CLAUSE_HELPER(FilterClause, _filterClause);
CLONE_CLAUSE_HELPER(PKFilterClause, _pkFilterClause);
CLONE_CLAUSE_HELPER(SortClause, _sortClause);
CLONE_CLAUSE_HELPER(DistinctClause, _distinctClause);
CLONE_CLAUSE_HELPER(AggregateClause, _aggregateClause);
CLONE_CLAUSE_HELPER(RankClause, _rankClause);
CLONE_CLAUSE_HELPER(DocIdClause, _docIdClause);
CLONE_CLAUSE_HELPER(ClusterClause, _clusterClause);
CLONE_CLAUSE_HELPER(HealthCheckClause, _healthCheckClause);
CLONE_CLAUSE_HELPER(AttributeClause, _attributeClause);
CLONE_CLAUSE_HELPER(VirtualAttributeClause, _virtualAttributeClause);
CLONE_CLAUSE_HELPER(FetchSummaryClause, _fetchSummaryClause);
CLONE_CLAUSE_HELPER(QueryLayerClause, _queryLayerClause);
CLONE_CLAUSE_HELPER(SearcherCacheClause, _searcherCacheClause);
CLONE_CLAUSE_HELPER(OptimizerClause, _optimizerClause);
CLONE_CLAUSE_HELPER(RankSortClause, _rankSortClause);
CLONE_CLAUSE_HELPER(FinalSortClause, _finalSortClause);
CLONE_CLAUSE_HELPER(LevelClause, _levelClause);
CLONE_CLAUSE_HELPER(AnalyzerClause, _analyzerClause);
CLONE_CLAUSE_HELPER(AuxQueryClause, _auxQueryClause);
CLONE_CLAUSE_HELPER(AuxFilterClause, _auxFilterClause);


void Request::setOriginalString(const string& originalString) {
    release();
    splitClause(originalString);
}

const std::string Request::getOriginalString() const {
    string kv_seperator(CLAUSE_KV_SEPERATOR);
    string clause_seperator(CLAUSE_SEPERATOR);
    string originalString;

#define COMPOSE_ORIGINALSTRING(_xxxClause, XXX_CLAUSE)                  \
    if (_xxxClause) {                                                   \
        if (!originalString.empty()) {                                  \
            originalString += clause_seperator;                         \
        }                                                               \
        originalString += XXX_CLAUSE + kv_seperator + _xxxClause->getOriginalString(); \
    }

    COMPOSE_ORIGINALSTRING(_configClause, CONFIG_CLAUSE);
    COMPOSE_ORIGINALSTRING(_aggregateClause, AGGREGATE_CLAUSE);
    COMPOSE_ORIGINALSTRING(_distinctClause, DISTINCT_CLAUSE);
    COMPOSE_ORIGINALSTRING(_filterClause, FILTER_CLAUSE);
    COMPOSE_ORIGINALSTRING(_pkFilterClause, PKFILTER_CLAUSE);
    COMPOSE_ORIGINALSTRING(_queryClause, QUERY_CLAUSE);
    COMPOSE_ORIGINALSTRING(_rankClause, RANK_CLAUSE);
    COMPOSE_ORIGINALSTRING(_sortClause, SORT_CLAUSE);
    COMPOSE_ORIGINALSTRING(_clusterClause, CLUSTER_CLAUSE);
    COMPOSE_ORIGINALSTRING(_healthCheckClause, HEALTHCHECK_CLAUSE);
    COMPOSE_ORIGINALSTRING(_attributeClause, ATTRIBUTE_CLAUSE);
    COMPOSE_ORIGINALSTRING(_virtualAttributeClause, VIRTUALATTRIBUTE_CLAUSE);
    COMPOSE_ORIGINALSTRING(_fetchSummaryClause, FETCHSUMMARY_CLAUSE);
    COMPOSE_ORIGINALSTRING(_kvPairClause, KVPAIR_CLAUSE);
    COMPOSE_ORIGINALSTRING(_queryLayerClause, QUERYLAYER_CLAUSE);
    COMPOSE_ORIGINALSTRING(_searcherCacheClause, SEARCHERCACHE_CLAUSE);
    COMPOSE_ORIGINALSTRING(_optimizerClause, OPTIMIZER_CLAUSE);
    COMPOSE_ORIGINALSTRING(_rankSortClause, RANKSORT_CLAUSE);
    COMPOSE_ORIGINALSTRING(_finalSortClause, FINALSORT_CLAUSE);
    COMPOSE_ORIGINALSTRING(_levelClause, LEVEL_CLAUSE);
    COMPOSE_ORIGINALSTRING(_analyzerClause, ANALYZER_CLAUSE);
    COMPOSE_ORIGINALSTRING(_auxQueryClause, AUX_QUERY_CLAUSE);
    COMPOSE_ORIGINALSTRING(_auxFilterClause, AUX_FILTER_CLAUSE);

    vector<string>::const_iterator it = _unknownClause.begin();
    if (it != _unknownClause.end() ) {
        if (!originalString.empty()) {
            originalString += clause_seperator;
        }
        originalString += *it;
        ++it;
    }
    for (; it != _unknownClause.end(); ++it) {
        originalString += (clause_seperator + (*it));
    }

    return originalString;
}

void Request::setPartitionMode(const string& partitionMode) {
    _partitionMode = partitionMode;
}

const string& Request::getPartitionMode() const {
    return _partitionMode;
}

#define REQUEST_MEMBERS(HA3_SERIALIZE_MACRO)        \
    HA3_SERIALIZE_MACRO(_configClause)              \
    HA3_SERIALIZE_MACRO(_queryClause)               \
    HA3_SERIALIZE_MACRO(_filterClause)              \
    HA3_SERIALIZE_MACRO(_pkFilterClause)            \
    HA3_SERIALIZE_MACRO(_sortClause)                \
    HA3_SERIALIZE_MACRO(_distinctClause)            \
    HA3_SERIALIZE_MACRO(_aggregateClause)        \
    HA3_SERIALIZE_MACRO(_rankClause)                \
    HA3_SERIALIZE_MACRO(_fetchSummaryClause)        \
    HA3_SERIALIZE_MACRO(_docIdClause)               \
    HA3_SERIALIZE_MACRO(_clusterClause)             \
    HA3_SERIALIZE_MACRO(_healthCheckClause)         \
    HA3_SERIALIZE_MACRO(_attributeClause)           \
    HA3_SERIALIZE_MACRO(_virtualAttributeClause)    \
    HA3_SERIALIZE_MACRO(_partitionMode)             \
    HA3_SERIALIZE_MACRO(_queryLayerClause)          \
    HA3_SERIALIZE_MACRO(_searcherCacheClause)       \
    HA3_SERIALIZE_MACRO(_optimizerClause)           \
    HA3_SERIALIZE_MACRO(_rankSortClause)            \
    HA3_SERIALIZE_MACRO(_finalSortClause)

void Request::serialize(autil::DataBuffer &dataBuffer) const {
    REQUEST_MEMBERS(HA3_DATABUFFER_WRITE);
    DataBuffer *auxQuerySection =
        dataBuffer.declareSectionBuffer(AUX_QUERY_DATABUFFER_SECTION);
    assert(auxQuerySection);
    auxQuerySection->write(_auxQueryClause);
    auxQuerySection->write(_auxFilterClause);
}

void Request::serializeWithoutAuxQuery(autil::DataBuffer &dataBuffer) const {
    REQUEST_MEMBERS(HA3_DATABUFFER_WRITE);
}

void Request::deserialize(autil::DataBuffer &dataBuffer) {
    REQUEST_MEMBERS(HA3_DATABUFFER_READ);
    DataBuffer *auxQuerySection =
        dataBuffer.findSectionBuffer(AUX_QUERY_DATABUFFER_SECTION);
    if (auxQuerySection) {
        auxQuerySection->read(_auxQueryClause);
        auxQuerySection->read(_auxFilterClause);
    }
}

void Request::serializeToString(string &str) const {
    autil::DataBuffer dataBuffer;
    serialize(dataBuffer);
    dataBuffer.serializeToStringWithSection(str);
}

void Request::deserializeFromString(const string &str,
                                    autil::mem_pool::Pool *pool)
{
    autil::DataBuffer dataBuffer(NULL, 0, NULL);
    dataBuffer.deserializeFromStringWithSection(str, pool);
    deserialize(dataBuffer);
}

void Request::splitClause(const string& oriStr)
{
    HA3_LOG(DEBUG, "split clause. request string[%s]", oriStr.c_str());

    if (oriStr == "") {
        return;
    }

    StringTokenizer st(oriStr, CLAUSE_SEPERATOR,
                       StringTokenizer::TOKEN_IGNORE_EMPTY |
                       StringTokenizer::TOKEN_TRIM);
    size_t numTokens = st.getNumTokens();
    if (numTokens < 1) {
        _unknownClause.push_back(oriStr);
        return;
    }

    for (size_t i = 0; i < numTokens; ++i) {
        size_t equalPos = st[i].find(CLAUSE_KV_SEPERATOR);
        if (equalPos == string::npos) {
            HA3_LOG(WARN, "Missing '=' in sub-clause[%s] in [%s]", st[i].c_str(),
                    oriStr.c_str());
            _unknownClause.push_back(st[i]);
            continue;
        }
        string clauseKey = st[i].substr(0, equalPos);
        string clauseValue = st[i].substr(equalPos + strlen(CLAUSE_KV_SEPERATOR));
        autil::StringUtil::trim(clauseKey);
        autil::StringUtil::trim(clauseValue);
        if (!setClauseString(clauseKey, clauseValue)){
            _unknownClause.push_back(st[i]);
            continue;
        }
    }

    // make sure the configclause be created, if not specified in request,
    // use the default.
    ConfigClause* configClause = getConfigClause();
    if (configClause == NULL) {
        setConfigClauseString("");
    }
}

bool Request::setClauseString(const string& clauseName, const string& clauseString) {
#define SETCLAUSESTRING_HELPER(XxxClause, XXX_CLAUSE)   \
    if (clauseName == XXX_CLAUSE) {                     \
        set##XxxClause##String(clauseString);           \
        return true;                                    \
    }

    SETCLAUSESTRING_HELPER(ConfigClause, CONFIG_CLAUSE);
    SETCLAUSESTRING_HELPER(QueryClause, QUERY_CLAUSE);
    SETCLAUSESTRING_HELPER(FilterClause, FILTER_CLAUSE);
    SETCLAUSESTRING_HELPER(PKFilterClause, PKFILTER_CLAUSE);
    SETCLAUSESTRING_HELPER(SortClause, SORT_CLAUSE);
    SETCLAUSESTRING_HELPER(DistinctClause, DISTINCT_CLAUSE);
    SETCLAUSESTRING_HELPER(AggregateClause, AGGREGATE_CLAUSE);
    SETCLAUSESTRING_HELPER(RankClause, RANK_CLAUSE);
    SETCLAUSESTRING_HELPER(ClusterClause, CLUSTER_CLAUSE);
    SETCLAUSESTRING_HELPER(HealthCheckClause, HEALTHCHECK_CLAUSE);
    SETCLAUSESTRING_HELPER(AttributeClause, ATTRIBUTE_CLAUSE);
    SETCLAUSESTRING_HELPER(VirtualAttributeClause, VIRTUALATTRIBUTE_CLAUSE);
    SETCLAUSESTRING_HELPER(FetchSummaryClause, FETCHSUMMARY_CLAUSE);
    SETCLAUSESTRING_HELPER(KVPairClause, KVPAIR_CLAUSE);
    SETCLAUSESTRING_HELPER(QueryLayerClause, QUERYLAYER_CLAUSE);
    SETCLAUSESTRING_HELPER(SearcherCacheClause, SEARCHERCACHE_CLAUSE);
    SETCLAUSESTRING_HELPER(OptimizerClause, OPTIMIZER_CLAUSE);
    SETCLAUSESTRING_HELPER(RankSortClause, RANKSORT_CLAUSE);
    SETCLAUSESTRING_HELPER(FinalSortClause, FINALSORT_CLAUSE);
    SETCLAUSESTRING_HELPER(LevelClause, LEVEL_CLAUSE);
    SETCLAUSESTRING_HELPER(AnalyzerClause, ANALYZER_CLAUSE);
    SETCLAUSESTRING_HELPER(AuxQueryClause, AUX_QUERY_CLAUSE);
    SETCLAUSESTRING_HELPER(AuxFilterClause, AUX_FILTER_CLAUSE);
    return false;
}

#define SET_CLAUSE_ORISTRING(ClauseClass)                               \
    void Request::set##ClauseClass##String(const string &clauseString)  \
    {                                                                   \
        ClauseClass *clause = new ClauseClass();                        \
        clause->setOriginalString(clauseString);                        \
        set##ClauseClass(clause);                                       \
    }

SET_CLAUSE_ORISTRING(ConfigClause);
SET_CLAUSE_ORISTRING(QueryClause);
SET_CLAUSE_ORISTRING(FilterClause);
SET_CLAUSE_ORISTRING(SortClause);
SET_CLAUSE_ORISTRING(PKFilterClause);
SET_CLAUSE_ORISTRING(DistinctClause);
SET_CLAUSE_ORISTRING(RankClause);
SET_CLAUSE_ORISTRING(AggregateClause);
SET_CLAUSE_ORISTRING(ClusterClause);
SET_CLAUSE_ORISTRING(HealthCheckClause);
SET_CLAUSE_ORISTRING(AttributeClause);
SET_CLAUSE_ORISTRING(VirtualAttributeClause);
SET_CLAUSE_ORISTRING(FetchSummaryClause);
SET_CLAUSE_ORISTRING(KVPairClause);
SET_CLAUSE_ORISTRING(QueryLayerClause);
SET_CLAUSE_ORISTRING(SearcherCacheClause);
SET_CLAUSE_ORISTRING(OptimizerClause);
SET_CLAUSE_ORISTRING(RankSortClause);
SET_CLAUSE_ORISTRING(FinalSortClause);
SET_CLAUSE_ORISTRING(LevelClause);
SET_CLAUSE_ORISTRING(AnalyzerClause);
SET_CLAUSE_ORISTRING(AuxQueryClause);
SET_CLAUSE_ORISTRING(AuxFilterClause);


const vector<string>& Request::getUnknownClause() const{
    return _unknownClause;
}

#define CLAUSE_TO_STRING(member)                \
    if (_##member) {                                             \
        ss << #member << " : " << _##member->toString() << endl; \
    }
std::string Request::toString() const {
    stringstream ss;
    CLAUSE_TO_STRING(configClause);
    CLAUSE_TO_STRING(queryClause);
    CLAUSE_TO_STRING(filterClause);
    CLAUSE_TO_STRING(pkFilterClause);
    CLAUSE_TO_STRING(sortClause);
    CLAUSE_TO_STRING(distinctClause);
    CLAUSE_TO_STRING(aggregateClause);
    CLAUSE_TO_STRING(rankClause);
    CLAUSE_TO_STRING(docIdClause);
    CLAUSE_TO_STRING(clusterClause);
    CLAUSE_TO_STRING(healthCheckClause);
    CLAUSE_TO_STRING(attributeClause);
    CLAUSE_TO_STRING(virtualAttributeClause);
    CLAUSE_TO_STRING(fetchSummaryClause);
    CLAUSE_TO_STRING(kvPairClause);
    CLAUSE_TO_STRING(queryLayerClause);
    CLAUSE_TO_STRING(searcherCacheClause);
    CLAUSE_TO_STRING(optimizerClause);
    CLAUSE_TO_STRING(rankSortClause);
    CLAUSE_TO_STRING(finalSortClause);
    CLAUSE_TO_STRING(levelClause);
    CLAUSE_TO_STRING(analyzerClause);
    CLAUSE_TO_STRING(auxQueryClause);
    CLAUSE_TO_STRING(auxFilterClause);
    return ss.str();
}

void Request::setDegradeLevel(float level, uint32_t rankSize, uint32_t rerankSize) {
    _degradeLevel = level;
    _rankSize = rankSize;
    _rerankSize = rerankSize;
}
void Request::getDegradeLevel(float &level, uint32_t &rankSize, uint32_t &rerankSize) const {
    level = _degradeLevel;
    rankSize = _rankSize;
    rerankSize = _rerankSize;
}

Request* Request::clone(autil::mem_pool::Pool *pool) {
    Request *request = new Request(pool);
    request->_configClause = cloneConfigClause();
    request->_queryClause = cloneQueryClause();
    request->_filterClause = cloneFilterClause();
    request->_pkFilterClause = clonePKFilterClause();
    request->_sortClause = cloneSortClause();
    request->_distinctClause = cloneDistinctClause();
    request->_aggregateClause = cloneAggregateClause();
    request->_rankClause = cloneRankClause();
    request->_docIdClause = cloneDocIdClause();
    request->_clusterClause = cloneClusterClause();
    request->_healthCheckClause = cloneHealthCheckClause();
    request->_attributeClause = cloneAttributeClause();
    request->_virtualAttributeClause = cloneVirtualAttributeClause();
    request->_fetchSummaryClause = cloneFetchSummaryClause();
    if (_kvPairClause) {
        request->_kvPairClause = new KVPairClause();
        request->_kvPairClause->setOriginalString(
                _kvPairClause->getOriginalString());
    }
    request->_queryLayerClause = cloneQueryLayerClause();
    request->_searcherCacheClause = cloneSearcherCacheClause();
    request->_optimizerClause = cloneOptimizerClause();
    request->_rankSortClause = cloneRankSortClause();
    request->_finalSortClause = cloneFinalSortClause();
    request->_levelClause = cloneLevelClause();
    request->_analyzerClause = cloneAnalyzerClause();
    request->_auxQueryClause = cloneAuxQueryClause();
    request->_auxFilterClause = cloneAuxFilterClause();

    request->_partitionMode = _partitionMode;
    request->_unknownClause = _unknownClause;
    // used in proxy
    request->_curClusterName = _curClusterName;
    request->_rowKey = _rowKey;
    // searcher degrade
    request->_degradeLevel = _degradeLevel;
    request->_rankSize = _rankSize;
    request->_rerankSize = _rerankSize;
    return request;
}

END_HA3_NAMESPACE(common);
