#include <ha3/common/RequestCacheKeyGenerator.h>
#include <ha3/queryparser/RequestSymbolDefine.h>
#include <autil/HashFuncFactory.h>
#include <autil/HashFunctionBase.h>

using namespace std;
using namespace autil;
using namespace suez::turing;
BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, RequestCacheKeyGenerator);

static string BoolToString(bool flag) {
    if (flag) {
        return string("true");
    } else {
        return string("false");
    }
}

RequestCacheKeyGenerator::RequestCacheKeyGenerator(const RequestPtr &requestPtr) 
{
#define GENERATE_CLAUSE_KVMAPS_HELPER(ClauseType, clauseName)           \
    {                                                                   \
        auto clause = requestPtr->get##ClauseType();                    \
        if (clause) {                                                   \
            _clauseCacheStrs[clauseName] = clause->toString();          \
        }                                                               \
    }
    GENERATE_CLAUSE_KVMAPS_HELPER(QueryClause, QUERY_CLAUSE);
    GENERATE_CLAUSE_KVMAPS_HELPER(FilterClause, FILTER_CLAUSE);
    GENERATE_CLAUSE_KVMAPS_HELPER(PKFilterClause, PKFILTER_CLAUSE);
    GENERATE_CLAUSE_KVMAPS_HELPER(SortClause, SORT_CLAUSE);
    GENERATE_CLAUSE_KVMAPS_HELPER(DistinctClause, DISTINCT_CLAUSE);    
    GENERATE_CLAUSE_KVMAPS_HELPER(AggregateClause, AGGREGATE_CLAUSE);
    GENERATE_CLAUSE_KVMAPS_HELPER(RankClause, RANK_CLAUSE);
    GENERATE_CLAUSE_KVMAPS_HELPER(HealthCheckClause, HEALTHCHECK_CLAUSE);
    GENERATE_CLAUSE_KVMAPS_HELPER(AttributeClause, ATTRIBUTE_CLAUSE);
    GENERATE_CLAUSE_KVMAPS_HELPER(QueryLayerClause, QUERYLAYER_CLAUSE);
    GENERATE_CLAUSE_KVMAPS_HELPER(OptimizerClause, OPTIMIZER_CLAUSE);
    GENERATE_CLAUSE_KVMAPS_HELPER(RankSortClause, RANKSORT_CLAUSE);
    GENERATE_CLAUSE_KVMAPS_HELPER(FinalSortClause, FINALSORT_CLAUSE);
#undef GENERATE_CLAUSE_KVMAPS_HELPER

    convertConfigClauseToKVMap(requestPtr->getConfigClause());
    convertKVClauseToKVMap(requestPtr->getConfigClause());
    convertVirtualAttributeClauseToKVMap(
            requestPtr->getVirtualAttributeClause());
}

RequestCacheKeyGenerator::~RequestCacheKeyGenerator() { 
}

uint64_t RequestCacheKeyGenerator::generateRequestCacheKey() const {
    return autil::HashAlgorithm::hashString64(
            generateRequestCacheString().c_str());
}

string RequestCacheKeyGenerator::generateRequestCacheString() const {
    string cacheStr;
    for (map<string, ClauseOptionKVMap>::const_iterator it1 = _clauseKVMaps.begin(); 
         it1 != _clauseKVMaps.end(); ++it1)
    {
        cacheStr.append(it1->first);
        cacheStr.append("={");
        for (ClauseOptionKVMap::const_iterator it2 = it1->second.begin(); 
             it2 != it1->second.end(); ++it2)
        {
            cacheStr.append("[");
            cacheStr.append(it2->first);
            cacheStr.append(":");
            cacheStr.append(it2->second);
            cacheStr.append("]");
        }
        cacheStr.append("};");            
    }

    for (map<string, string>::const_iterator it = _clauseCacheStrs.begin(); 
         it != _clauseCacheStrs.end(); ++it)
    {
        cacheStr.append(it->first);
        cacheStr.append("={");
        cacheStr.append(it->second);        
        cacheStr.append("};");
    }
    return cacheStr;
}

void RequestCacheKeyGenerator::disableOption(const string &clauseName, 
        const string &optionName) 
{
    map<string, ClauseOptionKVMap>::iterator it1 =  
        _clauseKVMaps.find(clauseName);
    if (_clauseKVMaps.end() == it1) {
        return;
    }
    
    ClauseOptionKVMap::iterator it2 = it1->second.find(optionName);
    if (it1->second.end() == it2) {
        return;
    }
    it1->second.erase(it2);
}

void RequestCacheKeyGenerator::disableClause(const string &clauseName) {
    map<string, ClauseOptionKVMap>::iterator it1 =  
        _clauseKVMaps.find(clauseName);
    if (_clauseKVMaps.end() != it1) {
        _clauseKVMaps.erase(it1);
        return;
    }

    map<string, string>::iterator it2 = _clauseCacheStrs.find(clauseName);
    if (_clauseCacheStrs.end() != it2) {
        _clauseCacheStrs.erase(it2);
    }
}

bool RequestCacheKeyGenerator::setClauseOption(const string &clauseName, 
        const string &optionName,
        const string &optionValue)
{
    map<string, ClauseOptionKVMap>::iterator it1 =  
        _clauseKVMaps.find(clauseName);
    if (_clauseKVMaps.end() == it1) {
        return false;
    }
    
    it1->second[optionName] = optionValue;
    return true;
}

string RequestCacheKeyGenerator::getClauseOption(const string &clauseName, 
        const string &optionName) const 
{
    map<string, ClauseOptionKVMap>::const_iterator it1 =  
        _clauseKVMaps.find(clauseName);
    if (_clauseKVMaps.end() == it1) {
        return string();
    }
    
    ClauseOptionKVMap::const_iterator it2 = it1->second.find(optionName);
    if (it1->second.end() == it2) {
        return string();
    }
    
    return it2->second;
}

void RequestCacheKeyGenerator::convertConfigClauseToKVMap(
        const ConfigClause *configClause) 
{
    if (NULL == configClause) {
        return;
    }
    ClauseOptionKVMap& kvMap = _clauseKVMaps[CONFIG_CLAUSE];
    kvMap[CONFIG_CLAUSE_KEY_RANKTRACE] = configClause->getRankTrace();
    kvMap[CONFIG_CLAUSE_KEY_ANALYZER] = StringUtil::toString(
            configClause->getAnalyzerName());
    kvMap[CONFIG_CLAUSE_KEY_RANKSIZE] = StringUtil::toString(
            configClause->getRankSize());
    kvMap[CONFIG_CLAUSE_KEY_RERANKSIZE] = StringUtil::toString(
            configClause->getRerankSize());
    kvMap[CONFIG_CLAUSE_KEY_BATCH_SCORE] = 
        BoolToString(configClause->isBatchScore());
    kvMap[CONFIG_CLAUSE_KEY_OPTIMIZE_RANK] = 
        BoolToString(configClause->isOptimizeRank());
    kvMap[CONFIG_CLAUSE_KEY_OPTIMIZE_COMPARATOR] = 
        BoolToString(configClause->isOptimizeComparator());
    kvMap[CONFIG_CLAUSE_KEY_DEBUG_QUERY_KEY] = configClause->getDebugQueryKey();
    kvMap[CONFIG_CLAUSE_SUB_DOC_DISPLAY_TYPE] = StringUtil::toString(
            (int32_t)configClause->getSubDocDisplayType());
    kvMap[CONFIG_CLAUSE_KEY_FETCH_SUMMARY_TYPE] = StringUtil::toString(
            configClause->getFetchSummaryType());

    string noTokenizeIndexStr;
    const set<string>& noTokenizeIndexes = 
        configClause->getNoTokenizeIndexes();
    set<string>::const_iterator iter = noTokenizeIndexes.begin();
    for (; iter != noTokenizeIndexes.end(); iter++) 
    {
        noTokenizeIndexStr.append(*iter);
        noTokenizeIndexStr.append("|");
    }

    kvMap[CONFIG_CLAUSE_NO_TOKENIZE_INDEXES] = noTokenizeIndexStr;

    string specificIndexAnalyzerStr;
    const ConfigClause::IndexAnalyzerMap& indexAnalyzerMap = configClause->getIndexAnalyzerMap();
    for (ConfigClause::IndexAnalyzerMap::const_iterator it = indexAnalyzerMap.begin();
         it != indexAnalyzerMap.end(); ++it) 
    {
        specificIndexAnalyzerStr.append(it->first);
        specificIndexAnalyzerStr.append("#");
        specificIndexAnalyzerStr.append(it->second);
        specificIndexAnalyzerStr.append("|");
    }

    kvMap[ANALYZER_CLAUSE_SPECIFIC_INDEX_ANALYZER] = specificIndexAnalyzerStr;
}

void RequestCacheKeyGenerator::convertKVClauseToKVMap(
        const ConfigClause *configClause) 
{    
    if (NULL == configClause) {
        return;
    }
    const map<string, string>& kvPairs = configClause->getKVPairs();
    if (kvPairs.empty()) {
        return;
    }
    _clauseKVMaps[KVPAIR_CLAUSE] = kvPairs;
}

void RequestCacheKeyGenerator::convertVirtualAttributeClauseToKVMap(
        const VirtualAttributeClause *vaClause) 
{
    if (NULL == vaClause) {
        return;
    }
        
    const vector<VirtualAttribute*>& virtualAttributes = 
        vaClause->getVirtualAttributes();
    ClauseOptionKVMap& kvMap = _clauseKVMaps[VIRTUALATTRIBUTE_CLAUSE];
    size_t vaCount = virtualAttributes.size();
    for (size_t i = 0; i < vaCount; i++) {
        string name = virtualAttributes[i]->getVirtualAttributeName();
        string exprStr = virtualAttributes[i]->getExprString();
        kvMap[name] = exprStr;
    }
}

END_HA3_NAMESPACE(common);

