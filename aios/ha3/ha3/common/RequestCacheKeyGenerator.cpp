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
#include "ha3/common/RequestCacheKeyGenerator.h"

#include <cstddef>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "autil/HashAlgorithm.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "ha3/common/AggregateClause.h"
#include "ha3/common/AttributeClause.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/DistinctClause.h"
#include "ha3/common/FilterClause.h"
#include "ha3/common/FinalSortClause.h"
#include "ha3/common/HealthCheckClause.h"
#include "ha3/common/OptimizerClause.h"
#include "ha3/common/PKFilterClause.h"
#include "ha3/common/QueryClause.h"
#include "ha3/common/QueryLayerClause.h"
#include "ha3/common/RankClause.h"
#include "ha3/common/RankSortClause.h"
#include "ha3/common/Request.h"
#include "ha3/common/RequestSymbolDefine.h"
#include "ha3/common/SortClause.h"
#include "ha3/common/VirtualAttributeClause.h"
#include "suez/turing/expression/util/VirtualAttribute.h"

using namespace std;
using namespace autil;
using namespace suez::turing;
namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, RequestCacheKeyGenerator);

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

} // namespace common
} // namespace isearch

