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
#include "ha3/sorter/DefaultSorter.h"

#include <assert.h>
#include <stdint.h>
#include <algorithm>
#include <cstddef>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringTokenizer.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/PoolVector.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/plugin/Sorter.h"
#include "suez/turing/expression/provider/SorterProvider.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"

#include "ha3/common/CommonDef.h"
#include "ha3/common/DistinctClause.h"
#include "ha3/common/DistinctDescription.h"
#include "ha3/common/FilterClause.h"
#include "ha3/common/FinalSortClause.h"
#include "ha3/common/RankSortClause.h"
#include "ha3/common/RankSortDescription.h"
#include "ha3/common/Request.h"
#include "ha3/common/SearcherCacheClause.h"
#include "ha3/common/SortClause.h"
#include "ha3/common/SortDescription.h"
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/common/RequestSymbolDefine.h"
#include "ha3/rank/ComboComparator.h"
#include "ha3/rank/Comparator.h"
#include "ha3/rank/DistinctHitCollector.h"
#include "ha3/rank/DistinctInfo.h"
#include "ha3/rank/ReferenceComparator.h"
#include "ha3/search/Filter.h"
#include "ha3/sorter/SorterProvider.h"
#include "ha3/sorter/SorterResource.h"
#include "autil/Log.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace isearch::common;
using namespace isearch::search;
using namespace isearch::rank;

namespace isearch {
namespace sorter {
AUTIL_LOG_SETUP(ha3, DefaultSorter);

const static string SORT_KEY = "sort_key";

DefaultSorter::DefaultSorter()
    : Sorter("DEFAULT")
    , _type(DS_UNKNOWN)
    , _cmp(NULL)
    , _needSort(true)
{ 

}

DefaultSorter::~DefaultSorter() { 
    POOL_DELETE_CLASS(_cmp);
}

bool DefaultSorter::needSort(SorterProvider *provider) const {
    if (provider->getResultSourceNum() == 1 && provider->getLocation() != SL_SEARCHER) {
        if (provider->getLocation() == SL_SEARCHCACHEMERGER) {
            const Request *request = provider->getRequest();   
            SearcherCacheClause *searcherCacheClause = request->getSearcherCacheClause();
            if (searcherCacheClause) {
                const set<string> &refreshAttrNames = 
                    searcherCacheClause->getRefreshAttributes();
                if (refreshAttrNames.empty()) {
                    return false;
                }
            }
        } else {
            return false;
        }
    }
    return true;
}

bool DefaultSorter::beginSort(suez::turing::SorterProvider *suezProvider) {
    auto provider = dynamic_cast<SorterProvider *>(suezProvider);
    assert(provider);
    auto request = provider->getRequest();
    assert(request);

    REQUEST_TRACE_WITH_TRACER(provider->getSorterResource().requestTracer,
                              TRACE1, "begin default sort");
    // init sort expression first, as merger need to use SortExprMeta
    SortExpressionVector sortExpressions;
    if (!initSortExpressions(provider, sortExpressions)) {
        string errorMsg = "init sort expression failed.";
        AUTIL_LOG(WARN, "%s", errorMsg.c_str());
        provider->setErrorMessage(errorMsg);
        return false;
    }
    serializeSortExprRef(sortExpressions);

    if (!needSort(provider)) {
        _needSort = false;
        return true;
    }

    initComparator(provider, sortExpressions);
    DistinctClause *distinctClause = request->getDistinctClause();

    if (distinctClause) {
        uint32_t distDescCount = distinctClause->getDistinctDescriptionsCount();
        DistinctDescription *distDesc = distDescCount == 2 ? 
                                        distinctClause->getDistinctDescription(1) :
                                        distinctClause->getDistinctDescription(0);
        if (!distDesc) {
            _type = DS_NORMAL_SORT;
        } else {
            _type = DS_DISTINCT;
            if (!prepareForDistinctHitCollector(distDesc, provider, sortExpressions[0]))
            {
                return false;
            }
        }
    } else {
        _type = DS_NORMAL_SORT;
    }
    return true;
}

void DefaultSorter::sort(suez::turing::SortParam &sortParam) {
    if (!_needSort) {
        return;
    }
    if (sortParam.requiredTopK == 0) {
        return;
    }
    PoolVector<matchdoc::MatchDoc> &matchDocs = sortParam.matchDocs;
    if (_type == DS_NORMAL_SORT) {
        MatchDocComp matchDocCmp(_cmp);
        uint32_t needSortCount = min(sortParam.requiredTopK, (uint32_t)matchDocs.size());
        std::partial_sort(matchDocs.begin(), matchDocs.begin() + needSortCount, 
                          matchDocs.end(), matchDocCmp);
    } else if (_type == DS_DISTINCT) {
        SortExpression *sortExpr = _distinctCollectorParam.sortExpr;
        DistinctHitCollector collector(sortParam.requiredTopK,
                _distinctCollectorParam.pool,
                _distinctCollectorParam.allocator,
                sortExpr->getAttributeExpression(), 
                _cmp, _distinctCollectorParam.dp, sortExpr->getSortFlag());
        collector.doCollect(&matchDocs[0], matchDocs.size());
        uint32_t count = collector.getItemCount();
        matchDocs.resize(count);
        for (int32_t i = count - 1; i >= 0; --i) {
            matchDocs[i] = collector.popItem();
        }
        sortParam.totalMatchCount -= collector.getDeletedDocCount();
        sortParam.actualMatchCount -= collector.getDeletedDocCount();
        _cmp = NULL; // avoid double free
    } else {
        assert(false);
    }
}

void DefaultSorter::initComparator(SorterProvider *provider, 
                                   const SortExpressionVector &sortExpressions) 
{
    ComboComparator *comboCmp = provider->createSortComparator(sortExpressions);
    if (comboCmp) {
        matchdoc::Reference<hashid_t> *hashIdRef =
            provider->findVariableReferenceWithRawName<hashid_t>(common::HASH_ID_REF);
        if (hashIdRef) {
            ReferenceComparator<hashid_t>* cmp = POOL_NEW_CLASS(provider->getSessionPool(),
                    ReferenceComparator<hashid_t>, hashIdRef);
            comboCmp->setExtrHashIdComparator(cmp);
        }
        matchdoc::Reference<clusterid_t> *clusterIdRef =
            provider->findVariableReferenceWithRawName<clusterid_t>(common::CLUSTER_ID_REF);
        if (clusterIdRef) {
            ReferenceComparator<clusterid_t>* cmp = POOL_NEW_CLASS(provider->getSessionPool(),
                    ReferenceComparator<clusterid_t>, clusterIdRef);
            comboCmp->setExtrClusterIdComparator(cmp);
        }
    }
    _cmp = comboCmp;
}

bool DefaultSorter::initSortExpressions(SorterProvider *provider,
                                        SortExpressionVector &sortExpressions)
{
    sortExpressions.clear();
    const Request *request = provider->getRequest();
    const FinalSortClause *finalSortClause = request->getFinalSortClause();

    if (finalSortClause) {
        const KeyValueMap &kvMap = finalSortClause->getParams();
        KeyValueMap::const_iterator iter = kvMap.find(SORT_KEY);
        if (iter != kvMap.end()) {
            if (!initSortExpressionsFromSortKey(iter->second, sortExpressions, provider)) {
                AUTIL_LOG(WARN, "init sortExpression from SortKey[%s] failed.", iter->second.c_str());
                return false;
            }
            return true;
        }
    }

    RankSortClause *rankSortClause = request->getRankSortClause();
    SortClause *sortClause = request->getSortClause();
    vector<SortDescription*> sortDescs;
    if (sortClause) {
        sortDescs = sortClause->getSortDescriptions();
    } else if (rankSortClause) {
        assert(rankSortClause->getRankSortDescCount() == 1);
        sortDescs = rankSortClause->getRankSortDesc(0)->getSortDescriptions();
    }  else { //both rankSortClause and sortClause are empty
        auto attrExpr = provider->getScoreExpression();
        if (!attrExpr) {
            string errorMsg = "get score expression failed.";
            AUTIL_LOG(WARN, "%s", errorMsg.c_str());
            provider->setErrorMessage(errorMsg);
            return false;
        }
        SortExpression *sortExpr = provider->createSortExpression(attrExpr, false);
        sortExpressions.push_back(sortExpr);
        addSortExprMeta("RANK", sortExpr->getReferenceBase(), false, provider);
        return true;
    }
    for (size_t i = 0; i < sortDescs.size(); ++i) {
        suez::turing::AttributeExpression *expr = NULL;
        if (sortDescs[i]->isRankExpression()) {
            expr = provider->getScoreExpression();
        } else {
            auto syntaxExpr = sortDescs[i]->getRootSyntaxExpr();
            expr = provider->createAttributeExpression(syntaxExpr);
        }
        if (!expr) {
            string errorMsg = "get attribute expression for [" + 
                              sortDescs[i]->getOriginalString() + "] failed.";
            AUTIL_LOG(WARN, "%s", errorMsg.c_str());
            provider->setErrorMessage(errorMsg);
            return false;
        }
        SortExpression *sortExpr = provider->createSortExpression(
                expr, sortDescs[i]->getSortAscendFlag());
        sortExpressions.push_back(sortExpr);
        string sortFieldName = sortDescs[i]->getOriginalString();
        if (sortFieldName[0] == SORT_CLAUSE_DESCEND
            || sortFieldName[0] == SORT_CLAUSE_ASCEND) {
            sortFieldName = sortFieldName.substr(1);
        }
        addSortExprMeta(sortFieldName,
                        sortExpr->getReferenceBase(),
                        sortDescs[i]->getSortAscendFlag(), provider);
    }
    return true;
}

bool DefaultSorter::prepareForDistinctHitCollector(
        DistinctDescription *distDesc, SorterProvider *provider, 
        SortExpression *expr)
{
    _distinctCollectorParam.pool = provider->getSessionPool();
    _distinctCollectorParam.allocator = provider->getMatchDocAllocator();
    _distinctCollectorParam.sortExpr = expr;
    DistinctParameter &dp = _distinctCollectorParam.dp;

    const suez::turing::SyntaxExpr *syntaxExpr = distDesc->getRootSyntaxExpr();
    auto keyAttribute = provider->createAttributeExpression(syntaxExpr);
    if (!keyAttribute) {
        string errorMsg = "create distinct key expression[" + 
                          syntaxExpr->getExprString() +  "] failed.";
        AUTIL_LOG(WARN, "%s", errorMsg.c_str());
        provider->setErrorMessage(errorMsg);
        return false;
    }
    dp.keyExpression = provider->createSortExpression(keyAttribute, false);
    // auto referBase = dp.keyExpression->getReferenceBase();
    // referBase->addGroupItem(FOR_DISTINCT);
    dp.distinctTimes = distDesc->getDistinctTimes();
    dp.distinctCount = distDesc->getDistinctCount();
    dp.maxItemCount = distDesc->getMaxItemCount();
    dp.reservedFlag = distDesc->getReservedFlag();
    dp.updateTotalHitFlag = distDesc->getUpdateTotalHitFlag();
    dp.gradeThresholds = distDesc->getGradeThresholds();

    dp.distinctInfoRef = provider->getMatchDocAllocator()->declare<DistinctInfo>(
            DISTINCT_INFO_REF);
    if (!dp.distinctInfoRef) {
        string errorMsg = "declare distinct info failed.";
        AUTIL_LOG(WARN, "%s", errorMsg.c_str());
        provider->setErrorMessage(errorMsg);
        return false;
    }
    const FilterClause *filterClause = distDesc->getFilterClause();
    if (filterClause) {
        syntaxExpr = filterClause->getRootSyntaxExpr();
        auto attrExpr = provider->createAttributeExpression(syntaxExpr);
        if (!expr) {
            string errorMsg = "create distinct filter[" + syntaxExpr->getExprString() + "] failed.";
            AUTIL_LOG(WARN, "%s", errorMsg.c_str());
            provider->setErrorMessage(errorMsg);
            return false;
        }
        auto boolAttrExpr = dynamic_cast<suez::turing::AttributeExpressionTyped<bool>*>(attrExpr);
        if (!boolAttrExpr) {
            string errorMsg = "filter expression[" + 
                              syntaxExpr->getExprString() + "] return type should be bool.";
            AUTIL_LOG(WARN, "%s", errorMsg.c_str());
            provider->setErrorMessage(errorMsg);
            return false;
        }
        dp.distinctFilter = POOL_NEW_CLASS(_distinctCollectorParam.pool, 
                Filter, boolAttrExpr);
    }

    return true;
}

bool DefaultSorter::initSortExpressionsFromSortKey(const string &sortKeyDesc, 
        SortExpressionVector &sortExpressions,
        SorterProvider *provider)
{
    StringTokenizer sortFields(sortKeyDesc, "|",
                               StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    uint32_t fieldNum = sortFields.getNumTokens();
    for (uint32_t i  = 0; i < fieldNum; i++) {
        string sortField = sortFields[i];
        bool sortFlag = false;
        if (sortField[0] == SORT_CLAUSE_ASCEND) {
            sortField = sortField.substr(1);
            sortFlag = true;
        } else if (sortField[0] == SORT_CLAUSE_DESCEND) {
            sortField = sortField.substr(1);
        }
        SortExpression *sortExpression = provider->createSortExpression(sortField, sortFlag);
        if (!sortExpression) {
            string errorMsg = "create score expression from " + sortField + " failed.";
            AUTIL_LOG(WARN, "%s", errorMsg.c_str());
            provider->setErrorMessage(errorMsg);
            return false;
        }
        auto attributeExpr = sortExpression->getAttributeExpression();
        if (attributeExpr->isMultiValue()) {
            AUTIL_LOG(WARN, "%s is multi value attribute, cannot be used in sort key!", 
                    sortField.c_str());
            return false;
        }
        auto referBase = attributeExpr->getReferenceBase();
        if (referBase->getSerializeLevel() < SL_CACHE) {
            referBase->setSerializeLevel(SL_CACHE);
        }
        sortExpressions.push_back(sortExpression);
        addSortExprMeta(sortField, referBase, sortFlag, provider);
    }
    return true;
}

void DefaultSorter::addSortExprMeta(const string &name,
                                    matchdoc::ReferenceBase* sortRef,
                                    bool sortFlag, SorterProvider *provider)
{
    assert(provider);
    provider->addSortExprMeta(name, sortRef, sortFlag);
}

void DefaultSorter::serializeSortExprRef(
        const SortExpressionVector &sortExpressions)
{
    for (uint32_t i = 0; i < sortExpressions.size(); ++i) {
        matchdoc::ReferenceBase* refer = sortExpressions[i]->getReferenceBase();
        if (refer->getSerializeLevel() < SL_CACHE) {
            refer->setSerializeLevel(SL_CACHE);
        }
    }
}

bool DefaultSorter::needScoreInSort(const common::Request *request) const {
    FinalSortClause *finalSortClause = request->getFinalSortClause();
    if (finalSortClause) {
        const KeyValueMap &kvMap = finalSortClause->getParams();
        KeyValueMap::const_iterator iter = kvMap.find(SORT_KEY);
        if (iter != kvMap.end()) {
            string sortKeyDesc = iter->second;
            StringTokenizer sortFields(sortKeyDesc, "|",
                    StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
            uint32_t fieldNum = sortFields.getNumTokens();
            for (uint32_t i  = 0; i < fieldNum; i++) {
                string sortField = sortFields[i];
                if (sortField[0] == SORT_CLAUSE_ASCEND) {
                    sortField = sortField.substr(1);
                } else if (sortField[0] == SORT_CLAUSE_DESCEND) {
                    sortField = sortField.substr(1);
                }
                if (sortField == SORT_CLAUSE_RANK) {
                    return true;
                }
            }
            return false;
        }
    }

    SortClause *sortClause = request->getSortClause();
    if (sortClause) {
        vector<SortDescription*> sortDescs = sortClause->getSortDescriptions(); 
        for (size_t i = 0; i < sortDescs.size(); i++) {
            if (sortDescs[i]->isRankExpression()) {
                return true;
            }
        }
        return false;
    }

    RankSortClause *rankSortClause = request->getRankSortClause();
    if (rankSortClause) {
        assert (rankSortClause->getRankSortDescCount() == 1);
        vector<SortDescription*> sortDescs = 
            rankSortClause->getRankSortDesc(0)->getSortDescriptions();
        for (size_t i = 0; i < sortDescs.size(); i++) {
            if (sortDescs[i]->isRankExpression()) {
                return true;
            }
        }
        return false;
    }
    return true;
}

bool DefaultSorter::validateSortInfo(const Request *request) const {
    FinalSortClause *finalSortClause = request->getFinalSortClause();
    RankSortClause *rankSortClause = request->getRankSortClause();
    SortClause *sortClause = request->getSortClause();
    if (finalSortClause) {
        const KeyValueMap &kvMap = finalSortClause->getParams();
        KeyValueMap::const_iterator iter = kvMap.find(SORT_KEY);
        if (iter != kvMap.end()) {
            return true;
        }
    }
    if (!sortClause && rankSortClause) {
        if (rankSortClause->getRankSortDescCount() > 1) {
            string errorMsg = "use multi dimension rank_sort clause,"
                              " without specify sort_key in final_sort clause!";
            AUTIL_LOG(WARN, "%s", errorMsg.c_str());
            return false;
        } 
    }
    return true;
}

} // namespace sorter
} // namespace isearch

