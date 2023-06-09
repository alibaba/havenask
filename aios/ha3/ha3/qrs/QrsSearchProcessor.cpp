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
#include "ha3/qrs/QrsSearchProcessor.h"

#include <assert.h>
#include <cstddef>
#include <map>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/HashFuncFactory.h"
#include "autil/HashFunctionBase.h"
#include "autil/Log.h"
#include "autil/LongHashValue.h"
#include "autil/TimeUtility.h"
#include "ha3/common/ClauseBase.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/FetchSummaryClause.h"
#include "ha3/common/FilterClause.h"
#include "ha3/common/GlobalIdentifier.h"
#include "ha3/common/Hit.h"
#include "ha3/common/Hits.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryClause.h"
#include "ha3/common/QueryFlatten.h"
#include "ha3/common/RequestCacheKeyGenerator.h"
#include "ha3/common/SearcherCacheClause.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/qrs/QrsProcessor.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/service/QrsSearchConfig.h"
#include "ha3/service/QrsSearcherProcessDelegation.h"
#include "ha3/turing/ops/QrsQueryResource.h"
#include "ha3/turing/qrs/QrsRunGraphContext.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/plugin/SorterManager.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace isearch {
namespace config {
struct SearchTaskQueueRule;
}  // namespace config
}  // namespace isearch

using namespace std;
using namespace autil;
using namespace suez::turing;

using namespace isearch::common;
using namespace isearch::search;
using namespace isearch::config;
using namespace isearch::service;

namespace isearch {
namespace qrs {
AUTIL_LOG_SETUP(ha3, QrsSearchProcessor);

QrsSearchProcessor::QrsSearchProcessor(
        int32_t connectionTimeout,
        const ClusterTableInfoMapPtr &clusterTableInfoMapPtr,
        const ClusterConfigMapPtr &clusterConfigMapPtr,
        const ClusterSorterManagersPtr &clusterSorterManagersPtr,
        autil::CompressType requestCompressType,
        const SearchTaskQueueRule &searchTaskQueueRule)
    : _clusterTableInfoMapPtr(clusterTableInfoMapPtr)
    , _clusterConfigMapPtr(clusterConfigMapPtr)
    , _hitSummarySchemaCache(new HitSummarySchemaCache(_clusterTableInfoMapPtr))
    , _qrsSearcherProcessDelegation(new QrsSearcherProcessDelegation(
                    _hitSummarySchemaCache, connectionTimeout, requestCompressType,
                    searchTaskQueueRule))
    , _clusterSorterManagersPtr(clusterSorterManagersPtr)
{
}

QrsSearchProcessor::QrsSearchProcessor(const QrsSearchProcessor& processor)
    : _clusterTableInfoMapPtr(processor._clusterTableInfoMapPtr)
    , _clusterConfigMapPtr(processor._clusterConfigMapPtr)
    , _hitSummarySchemaCache(processor._hitSummarySchemaCache)
    , _qrsSearcherProcessDelegation(new QrsSearcherProcessDelegation(
                    *processor._qrsSearcherProcessDelegation))
    , _clusterSorterManagersPtr(processor._clusterSorterManagersPtr)
{
}

QrsSearchProcessor::~QrsSearchProcessor() {
    if (_qrsSearcherProcessDelegation) {
        delete _qrsSearcherProcessDelegation;
        _qrsSearcherProcessDelegation = NULL;
    }
}

QrsProcessorPtr QrsSearchProcessor::clone() {
    QrsSearchProcessor *qrsSearcherProcessor = new QrsSearchProcessor(*this);
    qrsSearcherProcessor->initSearcherDelegation();
    return QrsProcessorPtr(qrsSearcherProcessor);
}

void QrsSearchProcessor::setSearcherDelegation(
        QrsSearcherProcessDelegation* delegation)
{
    if (delegation == _qrsSearcherProcessDelegation) {
        return;
    }
    if (_qrsSearcherProcessDelegation) {
        delete _qrsSearcherProcessDelegation;
    }
    _qrsSearcherProcessDelegation = delegation;
}

void QrsSearchProcessor::initSearcherDelegation() {
    _qrsSearcherProcessDelegation->setClusterSorterManagers(_clusterSorterManagersPtr);
    _qrsSearcherProcessDelegation->createClusterIdMap();
}

void QrsSearchProcessor::initQrsSearchProcesser() {
    _qrsSearcherProcessDelegation->setSessionMetricsCollector(_metricsCollectorPtr);
    _qrsSearcherProcessDelegation->setTracer(getTracer());
    _qrsSearcherProcessDelegation->setTimeoutTerminator(_timeoutTerminator);
    _qrsSearcherProcessDelegation->setRunGraphContext(_runGraphContext);
}

void QrsSearchProcessor::flatten(QueryClause *queryClause) {
    uint32_t queryCount = queryClause->getQueryCount();
    QueryFlatten queryFlatten;
    for (uint32_t i = 0; i < queryCount; ++i) {
        queryFlatten.flatten(queryClause->getRootQuery(i));
        Query *flattenQuery = queryFlatten.stealQuery();
        assert(flattenQuery);
        queryClause->setRootQuery(flattenQuery, i);
        REQUEST_TRACE(DEBUG, "after flatten %dth query:%s", i,
                      flattenQuery->toString().c_str());
    }
}

void QrsSearchProcessor::process(RequestPtr &requestPtr,
                                 ResultPtr &resultPtr)
{
    //flatten query
    initQrsSearchProcesser();
    ConfigClause *configClause = requestPtr->getConfigClause();
    rewriteMetricSrc(configClause);
    QueryClause *queryClause = requestPtr->getQueryClause();
    REQUEST_TRACE(DEBUG, "begin flatten QueryClause");
    flatten(queryClause);
    REQUEST_TRACE(DEBUG, "end flatten QueryClause");
    AuxQueryClause *auxQueryClause = requestPtr->getAuxQueryClause();
    if (auxQueryClause) {
        REQUEST_TRACE(DEBUG, "begin flatten AuxQueryClause");
        flatten(auxQueryClause);
        REQUEST_TRACE(DEBUG, "end flatten AuxQueryClause");
    }
    fillSearcherCacheKey(requestPtr);
    FetchSummaryClause *fetchSummaryClause = requestPtr->getFetchSummaryClause();
    if (!fetchSummaryClause) {
        resultPtr = _qrsSearcherProcessDelegation->search(requestPtr);
    } else {
        resultPtr = convertFetchSummaryClauseToHits(requestPtr);
    }
    convertSummaryCluster(resultPtr->getHits(), configClause);
    REQUEST_TRACE(DEBUG, "after searcher process delegation search.");
    QrsProcessor::process(requestPtr, resultPtr);
}

void QrsSearchProcessor::convertSummaryCluster(Hits *hits, ConfigClause *configClause)
{
    if (!hits) {
        return;
    }
    bool rewriteFetchSummaryType = configClause->getFetchSummaryType() == BY_DOCID;
    for (size_t i = 0; i < hits->size(); ++i) {
        const HitPtr &hitPtr = hits->getHit(i);
        const string &clusterName = hitPtr->getClusterName();
        const string &summaryCluster =
            configClause->getFetchSummaryClusterName(clusterName);
        if (!hitPtr->hasPrimaryKey()) {
            rewriteFetchSummaryType = false;
        }
        GlobalIdentifier &gid = hitPtr->getGlobalIdentifier();
        gid.setClusterId(_qrsSearcherProcessDelegation->getClusterId(summaryCluster));
        const string &rawPk = hitPtr->getRawPk();
        if (!rawPk.empty()) {
            if (!hitPtr->hasPrimaryKey() || summaryCluster != clusterName) {
                gid.setHashId(calculateHashId(summaryCluster, rawPk));
            }
            gid.setPrimaryKey(calculatePrimaryKey(summaryCluster, rawPk));
        }
        gid.setPosition((uint32_t)i);
        REQUEST_TRACE(DEBUG, "Cluster[%s] FetchSummaryCluster[%s] GlobalIdentifier:[%s]",
                      clusterName.c_str(), summaryCluster.c_str(),
                      gid.toString().c_str());
    }
    if (hits->size() > 0 && rewriteFetchSummaryType) {
        configClause->setFetchSummaryType(BY_PK);
        REQUEST_TRACE(DEBUG, "rewrite FetchSummaryType to BY_PK");
    }
}

void QrsSearchProcessor::fillSearcherCacheKey(RequestPtr &requestPtr) {
    SearcherCacheClause* searcherCacheClause = requestPtr->getSearcherCacheClause();
    if ((NULL == searcherCacheClause)
        || !searcherCacheClause->getUse()
        || (0L != searcherCacheClause->getKey()))
    {
        return;
    }
    RequestCacheKeyGenerator cacheKeyGenerator(requestPtr);
    searcherCacheClause->setKey(cacheKeyGenerator.generateRequestCacheKey());
}

ResultPtr QrsSearchProcessor::convertFetchSummaryClauseToHits(
        const RequestPtr &requestPtr)
{
    Hits *hits = new Hits;
    FetchSummaryClause *fetchSummaryClause = requestPtr->getFetchSummaryClause();
    ConfigClause *configClause = requestPtr->getConfigClause();
    const vector<string> &clusterNames = fetchSummaryClause->getClusterNames();
    const vector<GlobalIdentifier> &gids = fetchSummaryClause->getGids();
    const vector<string> &rawPks = fetchSummaryClause->getRawPks();
    uint32_t fetchSummaryType = configClause->getFetchSummaryType();
    for (size_t i = 0; i < clusterNames.size(); ++i) {
        const string &clusterName = clusterNames[i];
        HitPtr hitPtr(new Hit(0));
        if (fetchSummaryType == BY_DOCID || fetchSummaryType == BY_PK) {
            hitPtr->setGlobalIdentifier(gids[i]);
        } else {
            hitPtr->setRawPk(rawPks[i]);
        }
        hitPtr->setClusterName(clusterName);
        hits->addHit(hitPtr);
    }
    if (fetchSummaryType == BY_RAWPK) {
        configClause->setFetchSummaryType((uint32_t)BY_PK);
    }
    hits->setIndependentQuery(true);
    ResultPtr resultPtr(new common::Result);
    resultPtr->setHits(hits);
    return resultPtr;
}

void QrsSearchProcessor::fillSummary(const RequestPtr &requestPtr,
                                     const ResultPtr &resultPtr)
{
    initQrsSearchProcesser();
    fillPositionForHits(resultPtr);
    _metricsCollectorPtr->fillSummaryStartTrigger();
    if (unlikely(_tracer && resultPtr)) {
        ConfigClause *configClause = requestPtr->getConfigClause();
        TracerPtr tracerPtr(configClause->createRequestTracer("qrs", ""));
        resultPtr->setTracer(tracerPtr);
    }
    _qrsSearcherProcessDelegation->fillSummary(requestPtr, resultPtr);
    _metricsCollectorPtr->fillSummaryEndTrigger();
}

void QrsSearchProcessor::fillPositionForHits(const ResultPtr &result) {
    if (!result) {
        return;
    }
    Hits *hits = result->getHits();
    if (hits == NULL) {
        return;
    }
    for (uint32_t i = 0; i < hits->size(); i++) {
        const HitPtr &hit = hits->getHit(i);
        hit->setPosition(i);
    }
}

uint32_t QrsSearchProcessor::calculateHashId(const string &clusterName,
        const string &rawPk)
{
    map<std::string, ClusterConfigInfo>::const_iterator it =
        _clusterConfigMapPtr->find(clusterName);
    assert(it != _clusterConfigMapPtr->end());
    const ClusterConfigInfo &clusterConfigInfo = it->second;
    const string &hashFunction = clusterConfigInfo.getHashMode()._hashFunction;
    HashFunctionBasePtr hashFuncPtr = HashFuncFactory::createHashFunc(hashFunction);
    assert(hashFuncPtr);
    return hashFuncPtr->getHashId(rawPk);
}

primarykey_t QrsSearchProcessor::calculatePrimaryKey(const string &clusterName,
        const string &rawPk)
{
    ClusterTableInfoMap::const_iterator iter = _clusterTableInfoMapPtr->find(clusterName);
    assert(iter != _clusterTableInfoMapPtr->end());
    const IndexInfo *pkIndexInfo = iter->second->getPrimaryKeyIndexInfo();
    assert(pkIndexInfo != NULL);

    InvertedIndexType pkIndexType = pkIndexInfo->getIndexType();
    primarykey_t hashedPrimaryKey = 0;
    if (pkIndexType == it_primarykey64 || pkIndexType == it_primarykey128) {
        bool ret = false;
        if (pkIndexType == it_primarykey64) {
            ret = indexlib::index::KeyHasherWrapper::GetHashKey(pkIndexInfo->getSingleFieldType(),
                                                               pkIndexInfo->getPrimaryKeyHashType(),
                                                               rawPk.c_str(),
                                                               rawPk.size(),
                                                               hashedPrimaryKey.value[1]);
        } else {
            ret = indexlib::index::KeyHasherWrapper::GetHashKey(pkIndexInfo->getSingleFieldType(),
                                                               pkIndexInfo->getPrimaryKeyHashType(),
                                                               rawPk.c_str(),
                                                               rawPk.size(),
                                                               hashedPrimaryKey);
        }
        if (!ret || (hashedPrimaryKey == 0)) {
            AUTIL_LOG(ERROR,
                      "calculatePrimaryKey failed, pk(%s) fieldtype(%d), hashtype(%d)",
                      rawPk.c_str(),
                      pkIndexInfo->getSingleFieldType(),
                      pkIndexInfo->getPrimaryKeyHashType());
        }
    }
    return hashedPrimaryKey;
}

void QrsSearchProcessor::rewriteMetricSrc(ConfigClause *configClause) {
    if (configClause && _runGraphContext) {
        auto *queryResource = dynamic_cast<turing::QrsQueryResource *>(_runGraphContext->getQueryResource());
        if (queryResource && queryResource->qrsSearchConfig) {
            const string &src = configClause->getMetricsSrc();
            if (queryResource->qrsSearchConfig) {
                if (queryResource->qrsSearchConfig->_metricsSrcWhiteList.count(src) == 0) {
                    configClause->setMetricsSrc("others");
                }
            }
        }
    }
}

} // namespace qrs
} // namespace isearch
