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
#pragma once

#include <stdint.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/CompressionUtil.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/DocIdClause.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/HitSummarySchema.h"
#include "ha3/config/QrsConfig.h"
#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/service/HitSummarySchemaCache.h"
#include "ha3/turing/qrs/Ha3QrsRequestGenerator.h"
#include "ha3/turing/qrs/QrsRunGraphContext.h"
#include "ha3/util/TypeDefine.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/Reply.h"
#include "aios/network/gig/multi_call/interface/Response.h"
#include "suez/turing/expression/plugin/SorterManager.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace google {
namespace protobuf {
class Arena;
}  // namespace protobuf
}  // namespace google
namespace isearch {
namespace common {
class ConfigClause;
class Hits;
class LevelClause;
}  // namespace common
}  // namespace isearch

namespace multi_call {
class QuerySession;
class SyncClosure;
}

namespace isearch {
namespace service {
class QrsSearcherProcessDelegation
{
public:
    QrsSearcherProcessDelegation(
            const HitSummarySchemaCachePtr &hitSummarySchemaCache,
            int32_t connectionTimeout = QRS_ARPC_CONNECTION_TIMEOUT,
            autil::CompressType requestCompressType = autil::CompressType::NO_COMPRESS,
            const config::SearchTaskQueueRule &searchTaskqueueRule =
            config::SearchTaskQueueRule());
    virtual ~QrsSearcherProcessDelegation();
    QrsSearcherProcessDelegation(const QrsSearcherProcessDelegation& delegation);
public:
    virtual common::ResultPtr search(common::RequestPtr &requestPtr);
    void fillSummary(const common::RequestPtr &requestPtr,
                     const common::ResultPtr &resultPtr);
    void setClusterSorterManagers(
            const suez::turing::ClusterSorterManagersPtr &clusterSorterManagerPtr);
    void setSessionMetricsCollector(monitor::SessionMetricsCollectorPtr &collectorPtr)
    {
        _metricsCollectorPtr = collectorPtr;
    }
    clusterid_t getClusterId(const std::string &clusterName) const;
    void createClusterIdMap();
    void setTracer(common::Tracer *tracer) {
        _tracer = tracer;
    }
    void setTimeoutTerminator(common::TimeoutTerminator *timeoutTerminator) {
        _timeoutTerminator = timeoutTerminator;
    }
    void setRunGraphContext(const turing::QrsRunGraphContextPtr &context);

public:
    static void selectMatchDocsToFormat(const common::RequestPtr &requestPtr,
            common::ResultPtr &resultptr);

private:
    void doSearch(const std::vector<std::string> &clusterNameVec,
                  const common::RequestPtr &requestPtr,
                  common::ResultPtrVector &resultPtrVec,
                  common::MultiErrorResultPtr &errors);
    void doSearchAndResearch(
            const std::string &methodName,
            const std::vector<std::string> &clusterNameVec,
            const common::RequestPtr &requestPtr,
            common::ResultPtrVector &results, common::MultiErrorResultPtr &errors);
    bool needSearchLevel(const common::LevelClause *levelClause);
    int getMatchDocCount(const common::ResultPtrVector &results);
    bool needResearch(const common::RequestPtr &requestPtr,
                      const common::ResultPtrVector &results);
    void merge(const common::RequestPtr &requestPtr,
               const common::ResultPtrVector &results,
               common::ResultPtr &mergedResultPtr);
    void addClusterName(const std::string &clusterName,
                        std::vector<std::string> &clusterNameVec);
    void addClusterName(const std::vector<std::string> &fromClusterNameVec,
                        std::vector<std::string> &clusterNameVec);
    static void convertMatchDocsToHits(const common::RequestPtr &requestPtr,
            const common::ResultPtr &resultPtr,
            const std::vector<std::string> &clusterNameVec);
    void convertAggregateResults(const common::RequestPtr &requestPtr,
                                 const common::ResultPtr &resultPtr);
    int32_t getAndAdjustTimeOut(const common::RequestPtr &requestPtr);
    multi_call::ReplyPtr doAsyncProcessCallPhase1(const std::vector<std::string> &clusters,
            const common::RequestPtr &requestPtr,
            common::MultiErrorResultPtr &errors);
    multi_call::ReplyPtr doAsyncProcessCallPhase2(const common::DocIdClauseMap &clusters,
            const common::RequestPtr &requestPtr,
            common::MultiErrorResultPtr &errors);
    void doFillSummary(common::DocIdClauseMap &docIdClauseMap,
                       const common::RequestPtr &requestPtr,
                       common::ResultPtrVector &resultPtrVec,
                       common::MultiErrorResultPtr &errors);
    common::DocIdClauseMap createDocIdClauseMap(
            const common::RequestPtr &requestPtr,
            const common::ResultPtr &resultPtr,
            std::vector<config::HitSummarySchemaPtr> &hitSummarySchemas);
    bool waitResults();
    multi_call::VersionTy fillResults(const multi_call::ReplyPtr& reply,
                     common::ResultPtrVector &resultVec,
                     common::MultiErrorResultPtr &errors);
    bool fillResult(const multi_call::ResponsePtr& response,
                    common::ResultPtr &resultPtr);
    void checkCoveredRanges(
            const std::vector<std::string> &clusterNameVec,
            common::Result::ClusterPartitionRanges &coveredRanges);
    void mergeTracer(const common::ResultPtr &resultPtr);
    void updateHitSummarySchemaAndFillCache(
            common::Hits* hits,
            const std::vector<config::HitSummarySchemaPtr> &hitSummarySchemas,
            const common::RequestPtr &requestPtr);
    multi_call::ReplyPtr multiCall(const common::RequestPtr &requestPtr,
                                   common::MultiErrorResultPtr &errors);
    void updateTotalMatchDocs(const common::RequestPtr &requestPtr,
                              const common::ResultPtr &resultPtr);
    void clearResource();
    bool associateClusterName(const common::ResultPtr &resultPtr) const;
    std::string genHitSummarySchemaCacheKey(const std::string &clusterName,
            common::ConfigClause *configClause);
    common::ResultPtr runGraph(const common::RequestPtr& request,
                               const common::ResultPtrVector &results);
private:
    static multi_call::SourceIdTy getSourceId(const common::RequestPtr &request);
private:
    std::shared_ptr<google::protobuf::Arena> _arena;
    HitSummarySchemaCachePtr _hitSummarySchemaCache;
    turing::QrsRunGraphContextPtr _runGraphContext;
    multi_call::QuerySession *_querySession = nullptr;
    int32_t _connectionTimeout;
    autil::CompressType _requestCompressType;
    config::SearchTaskQueueRule _searchTaskqueueRule;
    monitor::SessionMetricsCollectorPtr _metricsCollectorPtr;
    suez::turing::ClusterSorterManagersPtr _clusterSorterManagersPtr;
    multi_call::SyncClosure *_qrsClosure = nullptr;
    std::vector<isearch::turing::Ha3QrsRequestGeneratorPtr> _generatorVec;
    bool _lackResult;
    autil::mem_pool::Pool *_memPool = nullptr;

    typedef std::map<std::string, clusterid_t> ClusterIdMap;
    std::vector<std::string> _clusterNames;
    ClusterIdMap _clusterIdMap;
    bool _hasReSearch;
    common::Tracer *_tracer = nullptr;
    common::TimeoutTerminator *_timeoutTerminator = nullptr;
    multi_call::VersionTy _phase1Version;
    multi_call::VersionTy _phase2Version;
private:
    friend class QrsSearcherProcessDelegationTest;
    friend class QrsDelegationAnomalyProcessTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QrsSearcherProcessDelegation> QrsSearcherProcessDelegationPtr;

} // namespace service
} // namespace isearch
