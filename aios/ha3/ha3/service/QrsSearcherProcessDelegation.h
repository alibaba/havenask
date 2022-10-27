#ifndef ISEARCH_QRSSEARCHERPROCESSDELEGATION_H
#define ISEARCH_QRSSEARCHERPROCESSDELEGATION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/common/DocIdClause.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <ha3/config/AnomalyProcessConfig.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <ha3/service/HitSummarySchemaCache.h>
#include <suez/turing/expression/plugin/SorterManager.h>
#include <suez/turing/expression/plugin/SorterWrapper.h>
#include <ha3/config/QrsConfig.h>
#include <sap_eagle/RpcContext.h>
#include <multi_call/interface/SyncClosure.h>
#include <ha3/common/TimeoutTerminator.h>
#include <ha3/turing/qrs/Ha3QrsRequestGenerator.h>
#include <ha3/turing/qrs/QrsRunGraphContext.h>
BEGIN_HA3_NAMESPACE(service);

class QrsSearcherProcessDelegation
{
public:
    QrsSearcherProcessDelegation(
            const HitSummarySchemaCachePtr &hitSummarySchemaCache,
            int32_t connectionTimeout = QRS_ARPC_CONNECTION_TIMEOUT,
            HaCompressType requestCompressType = NO_COMPRESS,
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
    void setRunGraphContext(const turing::QrsRunGraphContextPtr &context) {
        _runGraphContext = context;
        if (_runGraphContext) {
            auto *queryResource = _runGraphContext->getQueryResource();
            if (queryResource) {
                _querySession = queryResource->getGigQuerySession();
                if (_querySession) {
                    _rpcContext = _querySession->getRpcContext();
                }
            }
        }
    }
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
    sap::RpcContextPtr _rpcContext;
    int32_t _connectionTimeout;
    HaCompressType _requestCompressType;
    config::SearchTaskQueueRule _searchTaskqueueRule;
    monitor::SessionMetricsCollectorPtr _metricsCollectorPtr;
    suez::turing::ClusterSorterManagersPtr _clusterSorterManagersPtr;
    multi_call::SyncClosure *_qrsClosure = nullptr;
    std::vector<HA3_NS(turing)::Ha3QrsRequestGeneratorPtr> _generatorVec;
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
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QrsSearcherProcessDelegation);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_QRSSEARCHERPROCESSDELEGATION_H
