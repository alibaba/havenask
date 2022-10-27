#ifndef ISEARCH_MATCHDOCSEARCHERCREATOR_H
#define ISEARCH_MATCHDOCSEARCHERCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/MatchDocSearcher.h>
#include <ha3/search/OptimizerChainManager.h>
#include <ha3/rank/RankProfileManager.h>
#include <suez/turing/expression/plugin/SorterManager.h>

BEGIN_HA3_NAMESPACE(search);

class MatchDocSearcherCreator
{
public:
    MatchDocSearcherCreator(autil::mem_pool::Pool *pool);
    ~MatchDocSearcherCreator();

private:
    MatchDocSearcherCreator(const MatchDocSearcherCreator &);
    MatchDocSearcherCreator& operator=(const MatchDocSearcherCreator &);

public:
    MatchDocSearcherPtr createSearcher(
            common::Request *request,
            const IndexPartitionReaderWrapperPtr &indexPartReaderPtr,
            const config::SearcherCacheConfig &cacheConfig,
            const IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr &snapshot,
            rank::RankProfileManager* rankProfileManager = NULL);

    MatchDocSearcherPtr createSearcher(
            common::Request *request,
            const IndexPartitionReaderWrapperPtr &indexPartReaderPtr,
            search::SearcherCache *searcherCache,
            const IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr &snapshot,
            rank::RankProfileManager* rankProfileManager = NULL,
            const LayerMetasPtr &layerMetas = LayerMetasPtr());

    MatchDocSearcherPtr createSearcher(
            common::Request *request,
            const IndexPartitionReaderWrapperPtr &indexPartReaderPtr,
            const IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr &snapshot,
            rank::RankProfileManager* rankProfileManager = NULL,
            const LayerMetasPtr &layerMetas = LayerMetasPtr());

public:
    suez::turing::TableInfoPtr makeFakeTableInfo();
    rank::RankProfileManagerPtr makeFakeRankProfileMgr();

private:
    SearchRuntimeResourcePtr createSearchRuntimeResource(const common::Request *request,
            rank::RankProfileManager *rankProfileMgr,
            const search::SearchCommonResourcePtr &commonResource,
            suez::turing::AttributeExpressionCreator *attributeExpressionCreator);
private:
    autil::mem_pool::Pool *_pool;
    rank::RankProfileManagerPtr _rankProfileMgrPtr;
    suez::turing::TableInfoPtr _tableInfoPtr;
    SearcherCachePtr _cachePtr;
    suez::turing::FunctionInterfaceCreatorPtr _funcCreatorPtr;
    OptimizerChainManagerPtr _optimizerChainManagerPtr;
    suez::turing::SorterManagerPtr _sorterManager;
    monitor::SessionMetricsCollectorPtr _sessionMetricsCollectorPtr;
    config::ResourceReaderPtr _resourceReaderPtr;
    std::tr1::shared_ptr<SearchCommonResource> _resource;
    std::tr1::shared_ptr<SearchPartitionResource> _partitionResource;
    std::tr1::shared_ptr<SearchRuntimeResource> _runtimeResource;
    config::AggSamplerConfigInfo _aggSamplerConfigInfo;
    config::ClusterConfigInfo _clusterConfigInfo;
    uint32_t _partCount;
    std::map<size_t, ::cava::CavaJitModulePtr> _cavaJitModules;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(MatchDocSearcherCreator);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_MATCHDOCSEARCHERCREATOR_H
