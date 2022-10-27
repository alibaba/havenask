#ifndef ISEARCH_MATCHDOCSEARCHER_H
#define ISEARCH_MATCHDOCSEARCHER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/InnerSearchResult.h>
#include <ha3/search/SearcherCacheManager.h>
#include <ha3/search/MatchDocSearcherProcessorResource.h>
#include <ha3/search/SeekAndRankProcessor.h>
#include <ha3/search/RerankProcessor.h>
#include <ha3/search/ExtraRankProcessor.h>
#include <ha3/search/LayerMetas.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/SearcherCacheInfo.h>
#include <suez/turing/expression/plugin/SorterManager.h>
#include <ha3/common/HashJoinInfo.h>

BEGIN_HA3_NAMESPACE(rank);
class ScoringProvider;
class RankProfileManager;
END_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(search);

class OptimizerChainManager;
class SortExpressionCreator;

class MatchDocSearcher
{
public:
    MatchDocSearcher(
            SearchCommonResource &resource,
            SearchPartitionResource &partitionResource,
            SearchRuntimeResource &runtimeResource,
            const rank::RankProfileManager *rankProfileMgr,
            const OptimizerChainManager *optimizerChainManager,
            const suez::turing::SorterManager *sorterManager,
            const config::AggSamplerConfigInfo& aggSamplerConfigInfo,
            const config::ClusterConfigInfo &clusterConfigInfo,
            uint32_t partCount,
            SearcherCache *searcherCache = nullptr,
            const rank::RankProfile *rankProfile = NULL,
            const LayerMetasPtr &layerMetas = LayerMetasPtr());
    ~MatchDocSearcher();

private:
    MatchDocSearcher(const MatchDocSearcher &);
    MatchDocSearcher& operator=(const MatchDocSearcher &);

public:
    common::ResultPtr search(const common::Request *request);
    bool seek(const common::Request *request,
              search::SearcherCacheInfo *searcherCacheInfo,
              search::InnerSearchResult &innerResult);
    bool seekAndJoin(const common::Request *request,
                     search::SearcherCacheInfo *searcherCacheInfo,
                     common::HashJoinInfo *hashJoinInfo,
                     search::InnerSearchResult &innerResult);

    static rank::ScoringProvider *createScoringProvider(
            const common::Request *request,
            const config::IndexInfoHelper *indexInfoHelper,
            SearchCommonResource &resource,
            SearchPartitionResource &partitionResource,
            SearchProcessorResource &processorResource);
private:
    bool init(const common::Request *request);
    bool initProcessorResource(const common::Request *request);
    bool doInit(const common::Request *request);
    bool doOptimize(const common::Request *request,
                                  IndexPartitionReaderWrapper *readerWrapper);
    MatchDocSearchStrategy *getSearchStrategy(const common::Request *request) const;
    uint32_t getMatchDocMaxCount(int runtimeTopK) const;

    bool doCreateAttributeExpressionVec(const common::AttributeClause *attributeClause);
    bool initExtraDocIdentifier(const common::ConfigClause *configClause);
    bool getPKExprInfo(uint8_t phaseOneInfoMask,
                       std::string &exprName, std::string &refName) const;

    void fillGlobalInformations(common::Result *result) const;
    void doLazyEvaluate(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocVect) const;
    void fillAttributePhaseOne(common::MatchDocs *matchDocs) const;

    common::ResultPtr constructErrorResult() const;
    common::MatchDocs *createMatchDocs(InnerSearchResult &innerResult) const;
    common::Result *constructResult(InnerSearchResult &innerResult) const;
    bool initAuxJoin(common::HashJoinInfo *hashJoinInfo);

private:
    SearchCommonResource &_resource;
    SearchPartitionResource &_partitionResource;
    const config::IndexInfoHelper *_indexInfoHelper;
    const rank::RankProfileManager *_rankProfileMgr;
    const OptimizerChainManager *_optimizerChainManager;
    const suez::turing::SorterManager *_sorterManager;
    std::vector<suez::turing::AttributeExpression *> _attributeExpressionVec;
    SearcherCacheManager _searcherCacheManager;
    SearchProcessorResource _processorResource;
    SeekAndRankProcessor _seekAndRankProcessor;
    RerankProcessor _rerankProcessor;
    ExtraRankProcessor _extraRankProcessor;
    const config::ClusterConfigInfo &_clusterConfigInfo;
    uint32_t _partCount;
    search::LayerMetasPtr _layerMetas;
private:
    friend class MatchDocSearcherTest;
    friend class CachedMatchDocSearcherTest;
    friend class SearcherCacheManagerTest;

private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(MatchDocSearcher);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_MATCHDOCSEARCHER_H
