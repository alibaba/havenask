#ifndef ISEARCH_SEEKANDRANKPROCESSOR_H
#define ISEARCH_SEEKANDRANKPROCESSOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/SearchCommonResource.h>
#include <ha3/search/MatchDocSearcherProcessorResource.h>
#include <ha3/search/InnerSearchResult.h>
#include <ha3/search/RankSearcher.h>
#include <ha3/search/SearcherCacheInfo.h>
#include <ha3/common/HashJoinInfo.h>

BEGIN_HA3_NAMESPACE(rank);
class ComboComparator;
END_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(search);

class MatchDocSearchStrategy;
class HitCollectorManager;

struct SeekAndRankResult {
    bool ranked;
    const rank::ComboComparator *rankComp;
    SeekAndRankResult()
        : ranked(false)
        , rankComp(NULL)
    { }
};

class SeekAndRankProcessor
{
public:
    SeekAndRankProcessor(SearchCommonResource &resource,
                         SearchPartitionResource &partitionResouce,
                         SearchProcessorResource &processorResource,
                         const config::AggSamplerConfigInfo &aggSamplerConfigInfo);
    ~SeekAndRankProcessor();

private:
    SeekAndRankProcessor(const SeekAndRankProcessor &);
    SeekAndRankProcessor& operator=(const SeekAndRankProcessor &);

public:
    bool init(const common::Request* request);
    SeekAndRankResult process(const common::Request *request,
                              MatchDocSearchStrategy *searchStrategy,
                              InnerSearchResult& innerResult);

    SeekAndRankResult processWithCache(const common::Request *request,
            SearcherCacheInfo *cacheInfo,
            InnerSearchResult& innerResult);

    SeekAndRankResult processWithCacheAndJoinInfo(const common::Request *request,
            SearcherCacheInfo *cacheInfo,
            common::HashJoinInfo *hashJoinInfo,
            InnerSearchResult &innerResult);

    void initJoinFilter(const common::Request* request) {
        // create join filter have to go after createAttributeExpression (to create docid convert)
        _rankSearcher.createJoinFilter(
                _partitionResource.attributeExpressionCreator->getJoinDocIdConverterCreator(),
                getJoinType(request));
    }
private:
    void optimizeHitCollector(const common::Request *request,
                              HitCollectorManager *hitCollectorManager,
                              uint32_t phaseCount) const;
    HitCollectorManager *createHitCollectorManager(const common::Request *request) const;

    static bool needFlattenMatchDoc(const common::Request *request) {
        if (request->getConfigClause()) {
            return request->getConfigClause()->getSubDocDisplayType()
                == SUB_DOC_DISPLAY_FLAT;
        }
        return false;
    }

    static JoinType getJoinType(const common::Request *request) {
        JoinType joinType = DEFAULT_JOIN;
        const common::ConfigClause *configClause = request->getConfigClause();
        if (configClause) {
            joinType = configClause->getJoinType();
        }
        return joinType;
    }

private:
    SearchCommonResource &_resource;
    SearchPartitionResource &_partitionResource;
    SearchProcessorResource &_processorResource;
    RankSearcher _rankSearcher;
    suez::turing::RankAttributeExpression *_rankExpression;
    HitCollectorManager *_hitCollectorManager;
private:
    friend class MatchDocSearcherTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SeekAndRankProcessor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_SEEKANDRANKPROCESSOR_H
