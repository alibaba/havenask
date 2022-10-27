#include <ha3/search/DocCountLimits.h>
#include <ha3/rank/RankProfile.h>
#include <ha3/common/Request.h>
#include <multi_call/common/ControllerParam.h>
#include <limits>
#include <algorithm>

USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(search);

void DocCountLimits::init(const common::Request *request,
                          const rank::RankProfile *rankProfile,
                          const config::ClusterConfigInfo &clusterConfigInfo,
                          uint32_t partCount,
                          common::Tracer *tracer)
{
    auto clusterClause = request->getClusterClause();
    bool useTotal = !clusterClause || clusterClause->emptyPartIds();
    float degradeLevel;
    uint32_t degradeRankSize;
    uint32_t degradeRerankSize;
    uint32_t realPartCnt = partCount;
    if (!useTotal) {
        uint32_t searchPartCnt = clusterClause->getToalSearchPartCnt();
        if (searchPartCnt > 1) {
            useTotal = true;
            realPartCnt = searchPartCnt;
        }
    }
    request->getDegradeLevel(degradeLevel, degradeRankSize, degradeRerankSize);
    rankSize = getDegradeSize(degradeLevel, degradeRankSize,
                              getRankSize(request, rankProfile, realPartCnt, useTotal));
    rerankSize = getDegradeSize(degradeLevel, degradeRerankSize,
                                getRerankSize(request, rankProfile, realPartCnt, useTotal));
    requiredTopK = getRequiredTopK(request, clusterConfigInfo, realPartCnt, useTotal, tracer);
    runtimeTopK = getRuntimeTopK(request, requiredTopK, rerankSize);
}


uint32_t DocCountLimits::getRankSize(const common::Request *request,
                                     const rank::RankProfile *rankProfile,
                                     uint32_t partCount,
                                     bool useTotal)
{
    const common::ConfigClause *configClause = request->getConfigClause();
    uint32_t singleFromQuery = configClause->getRankSize();
    uint32_t totalFromQuery = configClause->getTotalRankSize();
    return getNewSize(partCount, singleFromQuery, totalFromQuery, rankProfile, 0,
                      std::numeric_limits<uint32_t>::max(), useTotal);
}

uint32_t DocCountLimits::getRerankSize(const common::Request *request,
                                       const rank::RankProfile *rankProfile,
                                       uint32_t partCount,
                                       bool useTotal)
{
    const common::ConfigClause *configClause = request->getConfigClause();
    uint32_t singleFromQuery = configClause->getRerankSize();
    uint32_t totalFromQuery = configClause->getTotalRerankSize();
    return getNewSize(partCount, singleFromQuery, totalFromQuery, rankProfile,
                      determinScorerId(request), 0, useTotal);
}

uint32_t DocCountLimits::getNewSize(uint32_t partCount,
                                    uint32_t singleFromQuery,
                                    uint32_t totalFromQuery,
                                    const rank::RankProfile *rankProfile,
                                    uint32_t scorerId,
                                    uint32_t defaultSize,
                                    bool useTotal)
{
    assert(partCount != 0);
    if (useTotal && 0 != totalFromQuery) {
        return ceil(((double)totalFromQuery) / partCount);
    }
    if (0 != singleFromQuery) {
        return singleFromQuery;
    }
    uint32_t singleFromConfig = 0;
    uint32_t totalFromConfig = 0;
    if (rankProfile) {
        totalFromConfig = rankProfile->getTotalRankSize(scorerId);
        singleFromConfig = rankProfile->getRankSize(scorerId);
        if (useTotal && 0 != totalFromConfig) {
            return ceil(((double)totalFromConfig) / partCount);
        }
        return singleFromConfig;
    } else {
        return defaultSize;
    }
}

uint32_t DocCountLimits::determinScorerId(const common::Request *request) {
    const common::SortClause *sortClause = request->getSortClause();
    const common::RankSortClause *rankSortClause = request->getRankSortClause();
    uint32_t scorerId = 0;
    if (!sortClause && !rankSortClause) {
        scorerId = 1;
    } else if (rankSortClause) {
        for (uint32_t i = 0; i < rankSortClause->getRankSortDescCount(); i++) {
            const common::RankSortDescription *rankSortDesc = rankSortClause->getRankSortDesc(i);
            const std::vector<common::SortDescription*> &sortDescriptions = rankSortDesc->getSortDescriptions();
            if (hasRankExpression(sortDescriptions)) {
                scorerId = 1;
                break;
            }
        }
    } else if (sortClause) {
        const std::vector<common::SortDescription*>& sortDescriptions = sortClause->getSortDescriptions();
        if (hasRankExpression(sortDescriptions)) {
            scorerId = 1;
        }
    }
    return scorerId;
}

bool DocCountLimits::hasRankExpression(const std::vector<common::SortDescription*> &sortDescriptions) {
    for (size_t i = 0; i < sortDescriptions.size(); i++) {
        if (sortDescriptions[i]->isRankExpression()) {
            return true;
        }
    }
    return false;
}

uint32_t DocCountLimits::getRequiredTopK(
        const common::Request *request,
        const ClusterConfigInfo &clusterConfigInfo,
        uint32_t partCount,
        bool useTotal,
        common::Tracer* tracer)
{
    auto returnHitThreshold = clusterConfigInfo._returnHitThreshold;
    auto returnHitRatio = useTotal? clusterConfigInfo._returnHitRatio : RETURN_HIT_REWRITE_RATIO;
    const common::ConfigClause *configClause = request->getConfigClause();
    auto searcherReturnHits = configClause->getSearcherReturnHits();
    auto startPlusHit = configClause->getStartOffset() + configClause->getHitCount();
    return calRequiredTopK(partCount, searcherReturnHits, startPlusHit,
                           returnHitThreshold, returnHitRatio, tracer);
}

uint32_t DocCountLimits::calRequiredTopK(
        uint32_t partCount,
        uint32_t searcherReturnHits,
        uint32_t startPlusHit,
        uint32_t returnHitThreshold,
        double returnHitRatio,
        common::Tracer *_tracer)
{
    if (0 != searcherReturnHits) {
        return std::min(searcherReturnHits, startPlusHit);
    }
    if (partCount <= 1
        || returnHitRatio <= 0.0
        || returnHitRatio > partCount)
    {
        return startPlusHit;
    }
    if (startPlusHit > returnHitThreshold) {
        uint32_t newReturnCount = (returnHitRatio * startPlusHit) / partCount;
        REQUEST_TRACE(TRACE3, "partCount: %d, returnHitThreshold: %u, searcher query return "
                      "count: %u, modified searcher return count: %u",
                      partCount, returnHitThreshold, startPlusHit, newReturnCount);
        return newReturnCount;
    } else {
        return startPlusHit;
    }
}

uint32_t DocCountLimits::getRuntimeTopK(const common::Request *request,
                                        uint32_t requiredTopK, uint32_t rerankSize)
{
    if (requiredTopK == 0) {
        return 0;
    }
    uint32_t topK = std::max(requiredTopK, rerankSize);
    common::DistinctClause *distClause = request->getDistinctClause();
    if (distClause) {
        common::DistinctDescription *distDesc = distClause->getDistinctDescription(0);
        if (distDesc) {
            topK = std::max((uint32_t)distDesc->getMaxItemCount(), topK);
        }
    }
    topK = std::min(topK, MAX_RERANK_SIZE);
    return topK;
}

uint32_t DocCountLimits::getDegradeSize(float level,
                                        uint32_t degradeSize,
                                        uint32_t orginSize)
{
    if (level <= multi_call::MIN_PERCENT
        || 0 == degradeSize
        || orginSize <= degradeSize)
    {
        return orginSize;
    }
    level = std::min(level, multi_call::MAX_PERCENT);
    return orginSize - (orginSize - degradeSize) * level;
}

END_HA3_NAMESPACE(search);
