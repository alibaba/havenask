#ifndef ISEARCH_DOCCOUNTLIMITS_H
#define ISEARCH_DOCCOUNTLIMITS_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <ha3/common/Tracer.h>
#include <ha3/common/Request.h>

BEGIN_HA3_NAMESPACE(rank);
class RankProfile;
END_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(common);
class SortDescription;
END_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);

struct DocCountLimits {
    uint32_t rankSize;
    uint32_t rerankSize;
    uint32_t requiredTopK;
    uint32_t runtimeTopK;
    DocCountLimits()
        : rankSize(0)
        , rerankSize(0)
        , requiredTopK(0)
        , runtimeTopK(0)
    { }

    static uint32_t getRankSize(const common::Request *request,
                                const rank::RankProfile *rankProfile,
                                uint32_t partCount,
                                bool useTotal);
    static uint32_t getRerankSize(const common::Request *request,
                                  const rank::RankProfile *rankProfile,
                                  uint32_t partCount,
                                  bool useTotal);
    static uint32_t getRequiredTopK(const common::Request *request,
                                    const config::ClusterConfigInfo &clusterConfigInfo,
                                    uint32_t partCount,
                                    bool useTotal,
                                    common::Tracer *_tracer);
    static uint32_t getRuntimeTopK(const common::Request *request,
                                   uint32_t requiredTopK, uint32_t rerankSize);

    static uint32_t determinScorerId(const common::Request *request);
    static bool hasRankExpression(const std::vector<common::SortDescription*> &sortDescriptions);
    static uint32_t getNewSize(uint32_t partCount,
                               uint32_t singleFromQuery,
                               uint32_t totalFromQuery,
                               const rank::RankProfile *rankProfile,
                               uint32_t scorerId,
                               uint32_t defaultSize,
                               bool useTotal);
    static uint32_t calRequiredTopK(uint32_t partCount,
                                    uint32_t searcherReturnHits,
                                    uint32_t startPlusHit,
                                    uint32_t returnHitThreshold,
                                    double returnHitRatio,
                                    common::Tracer *_tracer);
    static uint32_t getDegradeSize(float level, uint32_t degradeSize, uint32_t orginSize);

    void init(const common::Request *request,
              const rank::RankProfile *rankProfile,
              const config::ClusterConfigInfo &clusterConfigInfo,
              uint32_t partCount,
              common::Tracer *tracer);
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_DOCCOUNTLIMITS_H
