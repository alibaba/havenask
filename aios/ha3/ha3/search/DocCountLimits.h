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
#include <vector>

#include "ha3/common/Tracer.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace common {
class Request;
}  // namespace common
namespace config {
class ClusterConfigInfo;
}  // namespace config
}  // namespace isearch

namespace isearch {
namespace rank {
class RankProfile;

} // namespace rank
} // namespace isearch

namespace isearch {
namespace common {
class SortDescription;

} // namespace common
} // namespace isearch

namespace isearch {
namespace search {

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

} // namespace search
} // namespace isearch

