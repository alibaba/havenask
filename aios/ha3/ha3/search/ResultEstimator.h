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
#include <memory>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/LayerMetas.h"

namespace isearch {
namespace search {
class Aggregator;

class ResultEstimator
{
public:
    ResultEstimator();
    ~ResultEstimator();
private:
    ResultEstimator(const ResultEstimator &);
    ResultEstimator& operator=(const ResultEstimator &);
public:
    void init(const LayerMetas &allLayers, Aggregator *aggregator);
    void endLayer(uint32_t layer, uint32_t seekedCount,
                  uint32_t singleLayerMatchCount,
                  double truncateChainFactor);
    void endSeek();

    uint32_t getTotalMatchCount() const { return _totalMatchCount; }
    uint32_t getTotalSeekedCount() const { return _totalSeekedCount; }
    uint32_t getMatchCount() const { return _matchCount; }
    bool needAggregate(uint32_t layer) const { return _rangeSizes[layer] != 0; }

private:
    static bool isCovered(const LayerMeta &layer1, const LayerMeta &layer2);
    static void initLayerRangeSize(const LayerMetas &allLayers, 
                                   std::vector<uint32_t> &rangeSizes,
                                   std::vector<bool> &needAggregate);
    static void initCoveredInfo(const LayerMetas &allLayers, 
                                std::vector<std::vector<uint32_t> > &coveredLayerInfo);
private:
    std::vector<uint32_t> _rangeSizes;
    std::vector<bool> _needAggregate;
    uint32_t _totalSeekedLayerRange;
    uint32_t _totalSeekedCount;
    
    uint32_t _matchCount;
    uint32_t _totalMatchCount;
    Aggregator *_aggregator;

    std::vector<std::vector<uint32_t> > _coveredLayerInfo;
private:
    friend class ResultEstimatorTest;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ResultEstimator> ResultEstimatorPtr;

} // namespace search
} // namespace isearch

