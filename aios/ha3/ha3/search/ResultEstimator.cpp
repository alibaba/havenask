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
#include "ha3/search/ResultEstimator.h"

#include <assert.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "autil/mem_pool/PoolVector.h"

#include "ha3/search/Aggregator.h"
#include "ha3/search/LayerMetas.h"
#include "autil/Log.h"

using namespace std;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, ResultEstimator);

ResultEstimator::ResultEstimator()
    : _totalSeekedLayerRange(0)
    , _totalSeekedCount(0)
    , _matchCount(0)
    , _totalMatchCount(0)
    , _aggregator(NULL)
{ 
}

ResultEstimator::~ResultEstimator() { 
}

void ResultEstimator::init(const LayerMetas &allLayers, Aggregator *aggregator) {
    initLayerRangeSize(allLayers, _rangeSizes, _needAggregate);
    initCoveredInfo(allLayers, _coveredLayerInfo);
    _totalSeekedLayerRange = 0;
    _matchCount = 0;
    _totalMatchCount = 0;
    _aggregator = aggregator;
}

void ResultEstimator::endLayer(uint32_t layer, uint32_t seekedCount,
                               uint32_t singleLayerMatchCount, double truncateChainFactor)
{
    assert(layer < _rangeSizes.size());
    _totalSeekedLayerRange += _rangeSizes[layer];
    _needAggregate[layer] = true;
    _totalSeekedCount += seekedCount;
    _matchCount += singleLayerMatchCount;
    if (seekedCount != 0) {
        double factor = truncateChainFactor * _rangeSizes[layer] / (double)seekedCount;
        _totalMatchCount += (uint32_t)(singleLayerMatchCount * factor);
        if (_aggregator) {
            _aggregator->endLayer(factor);
        }
    }
    if (_rangeSizes[layer] == 0) {
        return;
    }
    for (size_t i = 0; i < _coveredLayerInfo[layer].size(); ++i) {
        _rangeSizes[_coveredLayerInfo[layer][i]] = 0;
    }
}

void ResultEstimator::endSeek() {
    uint32_t totalRange = 0;
    for (size_t i = 0; i < _rangeSizes.size(); ++i) {
        if (_needAggregate[i]) {
            totalRange += _rangeSizes[i];
        }
    }
    double factor = totalRange / (double)_totalSeekedLayerRange;
    _totalMatchCount = uint32_t(_totalMatchCount * factor);
    if (_aggregator) {
        _aggregator->estimateResult(factor);
    }
}

void ResultEstimator::initLayerRangeSize(const LayerMetas &allLayer, 
        vector<uint32_t> &layerRangeSizes, vector<bool> &needAggregate)
{
    for (size_t i = 0; i < allLayer.size(); ++i) {
        uint32_t rangeSize = 0;
        for (size_t j = 0; j < allLayer[i].size(); ++j) {
            rangeSize += allLayer[i][j].end - allLayer[i][j].begin + 1;
        }
        layerRangeSizes.push_back(rangeSize);
        needAggregate.push_back(allLayer[i].needAggregate);
    }
}

void ResultEstimator::initCoveredInfo(const LayerMetas &allLayer, 
                                      vector<vector<uint32_t> > &coveredLayerInfo) 
{
    coveredLayerInfo.resize(allLayer.size());
    for (size_t i = 0; i < allLayer.size(); ++i) {
        for (size_t j = 0; j < allLayer.size(); ++j) {
            if (i == j) {
                continue;
            }
            if (isCovered(allLayer[i], allLayer[j])) {
                coveredLayerInfo[i].push_back(j);
            }
        }
    }
}

bool ResultEstimator::isCovered(const LayerMeta &layer1, const LayerMeta &layer2) {
    if (layer1.size() != layer2.size()) {
        return false;
    }
    for (size_t i = 0; i < layer1.size(); ++i) {
        if (layer1[i].begin != layer2[i].begin
            || layer1[i].end != layer2[i].end)
        {
            return false;
        }
    }
    return true;
}
} // namespace search
} // namespace isearch

