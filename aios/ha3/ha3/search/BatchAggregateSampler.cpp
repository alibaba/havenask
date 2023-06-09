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
#include "ha3/search/BatchAggregateSampler.h"

#include "autil/Log.h"

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, BatchAggregateSampler);

BatchAggregateSampler::BatchAggregateSampler(
        uint32_t aggThreshold, uint32_t sampleMaxCount)
    : _aggThreshold(aggThreshold), _sampleMaxCount(sampleMaxCount), _maxSubDocNum(0)
{
    reStart();
    _docids.reserve(DEFAULT_DOCIDS_INIT_SIZE);
}

BatchAggregateSampler::~BatchAggregateSampler() { 
}

void BatchAggregateSampler::reset() {
    _docids.clear();
    _subdocidEnds.clear();
    _subdocids.clear();
    _multiLayerPos.clear();
    _multiLayerFactor.clear();
    reStart();
}

void BatchAggregateSampler::calculateSampleStep() {
    size_t count = _docids.size();
    if (count == 0) {
        return;
    }

    if(_aggThreshold == 0 && _sampleMaxCount == 0) {
        _step = 1;
        _aggDocCount = count;
        return;
    }

    if (count > _aggThreshold && _sampleMaxCount == 0) {
        _curLayerIdx = _multiLayerPos.size();
        return;
    }

    if (count <= _aggThreshold) {
        _step = 1;
        _aggDocCount = count;
    } else {
        _step = (count + _sampleMaxCount - 1) / _sampleMaxCount;
        _aggDocCount = (count + _step - 1) / _step;
    }
}


} // namespace search
} // namespace isearch

