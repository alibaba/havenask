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

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace search {

class AggregateSampler
{
public:
    AggregateSampler(uint32_t aggThreshold, uint32_t sampleStep);
    ~AggregateSampler();
public:
    inline bool sampling();
    inline uint32_t getFactor() const;
    inline bool isBeginOfSample() const;
    uint32_t getAggregateCount() {
        return _aggCount;
    }
    uint32_t getSampleStep() {
        return _sampleStep;
    }
private:
    const uint32_t _aggThreshold;
    const uint32_t _sampleStep;
    uint32_t _aggCount;
    uint32_t _stepCount;
private:
    AUTIL_LOG_DECLARE();
};

bool AggregateSampler::sampling() {
    if (_aggCount < _aggThreshold) {
        _aggCount++;
        return true;
    }
    
    _stepCount++;
    if (_stepCount == _sampleStep) {
        _stepCount = 0;
        _aggCount++;
        return true;
    }
        
    return false;
}

bool AggregateSampler::isBeginOfSample() const {
    return _aggCount  == _aggThreshold && _stepCount == 1;
}

uint32_t AggregateSampler::getFactor() const {
    if(_aggCount <= _aggThreshold
       && _stepCount == 0) 
    {
        return 1;
    }
    return _sampleStep;
}

typedef std::shared_ptr<AggregateSampler> AggregateSamplerPtr;

} // namespace search
} // namespace isearch

