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
#include "ha3/rank/SimpleDistinctBoostScorer.h"

#include "ha3/isearch.h"
#include "autil/Log.h"

namespace isearch {
namespace rank {
AUTIL_LOG_SETUP(ha3, SimpleDistinctBoostScorer);

SimpleDistinctBoostScorer::SimpleDistinctBoostScorer(uint32_t distinctTimes, 
        uint32_t distinctCount)
{ 
    _distinctTimes = distinctTimes;
    _distinctCount = distinctCount;
}

SimpleDistinctBoostScorer::~SimpleDistinctBoostScorer() { 
}

score_t SimpleDistinctBoostScorer::calculateBoost(uint32_t keyPosition, 
        score_t rawScore)
{
    uint32_t time = keyPosition / _distinctCount;
    if (time >= _distinctTimes) {
        return (score_t)0;
    } else {
        return (score_t)(_distinctTimes - time);
    }
}

} // namespace rank
} // namespace isearch
