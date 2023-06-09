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
#include "ha3/search/BatchMultiAggregator.h"

#include <vector>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "ha3/search/Aggregator.h"
#include "ha3/search/MultiAggregator.h"
#include "matchdoc/MatchDoc.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

namespace isearch {
namespace search {

using namespace autil;

AUTIL_LOG_SETUP(ha3, BatchMultiAggregator);

BatchMultiAggregator::BatchMultiAggregator(mem_pool::Pool *pool)
  : MultiAggregator(pool) 
{
}

BatchMultiAggregator::BatchMultiAggregator(
        const AggregatorVector &aggVec, mem_pool::Pool *pool) 
    : MultiAggregator(aggVec, pool)
{ 
}

BatchMultiAggregator::~BatchMultiAggregator() { 
}

void BatchMultiAggregator::doAggregate(matchdoc::MatchDoc matchDoc) {
    _sampler.collect(matchDoc, _matchDocAllocator);
}

void BatchMultiAggregator::estimateResult(double factor) {
    for (AggregatorVector::const_iterator it = _aggVector.begin();
         it != _aggVector.end(); it++)
    {
        _sampler.reStart();
        (*it)->estimateResult(factor, _sampler);
    }
}

void BatchMultiAggregator::endLayer(double factor) {
    _sampler.endLayer(factor);
}

} // namespace search
} // namespace isearch

