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
#include "ha3/search/MultiAggregator.h"

#include <memory>
#include <sstream>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "ha3/search/Aggregator.h"
#include "matchdoc/MatchDoc.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace std;
using namespace autil;
namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, MultiAggregator);
using namespace isearch::common;

MultiAggregator::MultiAggregator(mem_pool::Pool *pool) 
  : Aggregator(pool) 
{
}

MultiAggregator::MultiAggregator(const AggregatorVector& aggVec, 
                                 mem_pool::Pool *pool)
    : Aggregator(pool)
    , _aggVector(aggVec)
{
}

MultiAggregator::~MultiAggregator() { 
    for (AggregatorVector::iterator it = _aggVector.begin();
         it != _aggVector.end(); it++)
    {
        delete (*it);
    }
    _aggVector.clear();
}

void MultiAggregator::doAggregate(matchdoc::MatchDoc matchDoc) {
    for (AggregatorVector::iterator it = _aggVector.begin();
         it != _aggVector.end(); it++)
    {
        (*it)->doAggregate(matchDoc);
    }
}

common::AggregateResultsPtr MultiAggregator::collectAggregateResult() {
    common::AggregateResultsPtr aggResultsPtr(new common::AggregateResults());
    for (AggregatorVector::const_iterator it = _aggVector.begin();
         it != _aggVector.end(); it++)
    {
        const common::AggregateResultsPtr tmpAggResultsPtr =
            (*it)->collectAggregateResult();
        aggResultsPtr->insert(aggResultsPtr->end(),
                              tmpAggResultsPtr->begin(),
                              tmpAggResultsPtr->end());
    }
    return aggResultsPtr;
}

void MultiAggregator::estimateResult(double factor) {
    for (AggregatorVector::const_iterator it = _aggVector.begin();
         it != _aggVector.end(); it++)
    {
        (*it)->estimateResult(factor);
    }
}

void MultiAggregator::beginSample() {
    for (AggregatorVector::const_iterator it = _aggVector.begin();
         it != _aggVector.end(); it++)
    {
        (*it)->beginSample();
    }
}

void MultiAggregator::endLayer(double factor) {
    for (AggregatorVector::const_iterator it = _aggVector.begin();
         it != _aggVector.end(); it++)
    {
        (*it)->endLayer(factor);
    }
}

void MultiAggregator::updateExprEvaluatedStatus() {
    for (AggregatorVector::const_iterator it = _aggVector.begin();
         it != _aggVector.end(); it++)
    {
        (*it)->updateExprEvaluatedStatus();
    }
}

} // namespace search
} // namespace isearch
