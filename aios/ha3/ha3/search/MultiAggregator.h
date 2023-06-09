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

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/AggregateResult.h"
#include "ha3/search/Aggregator.h"
#include "ha3/search/NormalAggregator.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace matchdoc {
class MatchDoc;
class MatchDocAllocator;
}  // namespace matchdoc

namespace isearch {
namespace search {

class MultiAggregator : public Aggregator
{
public:
    typedef std::vector<Aggregator*> AggregatorVector;
public:
    MultiAggregator(autil::mem_pool::Pool *pool);
    MultiAggregator(const AggregatorVector &aggVec,
                    autil::mem_pool::Pool *pool);
    ~MultiAggregator();
public:
    void batchAggregate(std::vector<matchdoc::MatchDoc> &matchDocs) override {
        _toAggDocCount += matchDocs.size();
        for (AggregatorVector::iterator it = _aggVector.begin();
             it != _aggVector.end(); it++)
        {
            (*it)->batchAggregate(matchDocs);
        }
    }
    void doAggregate(matchdoc::MatchDoc matchDoc) override;
    void addAggregator(Aggregator *agg) {
        _aggVector.push_back(agg);
    }
    void setMatchDocAllocator(matchdoc::MatchDocAllocator *alloc) override {
        for (size_t i = 0; i < _aggVector.size(); ++i) {
            _aggVector[i]->setMatchDocAllocator(alloc);
        }
        this->_matchDocAllocator = alloc;
    }
    common::AggregateResultsPtr collectAggregateResult() override;
    void estimateResult(double factor) override;
    void beginSample() override;
    void endLayer(double factor) override;
    uint32_t getAggregateCount() override {
        assert (!_aggVector.empty());
        return _aggVector[0]->getAggregateCount();
    }
    void updateExprEvaluatedStatus() override;

    template <typename KeyType, typename ExprType, typename GroupMapType>
    const NormalAggregator<KeyType, ExprType, GroupMapType> *getNormalAggregator(uint32_t pos);

protected:
    AggregatorVector _aggVector;
private:
    AUTIL_LOG_DECLARE();
};

template <typename KeyType, typename ExprType, typename GroupMapType>
const NormalAggregator<
    KeyType, 
    ExprType, 
    GroupMapType> *MultiAggregator::getNormalAggregator(uint32_t pos)
{
    if (pos >= _aggVector.size()) {
        return NULL;
    }
    return dynamic_cast<
        NormalAggregator<KeyType, ExprType, GroupMapType>*>(_aggVector[pos]);
}

} // namespace search
} // namespace isearch

