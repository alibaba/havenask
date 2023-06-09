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

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/isearch.h"
#include "ha3/search/AggregateFunction.h"
#include "ha3/search/Aggregator.h"
#include "ha3/search/BatchAggregateSampler.h"
#include "ha3/search/NormalAggregator.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/common/QueryResource.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace suez {
namespace turing {
template <typename T> class AttributeExpressionTyped;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace search {

template <typename KeyType, typename ExprType = KeyType, typename GroupMapType =
          typename UnorderedMapTraits<KeyType>::GroupMapType>
class BatchAggregator : public NormalAggregator<KeyType, ExprType, GroupMapType>
{
public:
    BatchAggregator(suez::turing::AttributeExpressionTyped<ExprType> *attrExpr,
                    uint32_t maxKeyCount,
                    autil::mem_pool::Pool *pool,
                    uint32_t aggThreshold = 0,
                    uint32_t sampleMaxCount = 0,
                    uint32_t maxSortCount = 0,
                    suez::turing::QueryResource *queryResource = NULL)
        : NormalAggregator<KeyType, ExprType, GroupMapType>(attrExpr, maxKeyCount, pool,
                0, 1, maxSortCount, queryResource)
        , _sampler(aggThreshold, sampleMaxCount) {}

    virtual ~BatchAggregator() {}
private:
    BatchAggregator(const BatchAggregator &);
    BatchAggregator& operator=(const BatchAggregator &);
public:
    void doAggregate(matchdoc::MatchDoc matchDoc) override;
    void endLayer(double factor) override;
    void estimateResult(double factor) override;
    void estimateResult(double factor, BatchAggregateSampler &sampler) override;
    std::string getName() override {
        return "batchAgg";
    }
    uint32_t getAggregateCount() override {
        return _sampler.getAggregateCount();
    }
    void updateExprEvaluatedStatus() override {
    }
private:
    void updateSampler(BatchAggregateSampler &sampler);
private:
    BatchAggregateSampler _sampler;
private:
    template <typename T>
    friend class BatchAggregatorTest;
};

template<typename KeyType, typename ExprType, typename GroupMapType>
void BatchAggregator<KeyType, ExprType, GroupMapType>::doAggregate(matchdoc::MatchDoc matchDoc) {
    _sampler.collect(matchDoc, this->_matchDocAllocator);
}

template<typename KeyType, typename ExprType, typename GroupMapType>
void BatchAggregator<KeyType, ExprType, GroupMapType>::endLayer(double factor) {
    _sampler.endLayer(factor);
}

template<typename KeyType, typename ExprType, typename GroupMapType>
void BatchAggregator<KeyType, ExprType, GroupMapType>::updateSampler(BatchAggregateSampler &sampler) {
    sampler.setSampleMaxCount(_sampler.getSampleMaxCount());
    sampler.setAggThreshold(_sampler.getAggThreshold());
}

template<typename KeyType, typename ExprType, typename GroupMapType>
void BatchAggregator<KeyType, ExprType, GroupMapType>::estimateResult(double factor,
        BatchAggregateSampler &sampler) {
    updateSampler(sampler);
    sampler.calculateSampleStep();
    std::vector<matchdoc::MatchDoc> subDocVec;

    while (sampler.hasNextLayer()) {
        while (sampler.hasNext()) {
            auto tmpMatchDoc = this->_matchDocAllocator->batchAllocateSubdoc(
                    sampler.getMaxSubDocCount(), subDocVec, UNINITIALIZED_DOCID);
            this->_matchDocAllocator->fillDocidWithSubDoc(&sampler,
                    tmpMatchDoc, subDocVec);
            this->aggregateOneDoc(tmpMatchDoc);
            this->_matchDocAllocator->deallocate(tmpMatchDoc);
            sampler.next();
        }
        double layerFactor = sampler.getLayerFactor();
        for (typename NormalAggregator<KeyType, ExprType, GroupMapType>::ConstKeyIterator groupIt
                 = this->_groupMap.begin();
             groupIt != this->_groupMap.end(); groupIt ++)
        {
            for(Aggregator::FunctionVector::iterator funIt
                    = this->_funVector.begin();
                funIt != this->_funVector.end(); funIt ++)
            {
                (*funIt)->endLayer((uint32_t)sampler.getStep(),
                        layerFactor, groupIt->second);
            }
        }
    }
    NormalAggregator<KeyType, ExprType, GroupMapType>::estimateResult(factor);
}

template<typename KeyType, typename ExprType, typename GroupMapType>
void BatchAggregator<KeyType, ExprType, GroupMapType>::estimateResult(double factor) {
    estimateResult(factor, _sampler);
}

} // namespace search
} // namespace isearch

