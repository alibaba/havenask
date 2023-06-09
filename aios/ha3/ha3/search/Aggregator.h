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
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "cava/common/Common.h"
#include "ha3/common/AggregateResult.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/cava/common/CavaAggModuleInfo.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace matchdoc {
class MatchDocAllocator;
class ReferenceBase;
template <typename T> class Reference;
}  // namespace matchdoc
namespace suez {
namespace turing {
class AttributeExpression;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace search {
class AggregateFunction;
class BatchAggregateSampler;
class Filter;

class Aggregator
{
public:
    typedef std::vector<AggregateFunction*> FunctionVector;
public:
    Aggregator(autil::mem_pool::Pool *pool)
        : _pool(pool)
        , _matchDocAllocator(NULL)
        , _isStopped(false)
        , _toAggDocCount(0) {}
    virtual ~Aggregator() {}
public:
    void aggregate(matchdoc::MatchDoc matchDoc) {
        if (_isStopped) {
            return;
        }
        _toAggDocCount++;
        doAggregate(matchDoc);
    }
    void setFactor(double factor) {
        _factor = factor;
    }
    double getFactor() {
        return _factor;
    }
    virtual void batchAggregate(std::vector<matchdoc::MatchDoc> &matchDocs) {
        for (auto matchDoc : matchDocs) {
            aggregate(matchDoc);
        }
    }
    virtual void doAggregate(matchdoc::MatchDoc matchDoc) = 0;
    virtual common::AggregateResultsPtr collectAggregateResult() = 0;
    virtual void doCollectAggregateResult(common::AggregateResultPtr &aggResultPtr) {
        assert(false);
    }
    virtual void estimateResult(double factor) = 0;
    virtual void estimateResult(
            double factor, BatchAggregateSampler& sampler) {
        assert(false);
    }
    virtual void stop() {
        _isStopped = true;
    }
    virtual void setFilter(Filter *filter) {
        assert(false);
    }
    virtual void setMatchDocAllocator(matchdoc::MatchDocAllocator *alloc) {
        _matchDocAllocator = alloc;
    }
    template<typename T2>
    const matchdoc::Reference<T2> *getFunResultReference(
            const std::string &funString) {
        assert(false);
        return NULL;
    }
    virtual void addAggregateFunction(AggregateFunction *fun) {
    }
    virtual void beginSample() = 0;
    virtual void endLayer(double factor) = 0;
    virtual std::string getName() {
        assert(false);
        return "baseAgg";
    }
    virtual uint32_t getAggregateCount() {
        return 0;
    }
    uint32_t getToAggDocCount() {
        return _toAggDocCount;
    }
    virtual void updateExprEvaluatedStatus() = 0;
    virtual suez::turing::CavaAggModuleInfoPtr codegen() {
        return suez::turing::CavaAggModuleInfoPtr();
    }
    virtual suez::turing::AttributeExpression *getGroupKeyExpr() {
        assert(false);
        return NULL;
    }
    virtual suez::turing::AttributeExpression *getAggFunctionExpr(uint index) {
        assert(false);
        return NULL;
    }
    virtual matchdoc::ReferenceBase *getAggFunctionRef(uint index, uint id) {
        assert(false);
        return NULL;
    }
    virtual matchdoc::ReferenceBase *getGroupKeyRef() {
        assert(false);
        return NULL;
    }
    virtual uint32_t getMaxSortCount() {
        assert(false);
        return 0;
    }
protected:
    autil::mem_pool::Pool *_pool;
    matchdoc::MatchDocAllocator *_matchDocAllocator;
private:
    bool _isStopped;
    double _factor;
protected:
    uint32_t _toAggDocCount;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<Aggregator> AggregatorPtr;

} // namespace search
} // namespace isearch

