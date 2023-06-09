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
#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "cava/runtime/CavaCtx.h"
#include "ha3/common/AggregateResult.h"
#include "ha3/search/Aggregator.h"
#include "ha3/search/NormalAggregator.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/cava/common/CavaAggModuleInfo.h"
#include "suez/turing/expression/cava/common/CavaJitWrapper.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace ha3 {
class Aggregator;
}  // namespace ha3
namespace unsafe {
class MatchDocs;
}  // namespace unsafe

namespace isearch {
namespace search {
class AggregateFunction;
class Filter;

template <typename KeyType, typename ExprType = KeyType, typename GroupMapType =
          typename UnorderedMapTraits<KeyType>::GroupMapType>

class JitNormalAggregator : public Aggregator
{
public:
    JitNormalAggregator(NormalAggregator<KeyType, ExprType, GroupMapType> *aggregator,
                        autil::mem_pool::Pool *pool,
                        suez::turing::CavaAggModuleInfoPtr cavaAggModuleInfo,
                        suez::turing::SuezCavaAllocator *cavaAllocator)
        : Aggregator(pool)
        , _aggregator(aggregator)
        , _funcString(_aggregator->getFuncString())
        , _needSort(_aggregator->getNeedSort())
        , _maxKeyCount(_aggregator->getMaxKeyCount())
        , _cavaAggModuleInfo(cavaAggModuleInfo)
        , _aggregateCount(0)
        , _cavaCtx(cavaAllocator)
    {
        ::ha3::Aggregator *cavaAggPtr = (::ha3::Aggregator *)aggregator;
        _jitAggregator = _cavaAggModuleInfo->createProtoType(&_cavaCtx, cavaAggPtr);
    }
    ~JitNormalAggregator() {
        _cavaAggModuleInfo.reset();
        delete _aggregator;
    }
private:
    JitNormalAggregator(const JitNormalAggregator &);
    JitNormalAggregator& operator=(const JitNormalAggregator &);
public:
    class CompCount {
    public:
        CompCount(const matchdoc::Reference<int64_t> *ref) : _ref(ref) {}
        bool operator()(const matchdoc::MatchDoc &lhs,
                        const matchdoc::MatchDoc &rhs) const
        {
            int64_t leftCount = _ref->get(lhs);
            int64_t rightCount = _ref->get(rhs);
            return leftCount > rightCount;
        }
    private:
        const matchdoc::Reference<int64_t> *_ref;
    };
    void batchAggregate(std::vector<matchdoc::MatchDoc> &matchDocs) override {
        _aggregateCount += matchDocs.size();
        _toAggDocCount = _aggregateCount;
        _cavaAggModuleInfo->batchProtoType(_jitAggregator, &_cavaCtx,
                (::unsafe::MatchDocs *)matchDocs.data(), (uint32_t)matchDocs.size());
    }
    void doCollectAggregateResult(common::AggregateResultPtr &aggResultPtr) override {
        uint32_t count = _cavaAggModuleInfo->countProtoType(_jitAggregator, &_cavaCtx);
        auto aggAllocator = _aggregator->getAggAllocator();
        std::vector<matchdoc::MatchDoc> matchdocs = aggAllocator->batchAllocate(count);
        _cavaAggModuleInfo->resultProtoType(_jitAggregator, &_cavaCtx,
                (::unsafe::MatchDocs *)matchdocs.data(), (uint32_t)matchdocs.size());
        const matchdoc::Reference<int64_t> *refer =
            _aggregator-> template getFunResultReference<int64_t>(_funcString);
        auto begin = matchdocs.begin();
        auto end = matchdocs.end();
        if (_needSort && count > _maxKeyCount) {
            std::nth_element(matchdocs.begin(),
                    matchdocs.begin() + _maxKeyCount - 1, matchdocs.end(), CompCount(refer));
            end = matchdocs.begin() + _maxKeyCount;
        }
        for (auto it = begin; it != end; ++it) {
            aggResultPtr->addAggFunResults(*it);
        }
    }
    uint32_t getAggregateCount() override {
        return _aggregateCount;
    }
    common::AggregateResultsPtr collectAggregateResult() override {
        common::AggregateResultPtr aggResultPtr(new common::AggregateResult());
        aggResultPtr->setGroupExprStr(_aggregator->getGroupKeyStr());
        auto &funVector = _aggregator->getFunVector();
        for(auto funIt = funVector.begin(); funIt != funVector.end(); ++funIt) {
            (*funIt)->getReference()->setSerializeLevel(SL_CACHE);
            aggResultPtr->addAggFunName((*funIt)->getFunctionName());
            aggResultPtr->addAggFunParameter((*funIt)->getParameter());
        }
        auto aggAllocator = _aggregator->getAggAllocator();
        aggResultPtr->setMatchDocAllocator(aggAllocator);
        doCollectAggregateResult(aggResultPtr);
        if (aggAllocator) {
            aggAllocator->extend();
        }
        common::AggregateResultsPtr aggResultsPtr(new common::AggregateResults());
        aggResultsPtr->push_back(aggResultPtr);
        return aggResultsPtr;
    }
    void setFilter(Filter *filter) override {
        _aggregator->setFilter(filter);
    }
    void addAggregateFunction(AggregateFunction *fun) override {
        _aggregator->addAggregateFunction(fun);
    }
    std::string getName() override {
        return "JitAgg";
    }
    void updateExprEvaluatedStatus() override {
        _aggregator->updateExprEvaluatedStatus();
    }
public:
    void doAggregate(matchdoc::MatchDoc matchDoc) override {}
    void estimateResult(double factor) override {}
    void beginSample() override {}
    void endLayer(double factor) override {}
private:
    NormalAggregator<KeyType, ExprType, GroupMapType> *_aggregator;
    cava::CavaJitModulePtr _cavaJitModule;
    std::string _funcString;
    bool _needSort;
    uint32_t _maxKeyCount;
    suez::turing::CavaAggModuleInfoPtr _cavaAggModuleInfo;
    uint32_t _aggregateCount;
    void *_jitAggregator;
    CavaCtx _cavaCtx;
};

} // namespace search
} // namespace isearch

