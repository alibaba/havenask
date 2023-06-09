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
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/StringUtil.h"
#include "ha3/search/AggregateFunction.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/common.h"
#include "sys/types.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace suez {
namespace turing {
class AttributeExpression;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace search {
class CountAggregateFunction : public AggregateFunction
{
public:
    CountAggregateFunction()
        : AggregateFunction("count", "", vt_int64)
    {
        _totalResultRefer = NULL;
        _curLayerResultRefer = NULL;
        _curLayerSampleRefer = NULL;
    }

    ~CountAggregateFunction() {
        _totalResultRefer = NULL;
        _curLayerResultRefer = NULL;
        _curLayerSampleRefer = NULL;
    }
public:
    void calculate(matchdoc::MatchDoc matchDoc, matchdoc::MatchDoc aggMatchDoc) override {
        int64_t &countValue = _curLayerSampleRefer->getReference(aggMatchDoc);
        countValue++;
    }
    void declareResultReference(matchdoc::MatchDocAllocator *allocator) override {
        _totalResultRefer = allocator->declareWithConstructFlagDefaultGroup<int64_t>(
                "count", false);
        _curLayerResultRefer =
            allocator->declareWithConstructFlagDefaultGroup<int64_t>(
                    toString() + ".curLayerResult", false);
        _curLayerSampleRefer =
            allocator->declareWithConstructFlagDefaultGroup<int64_t>(
                    toString() + ".curLayerSample", false);
        assert(_totalResultRefer);
        assert(_curLayerResultRefer);
        assert(_curLayerSampleRefer);
    }
    void initFunction(matchdoc::MatchDoc aggMatchDoc,
                      autil::mem_pool::Pool *pool = NULL) override {
        _totalResultRefer->set(aggMatchDoc, 0);
        _curLayerResultRefer->set(aggMatchDoc, 0);
        _curLayerSampleRefer->set(aggMatchDoc, 0);
    }
    matchdoc::ReferenceBase *getReference(uint id = 0) const override {
        return _totalResultRefer;
    }
    void estimateResult(double factor, matchdoc::MatchDoc aggMatchDoc) override {
        int64_t &countValue = _totalResultRefer->getReference(aggMatchDoc);
        countValue = int64_t(countValue * factor);
    }
    void beginSample(matchdoc::MatchDoc aggMatchDoc) override {
        int64_t &sampleValue = _curLayerSampleRefer->getReference(aggMatchDoc);
        int64_t &layerValue = _curLayerResultRefer->getReference(aggMatchDoc);
        layerValue = sampleValue;
        sampleValue = 0;
    }
    void endLayer(uint32_t sampleStep, double factor,
                  matchdoc::MatchDoc aggMatchDoc) override
    {
        int64_t &sampleValue = _curLayerSampleRefer->getReference(aggMatchDoc);
        int64_t &layerValue = _curLayerResultRefer->getReference(aggMatchDoc);
        int64_t &totalValue = _totalResultRefer->getReference(aggMatchDoc);
        totalValue += int64_t(factor * (layerValue + sampleValue * sampleStep));
        sampleValue = 0;
        layerValue = 0;
    }
    std::string getStrValue(matchdoc::MatchDoc aggMatchDoc) const override {
        int64_t result = _totalResultRefer->get(aggMatchDoc);
        return autil::StringUtil::toString(result);
    }
    suez::turing::AttributeExpression *getExpr() override {
        return NULL;
    }
    bool canCodegen() override {
        return true;
    }
private:
    matchdoc::Reference<int64_t> *_totalResultRefer;
    matchdoc::Reference<int64_t> *_curLayerResultRefer;
    matchdoc::Reference<int64_t> *_curLayerSampleRefer;
private:
    AUTIL_LOG_DECLARE();
};


} // namespace search
} // namespace isearch

