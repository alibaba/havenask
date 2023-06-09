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
#include <string>

#include "alog/Logger.h"
#include "autil/Hyperloglog.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "ha3/search/AggregateFunction.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"  // IWYU pragma: keep
#include "matchdoc/Reference.h"
#include "suez/turing/expression/common.h"
#include "sys/types.h"

namespace suez {
namespace turing {
class AttributeExpression;
template <typename T> class AttributeExpressionTyped;
}  // namespace turing
}  // namespace suez

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

namespace isearch {
namespace search {

template <typename ValueType>
inline void hllCtxAddElement(autil::HllCtx &hllCtx, const ValueType &value,
                             autil::mem_pool::Pool *pool) {
    autil::Hyperloglog::hllCtxAdd(&hllCtx, (const unsigned char *)&value,
                                  sizeof(ValueType), pool);
    return;
}

template <>
inline void hllCtxAddElement<autil::MultiChar>(autil::HllCtx &hllCtx,
                                               const autil::MultiChar &value,
                                               autil::mem_pool::Pool *pool) {
    autil::Hyperloglog::hllCtxAdd(&hllCtx, (const unsigned char *)value.data(),
                                  value.size(), pool);
    return;
}

template <>
inline void hllCtxAddElement<bool>(autil::HllCtx &hllCtx,
                                               const bool &value,
                                               autil::mem_pool::Pool *pool) {
    unsigned char valueChar = (char)value;
    autil::Hyperloglog::hllCtxAdd(&hllCtx, (const unsigned char *)& valueChar,
                                  1, pool);
    return;
}


template <typename ExprType, typename SingleExprType = ExprType>
class DistinctCountAggregateFunction : public AggregateFunction
{
public:
    DistinctCountAggregateFunction(const std::string &parameter,
                                   suez::turing::AttributeExpressionTyped<ExprType> *attriExpr)
        : AggregateFunction("distinct_count", parameter, vt_int64)
        , _expr(attriExpr)
        , _totalResultRefer(NULL)
        , _curLayerResultRefer(NULL)
        , _curLayerSampleRefer(NULL)
        , _pool(NULL)
    {}

    ~DistinctCountAggregateFunction() {}

private:
    DistinctCountAggregateFunction(const DistinctCountAggregateFunction &);
    DistinctCountAggregateFunction& operator=(const DistinctCountAggregateFunction &);

public:
    void calculate(matchdoc::MatchDoc matchDoc,
                   matchdoc::MatchDoc aggMatchDoc) override {
        ExprType value = _expr->evaluateAndReturn(matchDoc);
        calculateDistinctCount(value, aggMatchDoc);
    }

    void declareResultReference(matchdoc::MatchDocAllocator *allocator) override {
        _totalResultRefer =
            allocator->declareWithConstructFlagDefaultGroup<autil::HllCtx>(
                    toString(), false);
        _curLayerResultRefer =
            allocator->declareWithConstructFlagDefaultGroup<autil::HllCtx>(
                    toString() + ".layerResult", false);
        _curLayerSampleRefer =
            allocator->declareWithConstructFlagDefaultGroup<autil::HllCtx>(
                    toString() + ".layerSample", false);
        assert(_totalResultRefer);
        assert(_curLayerResultRefer);
        assert(_curLayerSampleRefer);
    }

    void initFunction(matchdoc::MatchDoc aggMatchDoc,
                      autil::mem_pool::Pool *pool) override {
        autil::HllCtx *hllCtxTotal =
            autil::Hyperloglog::hllCtxCreate(HLL_DENSE, pool);
        autil::HllCtx *hllCtxCurLayerResult =
            autil::Hyperloglog::hllCtxCreate(HLL_DENSE, pool);
        autil::HllCtx *hllCtxCurLayerSample =
            autil::Hyperloglog::hllCtxCreate(HLL_DENSE, pool);

        _pool = pool;
        _totalResultRefer->set(aggMatchDoc, *hllCtxTotal);
        _curLayerResultRefer->set(aggMatchDoc, *hllCtxCurLayerResult);
        _curLayerSampleRefer->set(aggMatchDoc, *hllCtxCurLayerSample);

        return;
    }

    matchdoc::ReferenceBase *getReference(uint id = 0) const override {
        return _totalResultRefer;
    }

    void estimateResult(double factor, matchdoc::MatchDoc aggMatchDoc) override {
        //can't do estimate
    }

    void beginSample(matchdoc::MatchDoc aggMatchDoc) override {
        //useless:can't find call to this function
    }

    void endLayer(uint32_t sampleStep, double factor,
                  matchdoc::MatchDoc aggMatchDoc) override {
        //merge sample and layer, if step > 1, sample is not precise
        autil::HllCtx &sampleValue =
            _curLayerSampleRefer->getReference(aggMatchDoc);
        autil::HllCtx &layerValue =
            _curLayerResultRefer->getReference(aggMatchDoc);
        autil::HllCtx &totalValue = _totalResultRefer->getReference(aggMatchDoc);
        int res = autil::Hyperloglog::hllCtxMerge(&totalValue, &layerValue, _pool);
        if (res < 0) {
            AUTIL_LOG(ERROR, "hyperloglog merge layer failed");
        }
        res = autil::Hyperloglog::hllCtxMerge(&totalValue, &sampleValue, _pool);
        if (res < 0) {
            AUTIL_LOG(ERROR, "hyperloglog merge sample failed");
        }

        autil::Hyperloglog::hllCtxReset(&sampleValue);
        autil::Hyperloglog::hllCtxReset(&layerValue);
        return;

    }

    std::string getStrValue(matchdoc::MatchDoc aggMatchDoc) const override {
        autil::HllCtx &hllCtx = _totalResultRefer->getReference(aggMatchDoc);
        return autil::StringUtil::toString(hllCtx);
    }

    virtual suez::turing::AttributeExpression *getExpr() override {
        return _expr;
    }

  private:
    void calculateDistinctCount(const SingleExprType &attrExprValue,
                                matchdoc::MatchDoc aggMatchDoc) {

        autil::HllCtx &hllCtx = _curLayerSampleRefer->getReference(aggMatchDoc);
        hllCtxAddElement<SingleExprType>(hllCtx, attrExprValue, _pool);
        return;
    }

    void calculateDistinctCount(
        const autil::MultiValueType<SingleExprType> &attrExprValue,
        matchdoc::MatchDoc aggMatchDoc) {
        for (size_t i = 0; i < attrExprValue.size(); i++) {
            calculateDistinctCount(attrExprValue[i], aggMatchDoc);
        }
        return;
    }
/*
    void hllCtxAddElement(autil::HllCtx &hllCtx, const SingleExprType &value) {
        autil::Hyperloglog::hllCtxAdd(&hllCtx, (const unsigned char *)&value,
                                      sizeof(SingleExprType), _pool);
        return;
    }

    template <>
    void hllCtxAddElement<autil::MultiChar>(autil::HllCtx &hllCtx,
                                            const autil::MultiChar &value) {
        autil::Hyperloglog::hllCtxAdd(
            &hllCtx, (const unsigned char *)value.data(), value.size(), _pool);
        return;
    }
*/
  private:
    suez::turing::AttributeExpressionTyped<ExprType> *_expr;
    matchdoc::Reference<autil::HllCtx> *_totalResultRefer;
    matchdoc::Reference<autil::HllCtx> *_curLayerResultRefer;
    matchdoc::Reference<autil::HllCtx> *_curLayerSampleRefer;
    autil::mem_pool::Pool* _pool;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_2(ha3, DistinctCountAggregateFunction, ExprType, SingleExprType);

} // namespace search
} // namespace isearch
