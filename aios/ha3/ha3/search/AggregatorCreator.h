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
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ErrorResult.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/AggSamplerConfigInfo.h"
#include "suez/turing/common/QueryResource.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace suez {
namespace turing {
class AttributeExpressionCreatorBase;
class SuezCavaAllocator;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace common {
class AggFunDescription;
class AggregateClause;
class AggregateDescription;
}  // namespace common

namespace search {
class AggregateFunction;
class Aggregator;
class Filter;

class AggregatorCreator
{
public:
    AggregatorCreator(suez::turing::AttributeExpressionCreatorBase *attrExprCreator, 
                      autil::mem_pool::Pool *pool,
                      suez::turing::QueryResource *queryResource = NULL,
                      common::Tracer *tracer = NULL);
    ~AggregatorCreator();
public:
    Aggregator* createAggregator(const common::AggregateClause* aggClause);
    common::ErrorResult getErrorResult() { return _errorResult; }
    void setAggSamplerConfigInfo(const config::AggSamplerConfigInfo &aggSamplerConfigInfo) {
        _aggSamplerConfigInfo = aggSamplerConfigInfo;
    }
private:
    AggregateFunction *createAggregateFunction(
            const common::AggFunDescription *aggFunDes);
    template<typename ResultType, typename T>
    AggregateFunction *doCreateAggregateFunction(
            const common::AggFunDescription *aggFunDes);
    AggregateFunction *
    doCreateDistinctCountAggFunction(const common::AggFunDescription *aggFunDes);

    Aggregator *
    createSingleAggregator(const common::AggregateDescription *aggDes);
    template <typename KeyType>
    Aggregator *doCreateSingleAggregatorDelegator(
        const common::AggregateDescription *aggDes);
    template <typename KeyType, typename ExprType, typename GroupMapType>
    Aggregator *
    doCreateSingleAggregator(const common::AggregateDescription *aggDes);

    Filter *createAggregateFilter(const common::AggregateDescription *aggDes);

  private:
    suez::turing::AttributeExpressionCreatorBase *_attrExprCreator;
    autil::mem_pool::Pool *_pool;
    common::ErrorResult _errorResult;
    config::AggSamplerConfigInfo _aggSamplerConfigInfo;
  private:
    suez::turing::SuezCavaAllocator *_cavaAllocator;
    suez::turing::QueryResource *_queryResource;
    suez::turing::Tracer *_tracer;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AggregatorCreator> AggregatorCreatorPtr;

} // namespace search
} // namespace isearch

