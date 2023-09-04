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

#include <memory>
#include <stddef.h>
#include <stdint.h>

#include "indexlib/misc/common.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/provider/FunctionProvider.h"
#include "suez/turing/expression/provider/ScoringProvider.h"
#include "suez/turing/expression/provider/SorterProvider.h"
#include "turing_ops_util/variant/Tracer.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace indexlib {
namespace partition {
class PartitionReaderSnapshot;
} // namespace partition
} // namespace indexlib
namespace matchdoc {
class MatchDocAllocator;
} // namespace matchdoc

namespace suez {
namespace turing {

class AttributeExpressionCreator;
class SuezCavaAllocator;

class ProviderParam {
public:
    ProviderParam(matchdoc::MatchDocAllocator *allocator_,
                  autil::mem_pool::Pool *pool_,
                  SuezCavaAllocator *cavaAllocator_,
                  Tracer *requestTracer_,
                  indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot_,
                  const KeyValueMap *kvpairs_,
                  int32_t rankTraceLevel_ = ISEARCH_TRACE_NONE,
                  AttributeExpressionCreator *attributeExpressionCreator_ = NULL)
        : allocator(allocator_)
        , pool(pool_)
        , cavaAllocator(cavaAllocator_)
        , requestTracer(requestTracer_)
        , rankTraceLevel(rankTraceLevel_)
        , attributeExpressionCreator(attributeExpressionCreator_)
        , partitionReaderSnapshot(partitionReaderSnapshot_)
        , kvpairs(kvpairs_) {}
    ProviderParam()
        : allocator(NULL)
        , pool(NULL)
        , cavaAllocator(nullptr)
        , requestTracer(NULL)
        , rankTraceLevel(ISEARCH_TRACE_NONE)
        , attributeExpressionCreator(NULL)
        , kvpairs(NULL) {}

public:
    matchdoc::MatchDocAllocator *allocator;
    autil::mem_pool::Pool *pool;
    SuezCavaAllocator *cavaAllocator;
    Tracer *requestTracer;
    int32_t rankTraceLevel;
    AttributeExpressionCreator *attributeExpressionCreator;
    indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot;
    const KeyValueMap *kvpairs;
};

class ProviderCreator {
public:
    ProviderCreator(const ProviderParam &param) : _param(param) {}
    ProviderCreator() {}
    virtual ~ProviderCreator() {}

private:
    ProviderCreator(const ProviderCreator &);
    ProviderCreator &operator=(const ProviderCreator &);

public:
    virtual ScoringProviderPtr createScoringProvider() {
        ScoringProviderPtr scoringProviderPtr(new ScoringProvider(_param.allocator,
                                                                  _param.pool,
                                                                  _param.cavaAllocator,
                                                                  _param.requestTracer,
                                                                  _param.partitionReaderSnapshot,
                                                                  _param.kvpairs));
        scoringProviderPtr->setupTraceRefer(_param.rankTraceLevel);
        scoringProviderPtr->setAttributeExpressionCreator(_param.attributeExpressionCreator);
        return scoringProviderPtr;
    }
    virtual SorterProviderPtr createSorterProvider() {
        SorterProviderPtr sorterProviderPtr(new SorterProvider(_param.allocator,
                                                               _param.pool,
                                                               _param.cavaAllocator,
                                                               _param.requestTracer,
                                                               _param.partitionReaderSnapshot,
                                                               _param.kvpairs));
        sorterProviderPtr->setupTraceRefer(_param.rankTraceLevel);
        sorterProviderPtr->setAttributeExpressionCreator(_param.attributeExpressionCreator);
        return sorterProviderPtr;
    }
    virtual FunctionProviderPtr createFunctionProvider() {
        // function provider not support require attribute, DO NOT set attributeExpressionCreator
        FunctionProviderPtr functionProviderPtr(new FunctionProvider(_param.allocator,
                                                                     _param.pool,
                                                                     _param.cavaAllocator,
                                                                     _param.requestTracer,
                                                                     _param.partitionReaderSnapshot,
                                                                     _param.kvpairs));
        functionProviderPtr->setupTraceRefer(_param.rankTraceLevel);
        return functionProviderPtr;
    }

private:
    ProviderParam _param;
};

SUEZ_TYPEDEF_PTR(ProviderCreator);

} // namespace turing
} // namespace suez
