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
#include "suez/turing/expression/provider/ProviderBase.h"

#include "suez/turing/expression/framework/AttributeExpressionCreator.h"

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

namespace suez {
namespace turing {
class SuezCavaAllocator;

using namespace indexlib::partition;

AUTIL_LOG_SETUP(expression, ProviderBase);

ProviderBase::ProviderBase(matchdoc::MatchDocAllocator *allocator,
                           autil::mem_pool::Pool *pool,
                           SuezCavaAllocator *cavaAllocator,
                           Tracer *requestTracer,
                           PartitionReaderSnapshot *partitionReaderSnapshot,
                           const KeyValueMap *kvpairs,
                           kmonitor::MetricsReporter *metricsReporter)
    : _allocator(allocator)
    , _pool(pool)
    , _cavaAllocator(cavaAllocator)
    , _partitionReaderSnapshot(partitionReaderSnapshot)
    , _isSubScope(false)
    , _kvpairs(kvpairs)
    , _metricsReporter(metricsReporter)
    , _attributeExpressionCreator(NULL)
    , _traceRefer(NULL)
    , _requestTracer(requestTracer)
    , _rankTraceLevel(ISEARCH_TRACE_NONE)
    , _declareVariable(false) {}

ProviderBase::~ProviderBase() {}

AttributeExpression *ProviderBase::doRequireAttributeWithoutType(const std::string &attrName) {
    if (_attributeExpressionCreator) {
        auto attrExpr = _attributeExpressionCreator->createAtomicExpr(attrName);
        if (!attrExpr) {
            return NULL;
        }
        if (!attrExpr->allocate(_allocator)) {
            return NULL;
        }
        return attrExpr;
    }
    return NULL;
}

matchdoc::ReferenceBase *ProviderBase::requireAttributeWithoutType(const std::string &attrName, bool needSerialize) {
    // in proxy or qrs
    if (!_partitionReaderSnapshot) {
        auto name = _convertor.nameToName(attrName);
        return _allocator->findReferenceWithoutType(name);
    }
    auto attrExpr = doRequireAttributeWithoutType(attrName);
    if (attrExpr) {
        auto *refer = attrExpr->getReferenceBase();
        if (needSerialize) {
            refer->setSerializeLevel(SL_CACHE);
        }
        _needEvaluateAttrs.push_back(attrExpr);
        return refer;
    }
    return NULL;
}

} // namespace turing
} // namespace suez
