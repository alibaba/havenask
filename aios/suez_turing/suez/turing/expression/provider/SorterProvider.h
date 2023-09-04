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

#include <string>

#include "indexlib/misc/common.h"
#include "matchdoc/Trait.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/provider/ProviderBase.h"
#include "turing_ops_util/util/DocTracer.h"

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
class ReferenceBase;
template <typename T>
class Reference;
} // namespace matchdoc

namespace suez {
namespace turing {
class SuezCavaAllocator;
class Tracer;

class SorterProvider : public ProviderBase {
public:
    SorterProvider(matchdoc::MatchDocAllocator *allocator,
                   autil::mem_pool::Pool *pool,
                   SuezCavaAllocator *cavaAllocator,
                   Tracer *requestTracer,
                   indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot,
                   const KeyValueMap *kvpairs,
                   kmonitor::MetricsReporter *metricsReporter = nullptr,
                   DocTracer *docTracerPtr = nullptr);
    ~SorterProvider();

private:
    SorterProvider(const SorterProvider &);
    SorterProvider &operator=(const SorterProvider &);

public:
    // only for test compatibility, DO NOT USE
    SorterProvider(matchdoc::MatchDocAllocator *allocator,
                   autil::mem_pool::Pool *pool,
                   Tracer *requestTracer,
                   indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot,
                   const KeyValueMap *kvpairs)
        : SorterProvider(allocator, pool, nullptr, requestTracer, partitionReaderSnapshot, kvpairs) {}

public:
    DocTracer *getDocTracer();
    template <typename T>
    matchdoc::Reference<T> *requireAttribute(const std::string &attrName) {
        return ProviderBase::requireAttribute<T>(attrName, true);
    }
    matchdoc::ReferenceBase *requireAttributeWithoutType(const std::string &attrName) {
        return ProviderBase::requireAttributeWithoutType(attrName, true);
    }

    template <typename T>
    matchdoc::Reference<T> *declareVariable(const std::string &variName,
                                            bool needSerialize = true,
                                            bool needConstruct = matchdoc::ConstructTypeTraits<T>::NeedConstruct()) {
        return ProviderBase::declareVariable<T>(variName, needSerialize ? SL_VARIABLE : SL_NONE, needConstruct);
    }
    template <typename T>
    matchdoc::Reference<T> *declareSubVariable(const std::string &variName,
                                               bool needSerialize = true,
                                               bool needConstruct = matchdoc::ConstructTypeTraits<T>::NeedConstruct()) {
        return ProviderBase::declareSubVariable<T>(variName, needSerialize ? SL_VARIABLE : SL_NONE, needConstruct);
    }

private:
    DocTracer *_docTracerPtr;
};
SUEZ_TYPEDEF_PTR(SorterProvider);

} // namespace turing
} // namespace suez
