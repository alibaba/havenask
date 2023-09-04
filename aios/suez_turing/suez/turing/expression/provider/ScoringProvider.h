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

class ScorerProvider;
class SuezCavaAllocator;
class Tracer;

class ScoringProvider : public ProviderBase {
public:
    ScoringProvider(matchdoc::MatchDocAllocator *allocator,
                    autil::mem_pool::Pool *pool,
                    SuezCavaAllocator *cavaAllocator,
                    Tracer *requestTracer,
                    indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot,
                    const KeyValueMap *kvpairs);
    virtual ~ScoringProvider();

private:
    ScoringProvider(const ScoringProvider &);
    ScoringProvider &operator=(const ScoringProvider &);

public:
    const matchdoc::Reference<score_t> *getScoreReference() {
        return findVariableReferenceWithRawName<score_t>(SCORE_REF);
    }
    template <typename T>
    matchdoc::Reference<T> *requireAttribute(const std::string &attrName) {
        return ProviderBase::requireAttribute<T>(attrName, false);
    }
    matchdoc::ReferenceBase *requireAttributeWithoutType(const std::string &attrName) {
        return ProviderBase::requireAttributeWithoutType(attrName, false);
    }

    template <typename T>
    matchdoc::Reference<T> *declareVariable(const std::string &variName,
                                            bool needSerialize = false,
                                            bool needConstruct = matchdoc::ConstructTypeTraits<T>::NeedConstruct()) {
        return ProviderBase::declareVariable<T>(variName, needSerialize ? SL_VARIABLE : SL_NONE, needConstruct);
    }
    template <typename T>
    matchdoc::Reference<T> *declareSubVariable(const std::string &variName,
                                               bool needSerialize = false,
                                               bool needConstruct = matchdoc::ConstructTypeTraits<T>::NeedConstruct()) {
        return ProviderBase::declareSubVariable<T>(variName, needSerialize ? SL_VARIABLE : SL_NONE, needConstruct);
    }
    void setCavaScorerProvider(ScorerProvider *provider) { _cavaScorerProvider = provider; }
    ScorerProvider *getCavaScorerProvider() { return _cavaScorerProvider; }

    // for mock test
    void prepareMatchDoc(matchdoc::MatchDoc matchDoc);

private:
    ScorerProvider *_cavaScorerProvider;
};

SUEZ_TYPEDEF_PTR(ScoringProvider);

} // namespace turing
} // namespace suez
