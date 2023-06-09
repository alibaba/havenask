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

#include "matchdoc/Reference.h"

#include "ha3/cava/scorer/CompoundScorerDef.h"
#include "ha3/isearch.h"

namespace isearch {
namespace compound_scorer {

template<class T>
class PSWorker {
public:
    PSWorker(const matchdoc::Reference<T>* ref,bool bASC,
             matchdoc::Reference<isearch::common::Tracer>* traceRef)
        : _ref(ref)
        , _bASC(bASC)
        , _traceRefer(traceRef)
    {
    }
    bool operator()(score_t &PS, matchdoc::MatchDoc &matchDoc);
private:
    const matchdoc::Reference<T> *_ref;
    bool _bASC;
    matchdoc::Reference<isearch::common::Tracer> *_traceRefer;
};

template<class T>
bool PSWorker<T>::operator()(score_t &PS, matchdoc::MatchDoc &matchDoc) {
    T fieldVal = _ref->get(matchDoc);
    if (fieldVal == getNullValue<T>()) {
        PS = _bASC ? SCORE_MAX : SCORE_MIN;
        RANK_TRACE(DEBUG, matchDoc, "after PSScore: %f", PS);
        return false;
    }
    PS = fieldVal;
    RANK_TRACE(DEBUG, matchDoc, "after PSScore: %f", PS);
    return true;
}

} // namespace compound_scorer
} // namespace isearch

