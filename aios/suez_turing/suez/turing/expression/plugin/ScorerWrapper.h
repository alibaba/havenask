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
#include <string>
#include <vector>

#include "autil/Log.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/ExpressionEvaluator.h"
#include "suez/turing/expression/plugin/Scorer.h"
#include "suez/turing/expression/provider/ScoringProvider.h"

namespace suez {
namespace turing {

class ScorerWrapper {
public:
    ScorerWrapper(Scorer *scorer, matchdoc::Reference<score_t> *_scoreRef);
    ~ScorerWrapper();

private:
    ScorerWrapper(const ScorerWrapper &);
    ScorerWrapper &operator=(const ScorerWrapper &);

public:
    bool beginRequest(ScoringProvider *provider);
    void endRequest();
    score_t score(matchdoc::MatchDoc &matchDoc);
    void batchScore(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount);

    void setScoreRef(matchdoc::Reference<score_t> *scoreRef) { _scoreRef = scoreRef; }
    void setAttributeEvaluated() { _provider->setAttributeEvaluated(); }
    const std::string &getWarningInfo() { return _scorer->getWarningInfo(); }

private:
    void prepareMatchDoc(matchdoc::MatchDoc matchDoc) {
        ExpressionEvaluator<ExpressionVector> evaluator(_provider->getNeedEvaluateAttrs(),
                                                        _provider->getAllocator()->getSubDocAccessor());
        evaluator.evaluateExpressions(matchDoc);
        _provider->prepareTracer(matchDoc);
    }
    void batchPrepareMatchDoc(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) {
        ExpressionEvaluator<ExpressionVector> evaluator(_provider->getNeedEvaluateAttrs(),
                                                        _provider->getAllocator()->getSubDocAccessor());
        evaluator.batchEvaluateExpressions(matchDocs, matchDocCount);
        _provider->batchPrepareTracer(matchDocs, matchDocCount);
    }

private:
    ScoringProvider *_provider;
    Scorer *_scorer;
    matchdoc::Reference<score_t> *_scoreRef;

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(ScorerWrapper);

/////////////////////////////////////////////////////////////////

inline score_t ScorerWrapper::score(matchdoc::MatchDoc &matchDoc) {
    prepareMatchDoc(matchDoc);
    score_t s = _scorer->score(matchDoc);
    _scoreRef->set(matchDoc, s);
    return s;
}

inline void ScorerWrapper::batchScore(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) {
    ScorerParam scorerParam;
    scorerParam.matchDocs = matchDocs;
    scorerParam.scoreDocCount = matchDocCount;
    scorerParam.reference = _scoreRef;
    batchPrepareMatchDoc(scorerParam.matchDocs, scorerParam.scoreDocCount);
    _scorer->batchScore(scorerParam);
}

} // namespace turing
} // namespace suez
