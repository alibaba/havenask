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
#include "suez/turing/expression/framework/RankAttributeExpression.h"

#include <assert.h>
#include <string.h>

#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"

namespace matchdoc {
template <typename T>
class Reference;
} // namespace matchdoc

namespace suez {
namespace turing {
class Scorer;
class ScoringProvider;

AUTIL_LOG_SETUP(expression, RankAttributeExpression);

RankAttributeExpression::RankAttributeExpression(Scorer *scorer, matchdoc::Reference<score_t> *scoreRef)
    : _rankScorer(scorer, scoreRef) {
    _ref = scoreRef;
}

RankAttributeExpression::~RankAttributeExpression() {}

bool RankAttributeExpression::allocate(matchdoc::MatchDocAllocator *allocator) {
    if (!_ref) {
        auto scoreRef = allocator->declareWithConstructFlagDefaultGroup<score_t>(SCORE_REF, false, SL_CACHE);
        if (scoreRef) {
            _ref = scoreRef;
            _rankScorer.setScoreRef(scoreRef);
        }
    }
    return (_ref != NULL);
}

bool RankAttributeExpression::evaluate(matchdoc::MatchDoc matchDoc) {
    score_t s = _rankScorer.score(matchDoc);
    this->setValue(s);
    return true;
}

bool RankAttributeExpression::batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) {
    _rankScorer.batchScore(matchDocs, matchDocCount);
    return true;
}

bool RankAttributeExpression::beginRequest(ScoringProvider *provider) { return _rankScorer.beginRequest(provider); }

void RankAttributeExpression::endRequest() { _rankScorer.endRequest(); }

bool RankAttributeExpression::operator==(const AttributeExpression *checkExpr) const {
    const RankAttributeExpression *expr = dynamic_cast<const RankAttributeExpression *>(checkExpr);
    return expr != NULL;
}

void RankAttributeExpression::setEvaluated() {
    assert(getReference());
    AttributeExpressionTyped<score_t>::setEvaluated();
    _rankScorer.setAttributeEvaluated();
}

} // namespace turing
} // namespace suez
