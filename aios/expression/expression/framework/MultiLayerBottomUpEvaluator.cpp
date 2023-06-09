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
#include "expression/framework/MultiLayerBottomUpEvaluator.h"
#include "expression/framework/BottomUpEvaluator.h"

using namespace std;
using namespace matchdoc;

namespace expression {
AUTIL_LOG_SETUP(expression, MultiLayerBottomUpEvaluator);

MultiLayerBottomUpEvaluator::MultiLayerBottomUpEvaluator() {
}

MultiLayerBottomUpEvaluator::~MultiLayerBottomUpEvaluator() {
}

void MultiLayerBottomUpEvaluator::init(const vector<AttributeExpressionVec>& expressionLayers,
                                       SubDocAccessor *subDocAccessor)
{
    vector<expressionid_t> noNeedEvalExprIds;
    for (size_t i = 0; i < expressionLayers.size(); ++i) {
        if (expressionLayers[i].empty()) {
            continue;
        }
        BottomUpEvaluatorPtr evaluator(new BottomUpEvaluator(noNeedEvalExprIds));
        evaluator->init(expressionLayers[i], subDocAccessor);
        vector<expressionid_t> toEvalExprIds;
        evaluator->getToEvaluateExprIds(toEvalExprIds);
        noNeedEvalExprIds.insert(noNeedEvalExprIds.end(),
                toEvalExprIds.begin(), toEvalExprIds.end());
        _evaluators.push_back(evaluator);
    }
}

void MultiLayerBottomUpEvaluator::evaluate(MatchDoc matchDoc) {
    for (size_t i = 0; i < _evaluators.size(); ++i) {
        _evaluators[i]->evaluate(matchDoc);
    }
}

void MultiLayerBottomUpEvaluator::batchEvaluate(MatchDoc *matchDocs, uint32_t docCount) {
    for (size_t i = 0; i < _evaluators.size(); ++i) {
        _evaluators[i]->batchEvaluate(matchDocs, docCount);
    }
}
}
