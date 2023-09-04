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
#include "suez/turing/expression/framework/ComboAttributeExpression.h"

#include <cstddef>
#include <vector>

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, ComboAttributeExpression);

ComboAttributeExpression::ComboAttributeExpression() {}

ComboAttributeExpression::~ComboAttributeExpression() { _exprs.clear(); }

bool ComboAttributeExpression::evaluate(matchdoc::MatchDoc matchDoc) {
    for (ExpressionVector::iterator it = _exprs.begin(); it != _exprs.end(); it++) {
        if (!(*it)->evaluate(matchDoc)) {
            return false;
        }
    }
    return true;
}

bool ComboAttributeExpression::batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) {
    bool ret = true;
    for (size_t i = 0; i < _exprs.size(); ++i) {
        ret &= _exprs[i]->batchEvaluate(matchDocs, matchDocCount);
    }
    return ret;
}

void ComboAttributeExpression::addExpression(AttributeExpression *expr) {
    assert(expr);
    this->andIsDeterministic(expr->isDeterministic());
    _exprs.push_back(expr);
}

const ExpressionVector &ComboAttributeExpression::getExpressions() const { return _exprs; }

void ComboAttributeExpression::setEvaluated() {
    for (ExpressionVector::iterator it = _exprs.begin(); it != _exprs.end(); it++) {
        (*it)->setEvaluated();
    }
}

} // namespace turing
} // namespace suez
