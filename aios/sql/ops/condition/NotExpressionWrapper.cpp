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
#include "sql/ops/condition/NotExpressionWrapper.h"

#include <assert.h>
#include <cstddef>

#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

using namespace std;

namespace sql {

NotExpressionWrapper::NotExpressionWrapper(AttributeExpressionTyped<bool> *expression)
    : _expression(expression) {}

NotExpressionWrapper::~NotExpressionWrapper() {
    _expression = NULL;
}

bool NotExpressionWrapper::evaluateAndReturn(matchdoc::MatchDoc matchDoc) {
    assert(_expression);
    return !_expression->evaluateAndReturn(matchDoc);
}

} // namespace sql
