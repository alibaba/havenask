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
#include "ha3/queryparser/NumberTermExpr.h"

#include <sstream>
#include <string>
#include <memory>

#include "ha3/common/NumberTerm.h"
#include "ha3/queryparser/QueryExprEvaluator.h"
#include "autil/Log.h"

using namespace std;
namespace isearch {
namespace queryparser {
AUTIL_LOG_SETUP(ha3, NumberTermExpr);

NumberTermExpr::NumberTermExpr(int64_t leftNumber, bool leftInclusive,
                   int64_t rightNumber, bool rightInclusive) 
{
    _leftNumber = leftNumber;
    _rightNumber = rightNumber;
    _leftInclusive = leftInclusive;
    _rightInclusive = rightInclusive;
}

NumberTermExpr::~NumberTermExpr() { 
}

void NumberTermExpr::setLeftNumber(int64_t leftNumber, bool leftInclusive) {
    _leftNumber = leftNumber;
    _leftInclusive = leftInclusive;
}

void NumberTermExpr::setRightNumber(int64_t rightNumber, bool rightInclusive) {
    _rightNumber = rightNumber;
    _rightInclusive = rightInclusive;    
}

void NumberTermExpr::evaluate(QueryExprEvaluator *qee) {
    qee->evaluateNumberExpr(this);
}

common::TermPtr NumberTermExpr::constructSearchTerm() {
    common::NumberTermPtr numberTermPtr(new common::NumberTerm(
                    _leftNumber, _leftInclusive, _rightNumber,
                    _rightInclusive, _indexName.c_str(),
                    _requiredFields, _boost, _secondaryChain));
    return dynamic_pointer_cast<common::Term>(numberTermPtr);
}

} // namespace queryparser
} // namespace isearch

