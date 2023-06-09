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
#include "ha3/queryparser/AggregateParser.h"

#include <assert.h>
#include <stddef.h>
#include <sstream>

#include "suez/turing/expression/syntax/SyntaxExpr.h"

#include "ha3/common/AggFunDescription.h"
#include "ha3/common/AggregateDescription.h"
#include "ha3/common/FilterClause.h"
#include "ha3/common/RequestSymbolDefine.h"
#include "autil/Log.h"

using namespace std;
using namespace suez::turing;

using namespace isearch::common;

namespace isearch {
namespace queryparser {

AUTIL_LOG_SETUP(ha3, AggregateParser);

AggregateParser::AggregateParser() {
}

AggregateParser::~AggregateParser() { 
}

AggregateDescription* AggregateParser::createAggDesc() {
    return new AggregateDescription;
}

vector<string>* AggregateParser::createRangeExpr() {
    return new vector<string>;
}

vector<AggFunDescription*>* AggregateParser::createFuncDescs() {
    return new vector<AggFunDescription*>;
}

AggFunDescription* AggregateParser::createAggMaxFunc(SyntaxExpr *expr) {
    assert(expr);
    return new AggFunDescription(AGGREGATE_FUNCTION_MAX, expr);
}

AggFunDescription* AggregateParser::createAggMinFunc(SyntaxExpr *expr) {
    assert(expr);
    return new AggFunDescription(AGGREGATE_FUNCTION_MIN, expr);
}

AggFunDescription* AggregateParser::createAggCountFunc() {
    return new AggFunDescription(AGGREGATE_FUNCTION_COUNT, NULL);
}

AggFunDescription* AggregateParser::createAggSumFunc(SyntaxExpr *expr) {
    assert(expr);
    return new AggFunDescription(AGGREGATE_FUNCTION_SUM, expr);
}

AggFunDescription* AggregateParser::createAggDistinctCountFunc(SyntaxExpr *expr) {
    assert(expr);
    return new AggFunDescription(AGGREGATE_FUNCTION_DISTINCT_COUNT, expr);
}
 
FilterClause* AggregateParser::createFilterClause(SyntaxExpr *expr) {
    FilterClause* filterClause = new FilterClause(expr);
    if (expr) {
        filterClause->setOriginalString(expr->getExprString());
    }
    return filterClause;
}


} // namespace queryparser
} // namespace isearch

