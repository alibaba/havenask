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
#include "ha3/queryparser/MultiTermQueryExpr.h"

#include <iosfwd>

#include "autil/Log.h"
#include "ha3/common/Term.h"
#include "ha3/isearch.h"
#include "ha3/queryparser/AtomicQueryExpr.h"
#include "ha3/queryparser/QueryExprEvaluator.h"

using namespace std;
using namespace isearch::common;

namespace isearch {
namespace queryparser {
AUTIL_LOG_SETUP(ha3, MultiTermQueryExpr);

MultiTermQueryExpr::MultiTermQueryExpr(QueryOperator opExpr)
    : _defaultOP(opExpr)
    , _minShoudMatch(1) {}

MultiTermQueryExpr::~MultiTermQueryExpr() {
    for (TermExprArray::const_iterator it = _termQueryExprs.begin(); it != _termQueryExprs.end();
         it++) {
        delete (*it);
    }
}

void MultiTermQueryExpr::addTermQueryExpr(AtomicQueryExpr *expr) {
    _termQueryExprs.push_back(expr);
}

void MultiTermQueryExpr::setIndexName(const std::string &indexName) {
    for (TermExprArray::const_iterator it = _termQueryExprs.begin(); it != _termQueryExprs.end();
         it++) {
        (*it)->setIndexName(indexName);
    }
}

void MultiTermQueryExpr::setRequiredFields(const common::RequiredFields &requiredFields) {
    for (TermExprArray::const_iterator it = _termQueryExprs.begin(); it != _termQueryExprs.end();
         it++) {
        (*it)->setRequiredFields(requiredFields);
    }
}

void MultiTermQueryExpr::evaluate(QueryExprEvaluator *qee) {
    qee->evaluateMultiTermExpr(this);
}

} // namespace queryparser
} // namespace isearch
