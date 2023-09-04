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
#include "ha3/queryparser/AndTermQueryExpr.h"

#include <assert.h>

#include "autil/Log.h"
#include "ha3/queryparser/AndQueryExpr.h"
#include "ha3/queryparser/AtomicQueryExpr.h"
#include "ha3/queryparser/QueryExprEvaluator.h"

namespace isearch {
namespace common {
struct RequiredFields;
} // namespace common
} // namespace isearch

namespace isearch {
namespace queryparser {
AUTIL_LOG_SETUP(ha3, AndTermQueryExpr);

AndTermQueryExpr::AndTermQueryExpr(AtomicQueryExpr *a, AtomicQueryExpr *b)
    : _and(a, b) {}

AndTermQueryExpr::~AndTermQueryExpr() {}

void AndTermQueryExpr::setIndexName(const std::string &indexName) {
    AtomicQueryExpr *a = static_cast<AtomicQueryExpr *>(_and.getLeftExpr());
    AtomicQueryExpr *b = static_cast<AtomicQueryExpr *>(_and.getRightExpr());
    assert(a);
    assert(b);
    a->setIndexName(indexName);
    b->setIndexName(indexName);
}

void AndTermQueryExpr::setRequiredFields(const common::RequiredFields &requiredFields) {
    AtomicQueryExpr *a = static_cast<AtomicQueryExpr *>(_and.getLeftExpr());
    AtomicQueryExpr *b = static_cast<AtomicQueryExpr *>(_and.getRightExpr());
    assert(a);
    assert(b);
    a->setRequiredFields(requiredFields);
    b->setRequiredFields(requiredFields);
}

void AndTermQueryExpr::evaluate(QueryExprEvaluator *qee) {
    qee->evaluateAndExpr(&_and);
}

} // namespace queryparser
} // namespace isearch
