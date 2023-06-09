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

#include "ha3/search/FilterWrapper.h"
#include "ha3/sql/ops/scan/QueryExecutorExpressionWrapper.h"

namespace isearch {
namespace sql {

class QueryExprFilterWrapper : public search::FilterWrapper {
public:
    QueryExprFilterWrapper(const std::vector<suez::turing::AttributeExpression *> &queryExprVec)
        : _queryExprVec(queryExprVec)
    {}
    ~QueryExprFilterWrapper() {}
public:
    void resetFilter() override {
        for (auto expr: _queryExprVec) {
            auto *queryExpr = dynamic_cast<QueryExecutorExpressionWrapper *>(expr);
            if (queryExpr) {
                queryExpr->reset();
            }
        }
    }
private:
    std::vector<suez::turing::AttributeExpression *> _queryExprVec;
};
}
}
