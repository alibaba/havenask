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

#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/queryparser/AndQueryExpr.h"
#include "ha3/queryparser/AtomicQueryExpr.h"

namespace isearch {
namespace common {
struct RequiredFields;
} // namespace common
namespace queryparser {
class QueryExprEvaluator;
} // namespace queryparser
} // namespace isearch

namespace isearch {
namespace queryparser {

class AndTermQueryExpr : public AtomicQueryExpr {
public:
    AndTermQueryExpr(AtomicQueryExpr *a, AtomicQueryExpr *b);
    ~AndTermQueryExpr();

public:
    void setIndexName(const std::string &indexName) override;
    void evaluate(QueryExprEvaluator *qee) override;
    void setRequiredFields(const common::RequiredFields &requiredFields) override;
    void setLabel(const std::string &label) override {
        _and.setLabel(label);
    }
    std::string getLabel() const override {
        return _and.getLabel();
    }

private:
    AndQueryExpr _and;

private:
    AUTIL_LOG_DECLARE();
};

// TYPEDEF_PTR(AndTermQueryExpr);

} // namespace queryparser
} // namespace isearch
