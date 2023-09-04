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

#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/isearch.h"
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

class MultiTermQueryExpr : public AtomicQueryExpr {
public:
    typedef std::vector<AtomicQueryExpr *> TermExprArray;

public:
    MultiTermQueryExpr(QueryOperator opExpr);
    ~MultiTermQueryExpr();

public:
    void setIndexName(const std::string &indexName);
    void setRequiredFields(const common::RequiredFields &requiredFields);
    void evaluate(QueryExprEvaluator *qee);

public:
    QueryOperator getOpExpr() const {
        return _defaultOP;
    }
    TermExprArray &getTermExprs() {
        return _termQueryExprs;
    }

    void addTermQueryExpr(AtomicQueryExpr *expr);
    void setMinShouldMatch(uint32_t i) {
        _minShoudMatch = i;
    }
    uint32_t getMinShouldMatch() const {
        return _minShoudMatch;
    }

private:
    TermExprArray _termQueryExprs;
    QueryOperator _defaultOP;
    uint32_t _minShoudMatch;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MultiTermQueryExpr> MultiTermQueryExprPtr;

} // namespace queryparser
} // namespace isearch
