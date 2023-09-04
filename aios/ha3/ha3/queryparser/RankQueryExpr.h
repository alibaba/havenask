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

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/queryparser/BinaryQueryExpr.h"

namespace isearch {
namespace queryparser {
class QueryExpr;
class QueryExprEvaluator;
} // namespace queryparser
} // namespace isearch

namespace isearch {
namespace queryparser {

class RankQueryExpr : public BinaryQueryExpr {
public:
    RankQueryExpr(QueryExpr *a, QueryExpr *b)
        : BinaryQueryExpr(a, b) {}
    ~RankQueryExpr() {}

public:
    void evaluate(QueryExprEvaluator *qee);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace queryparser
} // namespace isearch
