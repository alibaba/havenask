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

#include <stdint.h>

#include "ha3/common/Term.h"
#include "ha3/queryparser/AtomicQueryExpr.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace queryparser {
class QueryExprEvaluator;
}  // namespace queryparser
}  // namespace isearch

namespace isearch {
namespace queryparser {

class NumberTermExpr : public AtomicQueryExpr
{
public:
    NumberTermExpr(int64_t leftNumber, bool leftInclusive,
                   int64_t rightNumber, bool rightInclusive);
    ~NumberTermExpr();
public:
    int64_t getLeftNumber() const {return _leftNumber;}
    int64_t getRightNumber() const {return _rightNumber;}
    int64_t getLeftInclusive() const {return _leftInclusive;}
    int64_t getRightInclusive() const {return _rightInclusive;}

    void setLeftNumber(int64_t leftNumber, bool leftInclusive);
    void setRightNumber(int64_t rightNumber, bool rightInclusive);

    void evaluate(QueryExprEvaluator *qee);
    common::TermPtr constructSearchTerm();
private:
    int64_t _leftNumber;
    int64_t _rightNumber;
    bool _leftInclusive;
    bool _rightInclusive;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace queryparser
} // namespace isearch

