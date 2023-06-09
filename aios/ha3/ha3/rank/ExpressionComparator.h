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

#include "matchdoc/MatchDoc.h"

#include "ha3/rank/Comparator.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace suez {
namespace turing {
template <typename T> class AttributeExpressionTyped;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace rank {

template <typename T>
class ExpressionComparator : public Comparator
{
public:
    ExpressionComparator(suez::turing::AttributeExpressionTyped<T> *expr, bool flag = false) {
        _expr = expr;
        _flag = flag;
    }
    ~ExpressionComparator() {}
private:
    ExpressionComparator(const ExpressionComparator &);
    ExpressionComparator& operator=(const ExpressionComparator &);
public:
    bool compare(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const  override {
        if (_flag) {
            return _expr->evaluateAndReturn(b) < _expr->evaluateAndReturn(a);
        } else {
            return _expr->evaluateAndReturn(a) < _expr->evaluateAndReturn(b);
        }
    }
    std::string getType() const  override { return "expression"; }

    void setReverseFlag(bool flag) { _flag = flag; }
    bool getReverseFlag() const { return _flag; }

private:
    suez::turing::AttributeExpressionTyped<T> *_expr;
    bool _flag;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace rank
} // namespace isearch

