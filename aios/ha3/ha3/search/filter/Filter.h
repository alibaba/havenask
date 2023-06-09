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

#include <assert.h>
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace suez {
namespace turing {
class AttributeExpressionCreatorBase;
class SyntaxExpr;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace search {

class Filter
{
public:
    Filter(suez::turing::AttributeExpressionTyped<bool> *attributeExpr);
    ~Filter();
private:
    Filter(const Filter &filter);
public:
    inline bool pass(matchdoc::MatchDoc doc);

    inline bool needFilterSubDoc() const {
        return _attributeExpr->isSubExpression();
    }

    suez::turing::AttributeExpression* getAttributeExpression() const {
        return _attributeExpr;
    }
    void setAttributeExpression(suez::turing::AttributeExpressionTyped<bool> *attributeExpr);

    void updateExprEvaluatedStatus();
public:
    static Filter *createFilter(
            const suez::turing::SyntaxExpr *filterExpr,
            suez::turing::AttributeExpressionCreatorBase *attributeExpressionCreator,
            autil::mem_pool::Pool *pool);

private:
    suez::turing::AttributeExpressionTyped<bool> *_attributeExpr;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<Filter> FilterPtr;

inline bool Filter::pass(matchdoc::MatchDoc doc) {
    assert(_attributeExpr);
    return _attributeExpr->evaluateAndReturn(doc);
}

} // namespace search
} // namespace isearch
