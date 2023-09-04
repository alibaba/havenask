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
#include "suez/turing/expression/framework/SimpleAttributeExpressionCreator.h"

#include <algorithm>
#include <cstddef>

#include "matchdoc/MatchDocAllocator.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, SimpleAttributeExpressionCreator);

SimpleAttributeExpressionCreator::SimpleAttributeExpressionCreator(autil::mem_pool::Pool *pool,
                                                                   matchdoc::MatchDocAllocator *matchAllocator)
    : AttributeExpressionCreatorBase(pool), _allocator(matchAllocator) {}

SimpleAttributeExpressionCreator::~SimpleAttributeExpressionCreator() {}

AttributeExpression *SimpleAttributeExpressionCreator::createAttributeExpression(const SyntaxExpr *syntaxExpr,
                                                                                 bool needValidate) {
    string exprString = syntaxExpr->getExprString();
    auto vt = syntaxExpr->getExprResultType();
    bool isMulti = syntaxExpr->isMultiValue();
    return createAttributeExpression(exprString, vt, isMulti);
}

AttributeExpression *SimpleAttributeExpressionCreator::createAttributeExpression(const string &exprString,
                                                                                 VariableType vt_type,
                                                                                 bool isMulti) {
#define SIMPLE_ATTRIBUTE_CREATOR_HELPER(vt_type)                                                                       \
    case vt_type: {                                                                                                    \
        if (isMulti) {                                                                                                 \
            typedef VariableTypeTraits<vt_type, true>::AttrExprType T;                                                 \
            auto ref = _allocator->findReference<T>(exprString);                                                       \
            if (!ref) {                                                                                                \
                return NULL;                                                                                           \
            }                                                                                                          \
            return createAttributeExpressionTyped(ref, exprString);                                                    \
        } else {                                                                                                       \
            typedef VariableTypeTraits<vt_type, false>::AttrExprType T;                                                \
            auto ref = _allocator->findReference<T>(exprString);                                                       \
            if (!ref) {                                                                                                \
                return NULL;                                                                                           \
            }                                                                                                          \
            return createAttributeExpressionTyped(ref, exprString);                                                    \
        }                                                                                                              \
    }
    switch (vt_type) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL(SIMPLE_ATTRIBUTE_CREATOR_HELPER);
        SIMPLE_ATTRIBUTE_CREATOR_HELPER(vt_string);
    default:
        assert(false);
        break;
    }
#undef SIMPLE_ATTRIBUTE_CREATOR_HELPER
    return NULL;
}

} // namespace turing
} // namespace suez
