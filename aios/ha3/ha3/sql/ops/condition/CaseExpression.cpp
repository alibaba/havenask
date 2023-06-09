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
#include "ha3/sql/ops/condition/CaseExpression.h"

#include <algorithm>
#include <cstddef>

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

namespace isearch {
namespace sql {

using namespace std;
using namespace suez::turing;

AttributeExpression *
CaseExpressionCreator::createCaseExpression(suez::turing::VariableType resultType,
                                            const std::vector<AttributeExpression *> &exprs,
                                            autil::mem_pool::Pool *pool) {
    // TODO error log
    if (1 != (exprs.size() % 2)) {
        return nullptr;
    }

#define CREATE_CASE_EXPR(vt_type)                                                                  \
    case (vt_type): {                                                                              \
        using OutputType = VariableTypeTraits<vt_type, false>::AttrExprType;                       \
        vector<CaseTuple<OutputType>> caseTuples;                                                  \
        OutputExpr<OutputType> *elseExpr;                                                          \
        size_t i;                                                                                  \
        for (i = 0; i < exprs.size() - 1; i += 2) {                                                \
            CaseTuple<OutputType> caseTuple;                                                       \
            caseTuple.whenExpr = dynamic_cast<AttributeExpressionTyped<bool> *>(exprs[i]);         \
            if (caseTuple.whenExpr == nullptr) {                                                   \
                return nullptr;                                                                    \
            }                                                                                      \
            caseTuple.thenExpr = CreateOutputExpr<OutputType>(exprs[i + 1], pool);                 \
            if (caseTuple.thenExpr == nullptr) {                                                   \
                return nullptr;                                                                    \
            }                                                                                      \
            caseTuples.emplace_back(caseTuple);                                                    \
        }                                                                                          \
        elseExpr = CreateOutputExpr<OutputType>(exprs[i], pool);                                   \
        if (elseExpr == nullptr) {                                                                 \
            return nullptr;                                                                        \
        }                                                                                          \
        return POOL_NEW_CLASS(pool, CaseExpression<OutputType>, caseTuples, elseExpr);             \
    }

    switch (resultType) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL_AND_STRING(CREATE_CASE_EXPR);
    default:
        return nullptr;
    }
#undef CREATE_CASE_EXPR
}

} // namespace sql
} // namespace isearch
