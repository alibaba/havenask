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
#include <new>
#include <stddef.h>
#include <stdint.h>
#include <vector>

#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

namespace isearch {
namespace sql {
template <typename OutputType>
class OutputExpr {
public:
    OutputExpr() {}
    virtual ~OutputExpr() {}
    virtual OutputType evaluateAndReturn(matchdoc::MatchDoc matchDoc) = 0;
    virtual bool operator==(const OutputExpr<OutputType> *checkExpr) const {
        assert(false);
        return false;
    }
};

template <typename OutputType, typename ExprType>
class OutputExprTyped : public OutputExpr<OutputType> {
public:
    OutputExprTyped(suez::turing::AttributeExpressionTyped<ExprType> *expr)
        : _expr(expr) {}
    ~OutputExprTyped() {}
    OutputType evaluateAndReturn(matchdoc::MatchDoc matchDoc) override {
        return (OutputType)_expr->evaluateAndReturn(matchDoc);
    }
    bool operator==(const OutputExpr<OutputType> *checkExpr) const override {
        const OutputExprTyped<OutputType, ExprType> *outputExprTyped
            = dynamic_cast<const OutputExprTyped<OutputType, ExprType> *>(checkExpr);
        if (NULL == outputExprTyped) {
            return false;
        }
        return (*_expr) == outputExprTyped->_expr;
    }

private:
    suez::turing::AttributeExpressionTyped<ExprType> *_expr;
};

template <typename OutputType>
class CaseTuple {
public:
    suez::turing::AttributeExpressionTyped<bool> *whenExpr;
    OutputExpr<OutputType> *thenExpr;
};

template <typename OutputType>
class CaseExpression : public suez::turing::AttributeExpressionTyped<OutputType> {
public:
    CaseExpression(const std::vector<CaseTuple<OutputType>> &caseTuples,
                   OutputExpr<OutputType> *elseExpr)
        : _caseTuples(caseTuples)
        , _elseExpr(elseExpr) {}
    virtual ~CaseExpression() {}

public:
    bool evaluate(matchdoc::MatchDoc matchDoc) override {
        this->evaluateAndReturn(matchDoc);
        return true;
    }
    OutputType evaluateAndReturn(matchdoc::MatchDoc matchDoc) override {
        OutputType value;
        size_t caseCount = _caseTuples.size();
        size_t i = 0;
        for (i = 0; i < caseCount; ++i) {
            CaseTuple<OutputType> &caseTuple = _caseTuples[i];
            if (caseTuple.whenExpr->evaluateAndReturn(matchDoc)) {
                value = caseTuple.thenExpr->evaluateAndReturn(matchDoc);
                break;
            }
        }
        if (i >= caseCount) {
            value = _elseExpr->evaluateAndReturn(matchDoc);
        }
        this->setValue(value);
        this->storeValue(matchDoc, this->_value);
        return this->_value;
    }
    bool batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t count) override {
        for (uint32_t i = 0; i < count; i++) {
            this->evaluate(matchDocs[i]);
        }
        return true;
    }
    void setEvaluated() override {
        this->_isEvaluated = true;
    }
    suez::turing::ExpressionType getExpressionType() const override {
        return suez::turing::ET_UNKNOWN;
    }
    bool operator==(const suez::turing::AttributeExpression *checkExpr) const override;

private:
    std::vector<CaseTuple<OutputType>> _caseTuples;
    OutputExpr<OutputType> *_elseExpr;
};

class CaseExpressionCreator {
public:
    static suez::turing::AttributeExpression *
    createCaseExpression(suez::turing::VariableType resultType,
                         const std::vector<suez::turing::AttributeExpression *> &exprs,
                         autil::mem_pool::Pool *pool);

private:
    template <typename OutputType>
    static OutputExpr<OutputType> *CreateOutputExpr(suez::turing::AttributeExpression *expr,
                                                    autil::mem_pool::Pool *pool);
};

template <typename OutputType>
inline OutputExpr<OutputType> *
CaseExpressionCreator::CreateOutputExpr(suez::turing::AttributeExpression *expr,
                                        autil::mem_pool::Pool *pool) {

#define CREATE_OUTPUT_EXPR(vt_type)                                                                \
    case (vt_type): {                                                                              \
        using ExprType = suez::turing::VariableTypeTraits<vt_type, false>::AttrExprType;           \
        suez::turing::AttributeExpressionTyped<ExprType> *exprTyped                                \
            = dynamic_cast<suez::turing::AttributeExpressionTyped<ExprType> *>(expr);              \
        if (NULL == exprTyped) {                                                                   \
            return NULL;                                                                           \
        }                                                                                          \
        using OutputExprType = OutputExprTyped<OutputType, ExprType>;                              \
        return new (pool->allocate(sizeof(OutputExprType))) OutputExprType(exprTyped);             \
    }

    suez::turing::VariableType type = expr->getType();
    switch (type) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL(CREATE_OUTPUT_EXPR);
    default:
        return NULL;
    }
#undef CREATE_OUTPUT_EXPR
}

template <>
inline OutputExpr<autil::MultiChar> *
CaseExpressionCreator::CreateOutputExpr(suez::turing::AttributeExpression *expr,
                                        autil::mem_pool::Pool *pool) {
    suez::turing::VariableType type = expr->getType();
    switch (type) {
    case vt_string: {
        using ExprType = suez::turing::VariableTypeTraits<vt_string, false>::AttrExprType;
        suez::turing::AttributeExpressionTyped<ExprType> *exprTyped
            = dynamic_cast<suez::turing::AttributeExpressionTyped<ExprType> *>(expr);
        if (NULL == exprTyped) {
            return NULL;
        }
        using OutputExprType = OutputExprTyped<autil::MultiChar, ExprType>;
        return new (pool->allocate(sizeof(OutputExprType))) OutputExprType(exprTyped);
    }
    default:
        return NULL;
    }
}

template <typename OutputType>
inline bool
CaseExpression<OutputType>::operator==(const suez::turing::AttributeExpression *checkExpr) const {
    assert(checkExpr);
    const CaseExpression<OutputType> *expr
        = dynamic_cast<const CaseExpression<OutputType> *>(checkExpr);
    if (expr && this->getType() == expr->getType()) {
        size_t caseCount = _caseTuples.size();
        if (caseCount != expr->_caseTuples.size()) {
            return false;
        }
        for (size_t i = 0; i < caseCount; ++i) {
            const CaseTuple<OutputType> &left = _caseTuples[i];
            const CaseTuple<OutputType> &right = expr->_caseTuples[i];
            if ((*left.whenExpr) == right.whenExpr && (*left.thenExpr) == right.thenExpr) {
                continue;
            } else {
                return false;
            }
        }
        return (*_elseExpr) == expr->_elseExpr;
    }
    return false;
}

} // namespace sql
} // namespace isearch
