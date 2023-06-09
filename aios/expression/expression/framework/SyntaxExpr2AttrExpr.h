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
#ifndef ISEARCH_EXPRESSION_SYNTAXEXPR2ATTREXPR_H
#define ISEARCH_EXPRESSION_SYNTAXEXPR2ATTREXPR_H

#include "expression/common.h"
#include "expression/framework/AttributeExpression.h"
#include "expression/framework/AtomicAttributeExpressionCreatorBase.h"
#include "expression/framework/AttributeExpressionPool.h"
#include "expression/framework/BinaryAttributeExpression.h"
#include "expression/framework/FunctionExpressionCreator.h"
#include "expression/framework/VirtualAttributeContainer.h"
#include "expression/syntax/SyntaxExprVisitor.h"
#include "expression/syntax/BinarySyntaxExpr.h"
#include "expression/syntax/AttributeSyntaxExpr.h"
#include "expression/syntax/FuncSyntaxExpr.h"
#include "expression/syntax/ConstSyntaxExpr.h"

namespace expression {

class SyntaxExpr2AttrExpr : public SyntaxExprVisitor
{
public:
    SyntaxExpr2AttrExpr(AtomicAttributeExpressionCreatorBase *atomicExprCreator,
                        FunctionExpressionCreator *funcExprCreator,
                        AttributeExpressionPool *exprPool,
                        const VirtualAttributeContainer *virtualAttributes,
                        autil::mem_pool::Pool *pool);
    ~SyntaxExpr2AttrExpr();
private:
    SyntaxExpr2AttrExpr(const SyntaxExpr2AttrExpr &);
    SyntaxExpr2AttrExpr& operator=(const SyntaxExpr2AttrExpr &);
public:
    virtual void visitConstSyntaxExpr(const ConstSyntaxExpr *syntaxExpr);
    virtual void visitAttributeSyntaxExpr(const AttributeSyntaxExpr *syntaxExpr);
    virtual void visitAddSyntaxExpr(const AddSyntaxExpr *syntaxExpr);
    virtual void visitMinusSyntaxExpr(const MinusSyntaxExpr *syntaxExpr);
    virtual void visitMulSyntaxExpr(const MulSyntaxExpr *syntaxExpr);
    virtual void visitDivSyntaxExpr(const DivSyntaxExpr *syntaxExpr);
    virtual void visitEqualSyntaxExpr(const EqualSyntaxExpr *syntaxExpr);
    virtual void visitNotEqualSyntaxExpr(const NotEqualSyntaxExpr *syntaxExpr);
    virtual void visitLessSyntaxExpr(const LessSyntaxExpr *syntaxExpr);
    virtual void visitGreaterSyntaxExpr(const GreaterSyntaxExpr *syntaxExpr);
    virtual void visitLessEqualSyntaxExpr(const LessEqualSyntaxExpr *syntaxExpr);
    virtual void visitGreaterEqualSyntaxExpr(const GreaterEqualSyntaxExpr *syntaxExpr);
    virtual void visitAndSyntaxExpr(const AndSyntaxExpr *syntaxExpr);
    virtual void visitOrSyntaxExpr(const OrSyntaxExpr *syntaxExpr);
    virtual void visitFuncSyntaxExpr(const FuncSyntaxExpr *syntaxExpr);

    virtual void visitBitAndSyntaxExpr(const BitAndSyntaxExpr *syntaxExpr);
    virtual void visitBitXorSyntaxExpr(const BitXorSyntaxExpr *syntaxExpr);
    virtual void visitBitOrSyntaxExpr(const BitOrSyntaxExpr *syntaxExpr);
    virtual void visitFuncArgumentExpr(const ConstSyntaxExpr *syntaxExpr);
public:
    AttributeExpression* createAtomicExpr(const std::string &attrName,
            const std::string &tableName = "");
    AttributeExpression* stealAttrExpr();
    ErrorCode getErrorCode() const {
        return _errorCode;
    }
    const std::string& getErrorMsg() const {
        return _errorMsg;
    }
private:
    bool visitChildrenExprs(
        const BinarySyntaxExpr *syntaxExpr,
        AttributeExpression* &leftAttrExpr,
        AttributeExpression* &rightAttrExpr);
    AttributeExpression *createConstAttrExpr(const SyntaxExpr *syntax, ExprValueType evt);
    
    template <template<class T> class BinaryOperator>
    void visitBinaryAttrExpr(const BinarySyntaxExpr *syntaxExpr, bool supportString);
    
    template <template<class T> class BinaryOperator>
    void visitBitOpBinaryAttrExpr(const BinarySyntaxExpr *syntaxExpr);
    
    template <template<class T> class BinaryOperator>
    void createBinaryAttrExpr(const std::string &exprStr,
                              AttributeExpression *leftExpr,
                              AttributeExpression *rightExpr);

    void initializeBinaryExpression(AttributeExpression* expr,
            AttributeExpression* lDepExpr, AttributeExpression* rDepExpr);

    void initializeAttributeExpression(AttributeExpression* expr,
            const std::vector<AttributeExpression*>& dependentExprs =
            std::vector<AttributeExpression*>());

    void addDependentExpression(std::vector<expressionid_t> &exprIdVec,
                                AttributeExpression* attrExpr);

    void mergeAndUniq(std::vector<expressionid_t> &exprIdVec);

    bool checkNoMultiExpression(AttributeExpression *leftExpr,
                                AttributeExpression *rightExpr,
                                const std::string &syntaxStr);
    bool checkNoStringExpression(AttributeExpression *leftExpr,
                                 AttributeExpression *rightExpr,
                                 const std::string &syntaxStr);
    bool checkBitOpResultType(ExprValueType evt,
                              const std::string &syntaxStr);

private:
    bool tryGetFromPool(const std::string &exprString);
private:
    AtomicAttributeExpressionCreatorBase *_atomicExprCreator;
    FunctionExpressionCreator *_funcExprCreator;
    AttributeExpressionPool *_exprPool;
    const VirtualAttributeContainer *_virtualAttributes;
    autil::mem_pool::Pool *_pool;

    AttributeExpression *_attrExpr;

    ErrorCode _errorCode;
    std::string _errorMsg;

private:
    AUTIL_LOG_DECLARE();
};

template <>
inline void SyntaxExpr2AttrExpr::createBinaryAttrExpr<multi_equal_to>(
        const std::string &exprStr,
        AttributeExpression *leftExpr,
        AttributeExpression *rightExpr)
{
    AttributeExpression *expr = NULL;
#define CREATE_BINARY_EXPR_HELPER(vt_type)                              \
    case vt_type:                                                       \
    {                                                                   \
        typedef ExprValueType2AttrValueType<vt_type, false>::AttrValueType LeftType; \
        typedef ExprValueType2AttrValueType<vt_type, true>::AttrValueType RightType; \
        typedef BinaryAttributeExpression<multi_equal_to, LeftType, RightType> BinaryExpr; \
        expr = POOL_NEW_CLASS(_pool, BinaryExpr,                        \
                              exprStr, leftExpr, rightExpr);            \
    }                                                                   \
    break

    ExprValueType leftType = leftExpr->getExprValueType();
    switch (leftType) {
        NUMERIC_EXPR_VALUE_TYPE_MACRO_WITH_STRING_HELPER(CREATE_BINARY_EXPR_HELPER);
    default:
        AUTIL_LOG(WARN, "not supported ExprResultType: %d", (int32_t)leftType);
        assert(false);
        break;
    }
#undef CREATE_BINARY_EXPR_HELPER

    if (expr != NULL) {
        initializeBinaryExpression(expr, leftExpr, rightExpr);
        _exprPool->addPair(exprStr, expr);
        _attrExpr = expr;
    }
}

template <>
inline void SyntaxExpr2AttrExpr::createBinaryAttrExpr<multi_not_equal_to>(
        const std::string &exprStr,
        AttributeExpression *leftExpr,
        AttributeExpression *rightExpr)
{
    AttributeExpression *expr = NULL;
#define CREATE_BINARY_EXPR_HELPER(vt_type)                              \
    case vt_type:                                                       \
    {                                                                   \
        typedef ExprValueType2AttrValueType<vt_type, false>::AttrValueType LeftType; \
        typedef ExprValueType2AttrValueType<vt_type, true>::AttrValueType RightType; \
        typedef BinaryAttributeExpression<multi_not_equal_to, LeftType, RightType> BinaryExpr; \
        expr = POOL_NEW_CLASS(_pool, BinaryExpr,                        \
                              exprStr, leftExpr, rightExpr);            \
    }                                                                   \
    break

    ExprValueType leftType = leftExpr->getExprValueType();
    switch (leftType) {
        NUMERIC_EXPR_VALUE_TYPE_MACRO_WITH_STRING_HELPER(CREATE_BINARY_EXPR_HELPER);
    default:
        AUTIL_LOG(WARN, "not supported ExprResultType: %d", (int32_t)leftType);
        assert(false);
        break;
    }
#undef CREATE_BINARY_EXPR_HELPER

    if (expr != NULL) {
        initializeBinaryExpression(expr, leftExpr, rightExpr);
        _exprPool->addPair(exprStr, expr);
        _attrExpr = expr;
    }
}

template <template<class T> class BinaryOperator>
inline void SyntaxExpr2AttrExpr::createBinaryAttrExpr(
        const std::string &exprStr,
        AttributeExpression *leftExpr,
        AttributeExpression *rightExpr)
{
    AttributeExpression *expr = NULL;
#define CREATE_BINARY_EXPR_HELPER(vt_type)                              \
    case vt_type:                                                       \
    {                                                                   \
        typedef ExprValueType2AttrValueType<vt_type, false>::AttrValueType LeftType; \
        typedef BinaryAttributeExpression<BinaryOperator, LeftType> BinaryExpr; \
        expr = POOL_NEW_CLASS(_pool, BinaryExpr,                        \
                              exprStr, leftExpr, rightExpr);            \
    }                                                                   \
    break

    ExprValueType leftType = leftExpr->getExprValueType();
    switch (leftType) {
        EXPR_VALUE_TYPE_MACRO_HELPER(CREATE_BINARY_EXPR_HELPER);
    default:
        AUTIL_LOG(WARN, "not supported ExprResultType: %d", (int32_t)leftType);
        assert(false);
        break;
    }
#undef CREATE_BINARY_EXPR_HELPER

    if (expr != NULL) {
        initializeBinaryExpression(expr, leftExpr, rightExpr);
        _exprPool->addPair(exprStr, expr);
        _attrExpr = expr;
    }
}

}

#endif //ISEARCH_EXPRESSION_SYNTAXEXPR2ATTREXPR_H
