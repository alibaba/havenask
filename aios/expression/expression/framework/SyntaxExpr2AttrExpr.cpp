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
#include "expression/framework/SyntaxExpr2AttrExpr.h"
#include "expression/framework/TypeTraits.h"
#include "expression/framework/ConstAttributeExpression.h"
#include "expression/function/ArgumentAttributeExpression.h"
#include "expression/util/SyntaxStringConvertor.h"
#include "autil/StringUtil.h"
#include "autil/MultiValueType.h"
#include "autil/MultiValueCreator.h"

using namespace std;
using namespace autil;

namespace expression {
AUTIL_LOG_SETUP(expression, SyntaxExpr2AttrExpr);

SyntaxExpr2AttrExpr::SyntaxExpr2AttrExpr(
        AtomicAttributeExpressionCreatorBase *atomicExprCreator,
        FunctionExpressionCreator *funcExprCreator,
        AttributeExpressionPool *exprPool,
        const VirtualAttributeContainer *virtualAttributes,
        mem_pool::Pool *pool)
    : _atomicExprCreator(atomicExprCreator)
    , _funcExprCreator(funcExprCreator)
    , _exprPool(exprPool)
    , _virtualAttributes(virtualAttributes)
    , _pool(pool)
    , _attrExpr(NULL)
    , _errorCode(ERROR_NONE)
{
}

SyntaxExpr2AttrExpr::~SyntaxExpr2AttrExpr() {
}

void SyntaxExpr2AttrExpr::visitConstSyntaxExpr(const ConstSyntaxExpr *syntaxExpr) {
    assert(false);
    AUTIL_LOG(WARN, "visit const syntax expr is not expected");
}

void SyntaxExpr2AttrExpr::visitFuncArgumentExpr(const ConstSyntaxExpr *syntaxExpr) {
    _attrExpr = POOL_NEW_CLASS(_pool, ArgumentAttributeExpression,
                               syntaxExpr->getValueType(),
                               syntaxExpr->getExprString());
    initializeAttributeExpression(_attrExpr);
    _exprPool->addNeedDeleteExpr(_attrExpr);
}

void SyntaxExpr2AttrExpr::visitAttributeSyntaxExpr(const AttributeSyntaxExpr *syntaxExpr) {
    const string &exprString = syntaxExpr->getExprString();
    if (tryGetFromPool(exprString)) {
        return;
    }
    const string &tableName = syntaxExpr->getTableName();
    const string &attrName = syntaxExpr->getAttributeName();
    if (tableName.empty()) {
        AttributeExpression *expr = _virtualAttributes->findVirtualAttribute(exprString);
        if (expr != NULL) {
            _attrExpr = expr;
            return;
        }
    }
    _attrExpr = _atomicExprCreator->createAtomicExpr(attrName, tableName);
    if (_attrExpr) {
        _exprPool->addPair(exprString, _attrExpr);
    } else {
        _errorCode = _atomicExprCreator->getErrorCode();
        _errorMsg =  _atomicExprCreator->getErrorMsg();
    }
}

void SyntaxExpr2AttrExpr::visitAddSyntaxExpr(const AddSyntaxExpr *syntaxExpr) {
    visitBinaryAttrExpr<std::plus>(syntaxExpr, false);
}

void SyntaxExpr2AttrExpr::visitMinusSyntaxExpr(const MinusSyntaxExpr *syntaxExpr) {
    visitBinaryAttrExpr<std::minus>(syntaxExpr, false);
}

void SyntaxExpr2AttrExpr::visitMulSyntaxExpr(const MulSyntaxExpr *syntaxExpr) {
    visitBinaryAttrExpr<std::multiplies>(syntaxExpr, false);
}

void SyntaxExpr2AttrExpr::visitDivSyntaxExpr(const DivSyntaxExpr *syntaxExpr) {
    visitBinaryAttrExpr<expression::divides>(syntaxExpr, false);
}

void SyntaxExpr2AttrExpr::visitEqualSyntaxExpr(const EqualSyntaxExpr *syntaxExpr) {
    if (tryGetFromPool(syntaxExpr->getExprString())) {
        return;
    }

    AttributeExpression *leftAttrExpr = NULL;
    AttributeExpression *rightAttrExpr = NULL;
    if (!visitChildrenExprs(syntaxExpr, leftAttrExpr, rightAttrExpr)) {
        return;
    }

    if (leftAttrExpr->isMulti() && rightAttrExpr->isMulti()) {
        AUTIL_LOG(WARN, "equal operation does not support two multi arguments.");
        return;
    }

    if (leftAttrExpr->isMulti()) {
        swap(leftAttrExpr, rightAttrExpr);
    }

    if (rightAttrExpr->isMulti()) {
        createBinaryAttrExpr<multi_equal_to>(syntaxExpr->getExprString(), leftAttrExpr, rightAttrExpr);
    } else {
        createBinaryAttrExpr<std::equal_to>(syntaxExpr->getExprString(), leftAttrExpr, rightAttrExpr);
    }
}

void SyntaxExpr2AttrExpr::visitNotEqualSyntaxExpr(const NotEqualSyntaxExpr *syntaxExpr) {
    if (tryGetFromPool(syntaxExpr->getExprString())) {
        return;
    }

    AttributeExpression *leftAttrExpr = NULL;
    AttributeExpression *rightAttrExpr = NULL;
    if (!visitChildrenExprs(syntaxExpr, leftAttrExpr, rightAttrExpr)) {
        return;
    }

    if (leftAttrExpr->isMulti() && rightAttrExpr->isMulti()) {
        AUTIL_LOG(WARN, "equal operation does not support two multi arg.");
        return;
    }

    if (leftAttrExpr->isMulti()) {
        swap(leftAttrExpr, rightAttrExpr);
    }

    if (rightAttrExpr->isMulti()) {
        createBinaryAttrExpr<multi_not_equal_to>(syntaxExpr->getExprString(), leftAttrExpr, rightAttrExpr);
    } else {
        createBinaryAttrExpr<std::not_equal_to>(syntaxExpr->getExprString(), leftAttrExpr, rightAttrExpr);
    }
}

void SyntaxExpr2AttrExpr::visitLessSyntaxExpr(const LessSyntaxExpr *syntaxExpr) {
    visitBinaryAttrExpr<std::less>(syntaxExpr, true);
}

void SyntaxExpr2AttrExpr::visitGreaterSyntaxExpr(const GreaterSyntaxExpr *syntaxExpr) {
    visitBinaryAttrExpr<std::greater>(syntaxExpr, true);
}

void SyntaxExpr2AttrExpr::visitLessEqualSyntaxExpr(const LessEqualSyntaxExpr *syntaxExpr) {
    visitBinaryAttrExpr<std::less_equal>(syntaxExpr, true);
}

void SyntaxExpr2AttrExpr::visitGreaterEqualSyntaxExpr(const GreaterEqualSyntaxExpr *syntaxExpr) {
    visitBinaryAttrExpr<std::greater_equal>(syntaxExpr, true);
}

void SyntaxExpr2AttrExpr::visitAndSyntaxExpr(const AndSyntaxExpr *syntaxExpr) {
    visitBinaryAttrExpr<std::logical_and>(syntaxExpr, false);
}

void SyntaxExpr2AttrExpr::visitOrSyntaxExpr(const OrSyntaxExpr *syntaxExpr) {
    visitBinaryAttrExpr<std::logical_or>(syntaxExpr, false);
}

void SyntaxExpr2AttrExpr::visitFuncSyntaxExpr(const FuncSyntaxExpr *syntaxExpr) {
    if (_funcExprCreator == NULL) {
        AUTIL_LOG(WARN, "can not create function expression");
        return;
    }

    if (tryGetFromPool(syntaxExpr->getExprString())) {
        return;
    }

    const FuncSyntaxExpr::SubExprType &argSyntaxExprs = syntaxExpr->getArgumentSyntaxExprs();
    AttributeExpressionVec subExprs;
    subExprs.reserve(argSyntaxExprs.size());

    for (size_t i = 0; i < argSyntaxExprs.size(); i++) {
        argSyntaxExprs[i]->accept(this);
        AttributeExpression *expr = stealAttrExpr();
        if (expr == NULL) {
            AUTIL_LOG(WARN, "fail to create expression for function argument[%s]",
                      argSyntaxExprs[i]->getExprString().c_str());
            return;
        } else {
            subExprs.push_back(expr);
        }
    }

    AttributeExpressionVec dependentExprs;
    _attrExpr = _funcExprCreator->createFunctionExpression(
            syntaxExpr->getExprString(), syntaxExpr->getFunctionName(),
            subExprs, dependentExprs);
    if (_attrExpr != NULL) {
        initializeAttributeExpression(_attrExpr, dependentExprs);
        _exprPool->addPair(syntaxExpr->getExprString(), _attrExpr);
    } else {
        _errorCode = _funcExprCreator->getErrorCode();
        _errorMsg = _funcExprCreator->getErrorMsg();
        AUTIL_LOG(WARN, "Failed to create function expression[%s]",
                  syntaxExpr->getFunctionName().c_str());
    }
}

void SyntaxExpr2AttrExpr::visitBitAndSyntaxExpr(const BitAndSyntaxExpr *syntaxExpr) {
    visitBitOpBinaryAttrExpr<bit_and>(syntaxExpr);
}

void SyntaxExpr2AttrExpr::visitBitXorSyntaxExpr(const BitXorSyntaxExpr *syntaxExpr) {
    visitBitOpBinaryAttrExpr<bit_xor>(syntaxExpr);
}

void SyntaxExpr2AttrExpr::visitBitOrSyntaxExpr(const BitOrSyntaxExpr *syntaxExpr) {
    visitBitOpBinaryAttrExpr<bit_or>(syntaxExpr);    
}

AttributeExpression* SyntaxExpr2AttrExpr::stealAttrExpr() {
    AttributeExpression *expr = _attrExpr;
    _attrExpr = NULL;
    return expr;
}

AttributeExpression* SyntaxExpr2AttrExpr::createAtomicExpr(const std::string &attrName,
        const string &tableName)
{
    AttributeSyntaxExpr syntax(attrName, tableName);
    visitAttributeSyntaxExpr(&syntax);
    return stealAttrExpr();
}

bool SyntaxExpr2AttrExpr::visitChildrenExprs(
        const BinarySyntaxExpr *syntaxExpr,
        AttributeExpression* &leftAttrExpr,
        AttributeExpression* &rightAttrExpr)
{
    assert(syntaxExpr != NULL);
    const SyntaxExpr *leftSyntax = syntaxExpr->getLeftExpr();
    const SyntaxExpr *rightSyntax = syntaxExpr->getRightExpr();
    if (leftSyntax->getSyntaxType() == SYNTAX_EXPR_TYPE_CONST_VALUE
        && rightSyntax->getSyntaxType() == SYNTAX_EXPR_TYPE_CONST_VALUE)
    {
        _errorCode = ERROR_EXPRESSION_LR_ALL_CONST_VALUE;
        _errorMsg = string("--leftExpr:[") + leftSyntax->getExprString() + "]"
                    "--rightExpr:[" + rightSyntax->getExprString() + "]";
        AUTIL_LOG(WARN, "not support two const expr : %s", _errorMsg.c_str());
        return false;
    }

    AttributeExpression *tmpLeft = NULL;
    AttributeExpression *tmpRight = NULL;
    if (leftSyntax->getSyntaxType() == SYNTAX_EXPR_TYPE_CONST_VALUE) {
        rightSyntax->accept(this);
        tmpRight = this->stealAttrExpr();
        if (tmpRight != NULL) {
            tmpLeft = createConstAttrExpr(leftSyntax, tmpRight->getExprValueType());
        }
    } else if (rightSyntax->getSyntaxType() == SYNTAX_EXPR_TYPE_CONST_VALUE) {
        leftSyntax->accept(this);
        tmpLeft = this->stealAttrExpr();
        if (tmpLeft != NULL) {
            tmpRight = createConstAttrExpr(rightSyntax, tmpLeft->getExprValueType());
        }
    } else {
        leftSyntax->accept(this);
        tmpLeft = this->stealAttrExpr();

        rightSyntax->accept(this);
        tmpRight = this->stealAttrExpr();
    }

    if (tmpLeft == NULL || tmpRight == NULL) {
        return false;
    }

    if (tmpLeft->getExprValueType() != tmpRight->getExprValueType()) {
        ostringstream oss;
        oss << "type between left[" << tmpLeft->getExprValueType() << "] "
            << "and right[" << tmpRight->getExprValueType() << "] are not consistent";
        _errorCode = ERROR_EXPRESSION_LR_TYPE_NOT_MATCH;
        _errorMsg = oss.str();
        AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
        return false;
    }

    leftAttrExpr = tmpLeft;
    rightAttrExpr = tmpRight;

    return true;
}

AttributeExpression *SyntaxExpr2AttrExpr::createConstAttrExpr(
        const SyntaxExpr *syntax, ExprValueType evt)
{
    const ConstSyntaxExpr *constSyntaxExpr = dynamic_cast<const ConstSyntaxExpr *>(syntax);
    if (constSyntaxExpr == NULL || evt == EVT_UNKNOWN) {
        return NULL;
    }
    const string &constValueStr = constSyntaxExpr->getValueString();
    AttributeExpression *expr = NULL;

#define CREATE_CONST_ATTR_EXPR_HELPER(evt_)                             \
    case evt_:                                                          \
    {                                                                   \
        typedef ExprValueType2AttrValueType<evt_, false>::AttrValueType T; \
        T value;                                                        \
        if (!StringUtil::numberFromString<T>(constValueStr, value)) {   \
            AUTIL_LOG(WARN, "numberFromString with %s failed", constValueStr.c_str()); \
            _errorCode = ERROR_CONST_EXPRESSION_VALUE;                  \
            ostringstream oss;                                          \
            oss << "const value[" << constValueStr << "] not match type " << evt_; \
            _errorMsg = oss.str();                                      \
        } else {                                                        \
            expr = POOL_NEW_CLASS(_pool, ConstAttributeExpression<T>, constValueStr, value); \
        }                                                               \
        break;                                                          \
    }

    switch (evt) {
        NUMERIC_EXPR_VALUE_TYPE_MACRO_HELPER(CREATE_CONST_ATTR_EXPR_HELPER);
    case EVT_STRING: {
        string decodeStr = SyntaxStringConvertor::decodeEscapeString(constValueStr);
        char *buf = MultiValueCreator::createMultiValueBuffer<char>(
            (char*)decodeStr.data(), decodeStr.size(), _pool);
        MultiChar mc;
        mc.init(buf);
        typedef ConstAttributeExpression<MultiChar> ConstStringAttributeExpression;
        expr = POOL_NEW_CLASS(_pool, ConstStringAttributeExpression, constValueStr, mc);
        break;
    }
    default:
        AUTIL_LOG(WARN, "unsupported const expr type[%d]", (int)evt);
        break;
    }

    if (expr != NULL) {
        initializeAttributeExpression(expr);
        _exprPool->addNeedDeleteExpr(expr);
    }
    return expr;
}

template <template<class T> class BinaryOperator>
void SyntaxExpr2AttrExpr::visitBinaryAttrExpr(const BinarySyntaxExpr *syntaxExpr, bool supportString) {
    const string &syntaxStr = syntaxExpr->getExprString();
    if (tryGetFromPool(syntaxStr)) {
        return;
    }
    
    AttributeExpression *leftAttrExpr = NULL;
    AttributeExpression *rightAttrExpr = NULL;
    if (!visitChildrenExprs(syntaxExpr, leftAttrExpr, rightAttrExpr)) {
        return;
    }
    if (!checkNoMultiExpression(leftAttrExpr, rightAttrExpr, syntaxStr)
        || (!supportString && !checkNoStringExpression(leftAttrExpr, rightAttrExpr, syntaxStr)))
    {
        return;
    }

    createBinaryAttrExpr<BinaryOperator>(
            syntaxExpr->getExprString(), leftAttrExpr, rightAttrExpr);
}

template <template<class T> class BinaryOperator>
void SyntaxExpr2AttrExpr::visitBitOpBinaryAttrExpr(const BinarySyntaxExpr *syntaxExpr) {
    const string &syntaxStr = syntaxExpr->getExprString();
    if (tryGetFromPool(syntaxStr)) {
        return;
    }

    AttributeExpression *leftAttrExpr = NULL;
    AttributeExpression *rightAttrExpr = NULL;
    if (!visitChildrenExprs(syntaxExpr, leftAttrExpr, rightAttrExpr)) {
        return;
    }
    
    ExprValueType evt = leftAttrExpr->getExprValueType();
    if (!checkBitOpResultType(evt, syntaxStr)
        || !checkNoMultiExpression(leftAttrExpr, rightAttrExpr, syntaxStr)
        || !checkNoStringExpression(leftAttrExpr, rightAttrExpr, syntaxStr))
    {
        return;
    }

    createBinaryAttrExpr<BinaryOperator>(
            syntaxExpr->getExprString(), leftAttrExpr, rightAttrExpr);
}

bool SyntaxExpr2AttrExpr::tryGetFromPool(const string &exprName) {
    _attrExpr = _exprPool->tryGetAttributeExpression(exprName);
    return _attrExpr != NULL;
}

void SyntaxExpr2AttrExpr::initializeBinaryExpression(
        AttributeExpression* expr,
        AttributeExpression* lDepExpr, AttributeExpression* rDepExpr)
{
    if (lDepExpr->isSubExpression() || rDepExpr->isSubExpression()) {
        expr->setIsSubExpression(true);
    }
    vector<AttributeExpression*> dependExprs;
    dependExprs.reserve(2);
    dependExprs.push_back(lDepExpr);
    dependExprs.push_back(rDepExpr);
    initializeAttributeExpression(expr, dependExprs);
}

void SyntaxExpr2AttrExpr::initializeAttributeExpression(
        AttributeExpression* expr,
        const vector<AttributeExpression*> &dependentExprs)
{
    assert(expr);
    vector<expressionid_t> exprIds;
    exprIds.reserve(dependentExprs.size() + 1);
    for (size_t i = 0; i < dependentExprs.size(); i++) {
        addDependentExpression(exprIds, dependentExprs[i]);
    }

    exprIds.push_back((expressionid_t)_exprPool->getAttributeExpressionCount());
    mergeAndUniq(exprIds);
    expr->init(exprIds, _exprPool);
}

void SyntaxExpr2AttrExpr::addDependentExpression(
        vector<expressionid_t> &exprIdVec,
        AttributeExpression* attrExpr) 
{
    if (!attrExpr) { return; }

    const vector<expressionid_t>& depExprIds = 
        attrExpr->getDependentExpressionIds();
    exprIdVec.insert(exprIdVec.end(), depExprIds.begin(), depExprIds.end());
}

void SyntaxExpr2AttrExpr::mergeAndUniq(vector<expressionid_t> &exprIdVec) {
    if (exprIdVec.size() == 1)
    {
        return;
    }

    sort(exprIdVec.begin(), exprIdVec.end());
    vector<expressionid_t>::iterator iter = 
        unique(exprIdVec.begin(), exprIdVec.end());
    exprIdVec.resize(distance(exprIdVec.begin(), iter));
}

bool SyntaxExpr2AttrExpr::checkNoMultiExpression(
        AttributeExpression *leftExpr, AttributeExpression *rightExpr,
        const string &syntaxStr)
{
    if (leftExpr->isMulti() || rightExpr->isMulti()) {
        _errorCode = ERROR_EXPRESSION_WITH_MULTI_VALUE;
        _errorMsg = "multi value is not supported for this expression["
                    + syntaxStr + "]";
        AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
        return false;
    }
    return true;
}

bool SyntaxExpr2AttrExpr::checkNoStringExpression(
        AttributeExpression *leftExpr, AttributeExpression *rightExpr,
        const string &syntaxStr)
{
    if ((leftExpr->getExprValueType() == EVT_STRING
         || rightExpr->getExprValueType() == EVT_STRING))
    {
        AUTIL_LOG(WARN, "string type is not supported for this expression[%s]",
                  syntaxStr.c_str());
        return false;
    }
    return true;
}

bool SyntaxExpr2AttrExpr::checkBitOpResultType(
        ExprValueType evt, const string &syntaxStr)
{
    switch (evt) {
    case EVT_INT8:
    case EVT_INT16:
    case EVT_INT32:
    case EVT_INT64:
    case EVT_UINT8:
    case EVT_UINT16:
    case EVT_UINT32:
    case EVT_UINT64:
        return true;
    case EVT_FLOAT:
        _errorMsg = "[float type] is not support in " + syntaxStr;
        break;
    case EVT_DOUBLE:
        _errorMsg = "[double type] is not support in " + syntaxStr;
        break;
    case EVT_STRING:
        _errorMsg = "[string type] is not support in " + syntaxStr;
        break;
    default:
        _errorMsg = "[unknown type] is not support in " + syntaxStr;
    }

    _errorCode = ERROR_BIT_UNSUPPORT_TYPE_ERROR;
    AUTIL_LOG(WARN, "%s",  _errorMsg.c_str());
    return false;
}

}
