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
#include "suez/turing/expression/framework/SyntaxExpr2AttrExpr.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <functional>
#include <new>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/ArgumentAttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionFactory.h"
#include "suez/turing/expression/framework/AttributeExpressionPool.h"
#include "suez/turing/expression/framework/BinaryAttributeExpression.h"
#include "suez/turing/expression/framework/ConstAttributeExpression.h"
#include "suez/turing/expression/framework/TupleAttributeExpression.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"
#include "suez/turing/expression/function/FunctionInterface.h"
#include "suez/turing/expression/function/FunctionManager.h"
#include "suez/turing/expression/syntax/AtomicSyntaxExpr.h"
#include "suez/turing/expression/syntax/BinarySyntaxExpr.h"
#include "suez/turing/expression/syntax/FuncSyntaxExpr.h"
#include "suez/turing/expression/syntax/MultiSyntaxExpr.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"
#include "suez/turing/expression/util/VirtualAttrConvertor.h"

namespace suez {
namespace turing {
class RankSyntaxExpr;
} // namespace turing
} // namespace suez

using namespace std;
using namespace autil;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, SyntaxExpr2AttrExpr);

SyntaxExpr2AttrExpr::SyntaxExpr2AttrExpr(AttributeExpressionFactory *attrFactory,
                                         FunctionManager *funcManager,
                                         AttributeExpressionPool *exprPool,
                                         VirtualAttrConvertor *convertor,
                                         autil::mem_pool::Pool *pool)
    : _attrFactory(attrFactory)
    , _funcManager(funcManager)
    , _attrExpr(NULL)
    , _exprPool(exprPool)
    , _convertor(convertor)
    , _pool(pool) {}

SyntaxExpr2AttrExpr::~SyntaxExpr2AttrExpr() {}

void SyntaxExpr2AttrExpr::visitAtomicSyntaxExpr(const AtomicSyntaxExpr *syntaxExpr) {
    AtomicSyntaxExprType atomicSyntaxExprType = syntaxExpr->getAtomicSyntaxExprType();
    ExprResultType attrType = syntaxExpr->getExprResultType();
    const std::string &originalStr = syntaxExpr->getExprString();
    const std::string &prefixStr = syntaxExpr->getPrefixString();
    const std::string &valueStr = syntaxExpr->getValueString();
    if (syntaxExpr->getSyntaxExprType() == SYNTAX_EXPR_TYPE_FUNC_ARGUMENT) {
        switch (atomicSyntaxExprType) {
        case INT8_VT:
        case UINT8_VT:
        case INT16_VT:
        case UINT16_VT:
        case INT32_VT:
        case UINT32_VT:
        case INT64_VT:
        case UINT64_VT:
        case FLOAT_VT:
        case DOUBLE_VT:
        case STRING_VT:
            _attrExpr = createArgAttrExpr(syntaxExpr, attrType);
            break;
        default:
            break;
        }
        assert(_attrExpr);
        AUTIL_LOG(TRACE3, "ArgumentAttributeExpression, atomicSyntaxExprType: %d", atomicSyntaxExprType);
        return;
    }
    switch (atomicSyntaxExprType) {
    case ATTRIBUTE_NAME:
        _attrExpr = createAtomicExpr(valueStr, prefixStr, originalStr);
        break;
    case INT8_VT:
    case UINT8_VT:
    case INT16_VT:
    case UINT16_VT:
    case INT32_VT:
    case UINT32_VT:
    case INT64_VT:
    case UINT64_VT:
    case FLOAT_VT:
    case DOUBLE_VT:
    case STRING_VT:
        _attrExpr = createConstAttrExpr(syntaxExpr, attrType);
        break;
    default:
        assert(false);
        break;
    }
    if (_attrExpr) {
        // attribute type is no the same as the type in schema.
        if (attrType != _attrExpr->getType()) {
            AUTIL_LOG(ERROR,
                      "attribute expression [%s] type does not match,"
                      " type from qrs is [%d], type from index is [%d]",
                      originalStr.c_str(),
                      attrType,
                      _attrExpr->getType());
            _attrExpr = NULL;
            return;
        }
    }
}

void SyntaxExpr2AttrExpr::visitAddSyntaxExpr(const AddSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");
    visitBinarySyntaxExpr<std::plus>(syntaxExpr);
}

void SyntaxExpr2AttrExpr::visitMinusSyntaxExpr(const MinusSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");
    visitBinarySyntaxExpr<std::minus>(syntaxExpr);
}

void SyntaxExpr2AttrExpr::visitMulSyntaxExpr(const MulSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");
    visitBinarySyntaxExpr<std::multiplies>(syntaxExpr);
}

void SyntaxExpr2AttrExpr::visitDivSyntaxExpr(const DivSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");
    visitBinarySyntaxExpr<divides>(syntaxExpr);
}

void SyntaxExpr2AttrExpr::visitPowerSyntaxExpr(const PowerSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");
    visitBinarySyntaxExpr<power>(syntaxExpr);
}

void SyntaxExpr2AttrExpr::visitLogSyntaxExpr(const LogSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");
    visitBinarySyntaxExpr<log>(syntaxExpr);
}

void SyntaxExpr2AttrExpr::visitEqualSyntaxExpr(const EqualSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");

    const SyntaxExpr *leftExpr = syntaxExpr->getLeftExpr();
    const SyntaxExpr *rightExpr = syntaxExpr->getRightExpr();
    bool leftIsMulti = leftExpr->isMultiValue();
    bool rightIsMulti = rightExpr->isMultiValue();
    assert(!(leftIsMulti && rightIsMulti));
    assert(leftExpr->getExprResultType() == rightExpr->getExprResultType());

    if (leftIsMulti || rightIsMulti) {
        AUTIL_LOG(TRACE3,
                  "there is a multivalue attr, left is[%s], right is [%s]",
                  leftExpr->getExprString().c_str(),
                  rightExpr->getExprString().c_str());
        visitMultiEqualBinarySyntaxExpr<multi_equal_to>(syntaxExpr);
    } else {
        visitBinarySyntaxExpr<std::equal_to>(syntaxExpr);
    }
}

void SyntaxExpr2AttrExpr::visitNotEqualSyntaxExpr(const NotEqualSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");

    const SyntaxExpr *leftExpr = syntaxExpr->getLeftExpr();
    const SyntaxExpr *rightExpr = syntaxExpr->getRightExpr();
    bool leftIsMulti = leftExpr->isMultiValue();
    bool rightIsMulti = rightExpr->isMultiValue();
    assert(!(leftIsMulti && rightIsMulti));
    assert(leftExpr->getExprResultType() == rightExpr->getExprResultType());

    if (leftIsMulti || rightIsMulti) {
        AUTIL_LOG(TRACE3,
                  "there is a multivalue attr, left is[%s], right is [%s]",
                  leftExpr->getExprString().c_str(),
                  rightExpr->getExprString().c_str());
        visitMultiEqualBinarySyntaxExpr<multi_not_equal_to>(syntaxExpr);
    } else {
        visitBinarySyntaxExpr<std::not_equal_to>(syntaxExpr);
    }
}

void SyntaxExpr2AttrExpr::visitLessSyntaxExpr(const LessSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");
    visitBinarySyntaxExpr<std::less>(syntaxExpr);
}

void SyntaxExpr2AttrExpr::visitGreaterSyntaxExpr(const GreaterSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");
    visitBinarySyntaxExpr<std::greater>(syntaxExpr);
}

void SyntaxExpr2AttrExpr::visitLessEqualSyntaxExpr(const LessEqualSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");
    visitBinarySyntaxExpr<std::less_equal>(syntaxExpr);
}

void SyntaxExpr2AttrExpr::visitGreaterEqualSyntaxExpr(const GreaterEqualSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");
    visitBinarySyntaxExpr<std::greater_equal>(syntaxExpr);
}

void SyntaxExpr2AttrExpr::visitAndSyntaxExpr(const AndSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");
    visitBinarySyntaxExpr<std::logical_and>(syntaxExpr);
}

void SyntaxExpr2AttrExpr::visitOrSyntaxExpr(const OrSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");
    visitBinarySyntaxExpr<std::logical_or>(syntaxExpr);
}

void SyntaxExpr2AttrExpr::visitBitAndSyntaxExpr(const BitAndSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");
    visitBinarySyntaxExpr<bit_and>(syntaxExpr);
}

void SyntaxExpr2AttrExpr::visitBitXorSyntaxExpr(const BitXorSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");
    visitBinarySyntaxExpr<bit_xor>(syntaxExpr);
}

void SyntaxExpr2AttrExpr::visitBitOrSyntaxExpr(const BitOrSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");
    visitBinarySyntaxExpr<bit_or>(syntaxExpr);
}

void SyntaxExpr2AttrExpr::visitRankSyntaxExpr(const RankSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");
    assert(false);
}

void SyntaxExpr2AttrExpr::visitFuncSyntaxExpr(const FuncSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");

    if (tryGetFromPool(syntaxExpr))
        return;

    FunctionSubExprVec subExprVec;

    const FuncSyntaxExpr::SubExprType &exprs = syntaxExpr->getParam();
    for (FuncSyntaxExpr::SubExprType::const_iterator i = exprs.begin(); i != exprs.end(); ++i) {
        (*i)->accept(this);
        AttributeExpression *expr = stealAttrExpr();
        if (!expr) {
            AUTIL_LOG(WARN, "Expression is NULL");
            return;
        }
        subExprVec.push_back(expr);
    }

    const std::string &name = syntaxExpr->getFuncName();
    assert(_funcManager);
    _attrExpr = _funcManager->createFuncExpression(
        name, subExprVec, syntaxExpr->getExprResultType(), syntaxExpr->isMultiValue(), syntaxExpr->isSubExpression());
    const std::string &exprString = syntaxExpr->getExprString();
    if (_attrExpr) {
        _attrExpr->setOriginalString(exprString);
        _attrExpr->setIsSubExpression(syntaxExpr->isSubExpression());
        _exprPool->addPair(exprString, _attrExpr);
    } else {
        AUTIL_LOG(WARN, "Failed to create function[%s] expression[%s]", name.c_str(), exprString.c_str());
    }
}

void SyntaxExpr2AttrExpr::visitMultiSyntaxExpr(const MultiSyntaxExpr *syntaxExpr) {
    if (tryGetFromPool(syntaxExpr)) {
        return;
    }
    TupleAttributeExpression *attrExpr = POOL_NEW_CLASS(_pool, TupleAttributeExpression);
    for (auto expr : syntaxExpr->getSyntaxExprs()) {
        expr->accept(this);
        AttributeExpression *tmpAttrExpr = this->stealAttrExpr();
        if (!tmpAttrExpr) {
            POOL_DELETE_CLASS(_attrExpr);
            AUTIL_LOG(WARN, "Expression is NULL");
            return;
        }
        if (tmpAttrExpr->isMultiValue()) {
            POOL_DELETE_CLASS(_attrExpr);
            AUTIL_LOG(WARN, "TupleAttributeExpression not support multi value");
            return;
        }
        if (tmpAttrExpr->getType() == vt_string) {
            auto attributeTyped = dynamic_cast<AttributeExpressionTyped<autil::MultiChar> *>(tmpAttrExpr);
            if (!attributeTyped) {
                POOL_DELETE_CLASS(_attrExpr);
                AUTIL_LOG(WARN, "TupleAttributeExpression not support std string");
                return;
            }
        }
        attrExpr->addAttributeExpression(tmpAttrExpr);
    }
    _attrExpr = attrExpr;
    const std::string &exprString = syntaxExpr->getExprString();
    _attrExpr->setOriginalString(exprString);
    _attrExpr->setIsSubExpression(syntaxExpr->isSubExpression());
    _exprPool->addPair(exprString, _attrExpr);
    return;
}

template <template <class T> class BinaryOperator>
void SyntaxExpr2AttrExpr::visitBinarySyntaxExpr(const BinarySyntaxExpr *syntaxExpr) {
    AttributeExpression *leftAttrExpr = NULL;
    AttributeExpression *rightAttrExpr = NULL;
    if (!visitChildrenExprs(syntaxExpr, leftAttrExpr, rightAttrExpr)) {
        return;
    }
#define CONVERT_BINARY_SYNTAX_EXPR_HELPER(vt_type)                                                                     \
    case vt_type: {                                                                                                    \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T;                                                    \
        typedef BinaryAttributeExpression<BinaryOperator, T> BinaryExpr;                                               \
        _attrExpr = POOL_NEW_CLASS(_pool, BinaryExpr, leftAttrExpr, rightAttrExpr);                                    \
    } break

    ExprResultType leftType = syntaxExpr->getLeftExpr()->getExprResultType();
    switch (leftType) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL_AND_STRING(CONVERT_BINARY_SYNTAX_EXPR_HELPER);
    default:
        AUTIL_LOG(WARN, "not supported ExprResultType: %d", (int32_t)leftType);
        assert(false);
        break;
    }

#undef CONVERT_BINARY_SYNTAX_EXPR_HELPER
    if (_attrExpr) {
        const std::string &exprString = syntaxExpr->getExprString();
        _attrExpr->setOriginalString(exprString);
        _attrExpr->setIsSubExpression(syntaxExpr->isSubExpression());
        _exprPool->addPair(exprString, _attrExpr);
    }
}

template <template <class T> class BinaryOperator>
void SyntaxExpr2AttrExpr::visitMultiEqualBinarySyntaxExpr(const BinarySyntaxExpr *syntaxExpr) {
    AttributeExpression *leftAttrExpr = NULL;
    AttributeExpression *rightAttrExpr = NULL;
    if (!visitChildrenExprs(syntaxExpr, leftAttrExpr, rightAttrExpr)) {
        return;
    }
    assert(leftAttrExpr != NULL && rightAttrExpr != NULL);
    bool leftIsMulti = leftAttrExpr->isMultiValue();
    if (leftIsMulti) {
        // the left attrExpr must be not multi_value_type
        std::swap(leftAttrExpr, rightAttrExpr);
    }

#define CONVERT_MULTI_VALUE_BINARY_SYNTAX_EXPR_HELPER(vt_type)                                                         \
    case vt_type: {                                                                                                    \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T;                                                    \
        typedef BinaryAttributeExpression<BinaryOperator, T, MultiValueType<T>> BinaryExpr;                            \
        _attrExpr = POOL_NEW_CLASS(_pool, BinaryExpr, leftAttrExpr, rightAttrExpr);                                    \
    } break

    ExprResultType leftType = syntaxExpr->getLeftExpr()->getExprResultType();
    switch (leftType) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_STRING(CONVERT_MULTI_VALUE_BINARY_SYNTAX_EXPR_HELPER);
    case vt_bool:
        assert(false);
        break;
    default:
        AUTIL_LOG(WARN, "not supported ExprResultType: %d", (int32_t)leftType);
        assert(false);
        break;
    }

#undef CONVERT_MULTI_VALUE_BINARY_SYNTAX_EXPR_HELPER

    if (_attrExpr) {
        _attrExpr->setOriginalString(syntaxExpr->getExprString());
        _attrExpr->setIsSubExpression(syntaxExpr->isSubExpression());
        _exprPool->addPair(syntaxExpr->getExprString(), _attrExpr);
    }
}

AttributeExpression *SyntaxExpr2AttrExpr::createConstAttrExpr(const AtomicSyntaxExpr *atomicSyntaxExpr,
                                                              VariableType type) const {
    const string &constValue = atomicSyntaxExpr->getValueString();
    AttributeExpression *ret = NULL;

#define CREATE_CONST_ATTR_EXPR_HELPER(vt)                                                                              \
    case vt: {                                                                                                         \
        typedef VariableTypeTraits<vt, false>::AttrExprType T;                                                         \
        T exprValue;                                                                                                   \
        if (StringUtil::numberFromString<T>(constValue, exprValue)) {                                                  \
            ret = POOL_NEW_CLASS(_pool, ConstAttributeExpression<T>, exprValue);                                       \
        } else {                                                                                                       \
            AUTIL_LOG(WARN, "createConstAttrExpr for number failed, value: %s", constValue.c_str());                   \
        }                                                                                                              \
        break;                                                                                                         \
    }

    switch (type) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL(CREATE_CONST_ATTR_EXPR_HELPER);
    case vt_string:
        ret = createConstStringAttrExpr(constValue);
        break;
    default:
        AUTIL_LOG(WARN, "Does not support this type:%d", (int32_t)type);
        assert(false);
    }
#undef CREATE_CONST_ATTR_EXPR_HELPER
    if (ret) {
        _exprPool->addNeedDeleteExpr(ret);
        ret->setOriginalString(atomicSyntaxExpr->getExprString());
    }

    return ret;
}

AttributeExpression *SyntaxExpr2AttrExpr::createArgAttrExpr(const AtomicSyntaxExpr *atomicSyntaxExpr,
                                                            VariableType type) const {
    const string &argValue = atomicSyntaxExpr->getValueString();
    AttributeExpression *ret = NULL;

#define CREATE_ARG_ATTR_EXPR_HELPER(vt)                                                                                \
    case vt: {                                                                                                         \
        typedef VariableTypeTraits<vt, false>::AttrExprType T;                                                         \
        T exprValue;                                                                                                   \
        if (StringUtil::numberFromString<T>(argValue, exprValue)) {                                                    \
            ret = POOL_NEW_CLASS(_pool, ArgumentAttributeExpressionTyped<T>, exprValue);                               \
        } else {                                                                                                       \
            AUTIL_LOG(WARN, "createArgAttrExpr for number failed, value: %s", argValue.c_str());                       \
        }                                                                                                              \
        break;                                                                                                         \
    }

    switch (type) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL(CREATE_ARG_ATTR_EXPR_HELPER);
    case vt_string:
        ret = createArgStringAttrExpr(argValue);
        break;
    default:
        AUTIL_LOG(WARN, "Does not support this type:%d", (int32_t)type);
        assert(false);
    }
#undef CREATE_ARG_ATTR_EXPR_HELPER
    if (ret) {
        _exprPool->addNeedDeleteExpr(ret);
        ret->setOriginalString(atomicSyntaxExpr->getExprString());
    }

    return ret;
}

AttributeExpression *SyntaxExpr2AttrExpr::createConstStringAttrExpr(const string &constValue) const {
    AttributeExpression *ret = NULL;
    typedef ConstAttributeExpression<MultiChar> ConstStringAttributeExpression;
    MultiChar mc;
    mc.init(MultiValueCreator::createMultiValueBuffer(constValue.data(), constValue.size(), _pool));
    ret = POOL_NEW_CLASS(_pool, ConstStringAttributeExpression, mc);
    return ret;
}

AttributeExpression *SyntaxExpr2AttrExpr::createArgStringAttrExpr(const string &argValue) const {
    AttributeExpression *ret = NULL;
    typedef ArgumentAttributeExpressionTyped<MultiChar> ArgStringAttributeExpression;
    MultiChar mc;
    mc.init(MultiValueCreator::createMultiValueBuffer(argValue.data(), argValue.size(), _pool));
    ret = POOL_NEW_CLASS(_pool, ArgStringAttributeExpression, mc);
    return ret;
}

bool SyntaxExpr2AttrExpr::visitChildrenExprs(const BinarySyntaxExpr *syntaxExpr,
                                             AttributeExpression *&leftAttrExpr,
                                             AttributeExpression *&rightAttrExpr) {
    if (tryGetFromPool(syntaxExpr)) {
        return false;
    }
    const SyntaxExpr *leftExpr = syntaxExpr->getLeftExpr();
    const SyntaxExpr *rightExpr = syntaxExpr->getRightExpr();

    // construct children 'AttributeExpression'
    leftExpr->accept(this);
    AttributeExpression *tmpLeftAttrExpr = this->stealAttrExpr();
    if (!tmpLeftAttrExpr) {
        AUTIL_LOG(WARN, "Expression is NULL");
        return false;
    }
    rightExpr->accept(this);
    AttributeExpression *tmpRightAttrExpr = this->stealAttrExpr();
    if (!tmpRightAttrExpr) {
        AUTIL_LOG(WARN, "Expression is NULL");
        return false;
    }
    leftAttrExpr = tmpLeftAttrExpr;
    rightAttrExpr = tmpRightAttrExpr;
    return true;
}

AttributeExpression *SyntaxExpr2AttrExpr::stealAttrExpr() {
    AttributeExpression *expr = _attrExpr;
    _attrExpr = NULL;
    return expr;
}

bool SyntaxExpr2AttrExpr::tryGetFromPool(const SyntaxExpr *syntaxExpr) {
    _attrExpr = _exprPool->tryGetAttributeExpression(syntaxExpr->getExprString());
    return _attrExpr != NULL;
}

AttributeExpression *SyntaxExpr2AttrExpr::createAtomicExpr(const string &attrName) {
    return createAtomicExpr(attrName, "", attrName);
}

AttributeExpression *SyntaxExpr2AttrExpr::createAtomicExpr(const string &attrName,
                                                           const string &prefixName,
                                                           const string &completeAttrName) {
    AttributeExpression *attrExpr = _exprPool->tryGetAttributeExpression(completeAttrName);
    if (attrExpr) {
        return attrExpr;
    }
    const SyntaxExpr *newSyntax = _convertor->nameToExpr(attrName);
    AttributeExpression *lastExpr = _attrExpr;
    if (newSyntax) {
        newSyntax->accept(this);
    } else {
        _attrExpr = _attrFactory->createAtomicExpr(attrName, prefixName);
        if (_attrExpr) {
            _attrExpr->setOriginalString(attrName);
            _exprPool->addPair(completeAttrName, _attrExpr);
        }
    }
    swap(_attrExpr, lastExpr);
    return lastExpr;
}

} // namespace turing
} // namespace suez
