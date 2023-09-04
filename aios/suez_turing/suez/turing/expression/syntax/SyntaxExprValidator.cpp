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
#include "suez/turing/expression/syntax/SyntaxExprValidator.h"

#include <assert.h>
#include <map>
#include <memory>
#include <ostream>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/expression/cava/common/CavaFieldTypeHelper.h"
#include "suez/turing/expression/cava/common/CavaFunctionModuleInfo.h"
#include "suez/turing/expression/cava/common/CavaModuleInfo.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"
#include "suez/turing/expression/function/FunctionMap.h"
#include "suez/turing/expression/syntax/AtomicSyntaxExpr.h"
#include "suez/turing/expression/syntax/BinarySyntaxExpr.h"
#include "suez/turing/expression/syntax/ConditionalSyntaxExpr.h"
#include "suez/turing/expression/syntax/FuncSyntaxExpr.h"
#include "suez/turing/expression/syntax/MultiSyntaxExpr.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"
#include "suez/turing/expression/util/AttributeInfos.h"
#include "suez/turing/expression/util/VirtualAttribute.h"

namespace suez {
namespace turing {
class RankSyntaxExpr;
} // namespace turing
} // namespace suez

using namespace std;
using namespace autil;
namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, SyntaxExprValidator);

SyntaxExprValidator::SyntaxExprValidator(const AttributeInfos *attrInfos,
                                         const VirtualAttributes &virtualAttributes,
                                         bool isSubDocQuery)
    : _attrInfos(attrInfos), _virtualAttributes(virtualAttributes), _isSubDocQuery(isSubDocQuery) {
    reset();
}

SyntaxExprValidator::~SyntaxExprValidator() {}

void SyntaxExprValidator::validateAttributeAndGetInfo(const string &attrName,
                                                      SyntaxExprType &syntaxExprType,
                                                      ExprResultType &resultType,
                                                      bool &isMultiValue,
                                                      bool &isSubExpression) {
    if ((NULL != _attrInfos)) {
        const AttributeInfo *attrInfo = _attrInfos->getAttributeInfo(attrName.c_str());
        if (attrInfo) {
            syntaxExprType = SYNTAX_EXPR_TYPE_ATOMIC_ATTR;
            matchdoc::ValueType vt = attrInfo->getValueType();
            resultType = vt.getBuiltinType();
            isMultiValue = vt.isMultiValue();
            isSubExpression = attrInfo->getSubDocAttributeFlag();
            if (!_isSubDocQuery && isSubExpression) {
                _errorCode = ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT;
                _errorMsg = "attribute [" + attrName + "] using sub expression when sub doc display is not set";
            }
            AUTIL_LOG(
                TRACE3, "validate Attribute name: [%s], isMultivalue = %d", attrName.c_str(), (int32_t)isMultiValue);
            return;
        }
    }

    for (vector<VirtualAttribute *>::const_iterator it = _virtualAttributes.begin(); it != _virtualAttributes.end();
         ++it) {
        if (attrName == (*it)->getVirtualAttributeName()) {
            syntaxExprType = SYNTAX_EXPR_TYPE_VIRTUAL_ATTR;
            SyntaxExpr *syntaxExpr = (*it)->getVirtualAttributeSyntaxExpr();
            assert(syntaxExpr);
            resultType = syntaxExpr->getExprResultType();
            isMultiValue = syntaxExpr->isMultiValue();
            isSubExpression = syntaxExpr->isSubExpression();
            return;
        }
    }
    _errorCode = ERROR_ATTRIBUTE_NOT_EXIST;
    _errorMsg = "attribute [" + attrName + "] not exist";
}

const SyntaxExpr *SyntaxExprValidator::findActualExpr(const SyntaxExpr *syntaxExpr) {
    assert(syntaxExpr);
    if (SYNTAX_EXPR_TYPE_VIRTUAL_ATTR != syntaxExpr->getSyntaxExprType()) {
        return syntaxExpr;
    }

    for (vector<VirtualAttribute *>::const_iterator it = _virtualAttributes.begin(); it != _virtualAttributes.end();
         ++it) {
        if (syntaxExpr->getExprString() == (*it)->getVirtualAttributeName()) {
            return (*it)->getVirtualAttributeSyntaxExpr();
        }
    }

    return NULL;
}

ExprResultType SyntaxExprValidator::validateBinarySyntaxExpr(const BinarySyntaxExpr *syntaxExpr) {
    const SyntaxExpr *leftExpr = syntaxExpr->getLeftExpr();
    const SyntaxExpr *rightExpr = syntaxExpr->getRightExpr();
    assert(leftExpr);
    assert(rightExpr);

    leftExpr->accept(this);
    if (getErrorCode() != ERROR_NONE) {
        AUTIL_LOG(WARN, "validate leftExpr error:[%s]", _errorMsg.c_str());
        return vt_unknown;
    }

    rightExpr->accept(this);
    if (getErrorCode() != ERROR_NONE) {
        AUTIL_LOG(WARN, "validate rightExpr error:[%s]", _errorMsg.c_str());
        return vt_unknown;
    }

    // for virtual attribute
    const SyntaxExpr *actualLeftExpr = findActualExpr(leftExpr);
    const SyntaxExpr *actualRightExpr = findActualExpr(rightExpr);
    if (NULL == actualLeftExpr || NULL == actualRightExpr) {
        _errorCode = ERROR_STRING_IN_BINARY_EXPRESSION;
        _errorMsg = "actualLeftExpr or actualRightExpr is NULL";
        AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
        return vt_unknown;
    }

    if (!checkStringValueInBinaryExpr(syntaxExpr, actualLeftExpr, actualRightExpr)) {
        _errorCode = ERROR_STRING_IN_BINARY_EXPRESSION;
        _errorMsg = "string in binary expression is not supported.";
        AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
        return vt_unknown;
    }

    syntaxExpr->setMultiValue(actualLeftExpr->isMultiValue() || actualRightExpr->isMultiValue());
    syntaxExpr->setIsSubExpression(actualLeftExpr->isSubExpression() || actualRightExpr->isSubExpression());
    if (actualLeftExpr->isMultiValue() && actualRightExpr->isMultiValue()) {
        _errorCode = ERROR_EXPRESSION_WITH_MULTI_VALUE;
        _errorMsg = "binary expression with multivalue is not supported.";
        AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
        return vt_unknown;
    }

    return checkExprType(actualLeftExpr, actualRightExpr);
}

bool SyntaxExprValidator::checkStringValueInBinaryExpr(const SyntaxExpr *syntaxExpr,
                                                       const SyntaxExpr *leftExpr,
                                                       const SyntaxExpr *rightExpr) {
    ExprResultType leftResultType = leftExpr->getExprResultType();
    ExprResultType rightResultType = rightExpr->getExprResultType();
    SyntaxExprType syntaxExprType = syntaxExpr->getSyntaxExprType();

    if ((leftResultType == vt_string || rightResultType == vt_string) && !supportStringAttribute(syntaxExprType)) {
        return false;
    }
    return true;
}

// only relational expression supports string attribute
bool SyntaxExprValidator::supportStringAttribute(SyntaxExprType syntaxExprType) const {
    return syntaxExprType == SYNTAX_EXPR_TYPE_EQUAL            // =
           || syntaxExprType == SYNTAX_EXPR_TYPE_NOTEQUAL      // !=
           || syntaxExprType == SYNTAX_EXPR_TYPE_LESS          // <
           || syntaxExprType == SYNTAX_EXPR_TYPE_GREATERTHAN   // >
           || syntaxExprType == SYNTAX_EXPR_TYPE_LESSEQUAL     // <=
           || syntaxExprType == SYNTAX_EXPR_TYPE_GREATEREQUAL; // >=
}

void SyntaxExprValidator::checkConstExprType(const SyntaxExpr *constExpr) {
    const AtomicSyntaxExpr *exprA = dynamic_cast<const AtomicSyntaxExpr *>(constExpr);
    assert(exprA);
    AtomicSyntaxExprType atomicSyntaxExprType = exprA->getAtomicSyntaxExprType();
    switch (atomicSyntaxExprType) {
    case STRING_VT: {
        exprA->setExprResultType(vt_string);
        return; // not need check type
    } break;
    case INT64_VT: {
        exprA->setExprResultType(vt_int64);
        if (!checkConstExprValue(exprA)) {
            exprA->setExprResultType(vt_uint64);
            if (!checkConstExprValue(exprA)) {
                exprA->setExprResultType(vt_double);
                if (!checkConstExprValue(exprA)) {
                    stringstream ss;
                    ss << "check const value invalid, value: " << exprA->getValueString()
                       << " is not type int64_t or uint64_t";
                    _errorCode = ERROR_CONST_EXPRESSION_VALUE;
                    _errorMsg = ss.str();
                    AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
                    return;
                }
            }
        }
        return; // not need check type
    } break;
    case UINT64_VT: {
        exprA->setExprResultType(vt_uint64);
    } break;
    case INT32_VT: {
        exprA->setExprResultType(vt_int32);
    } break;
    case UINT32_VT: {
        exprA->setExprResultType(vt_uint32);
    } break;
    case INT16_VT: {
        exprA->setExprResultType(vt_int16);
    } break;
    case UINT16_VT: {
        exprA->setExprResultType(vt_uint16);
    } break;
    case INT8_VT: {
        exprA->setExprResultType(vt_int8);
    } break;
    case UINT8_VT: {
        exprA->setExprResultType(vt_uint8);
    } break;
    case DOUBLE_VT: {
        exprA->setExprResultType(vt_double);
    } break;
    case FLOAT_VT: {
        exprA->setExprResultType(vt_float);
    } break;
    default: {
        _errorCode = ERROR_CONST_EXPRESSION_TYPE;
        stringstream ss;
        ss << "const value[type:" << atomicSyntaxExprType << "]";
        _errorMsg = ss.str();
        AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
        return;
    } break;
    }
    if (!checkConstExprValue(exprA)) {
        stringstream ss;
        ss << "check const value invalid, value: " << exprA->getValueString() << " is not "
           << matchdoc::builtinTypeToString(exprA->getExprResultType());
        _errorCode = ERROR_CONST_EXPRESSION_VALUE;
        _errorMsg = ss.str();
        AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
    }
    return;
}

#define CHECK_CONST_EXPR_TYPE(expr_type, value_type)                                                                   \
    case expr_type: {                                                                                                  \
        if (resultTypeB == value_type) {                                                                               \
            exprA->setExprResultType(resultTypeB);                                                                     \
            exprResultType = resultTypeB;                                                                              \
        }                                                                                                              \
    } break;

ExprResultType SyntaxExprValidator::checkConstExprType(const SyntaxExpr *constExpr, const SyntaxExpr *nonconstExpr) {
    ExprResultType resultTypeB = nonconstExpr->getExprResultType();

    const AtomicSyntaxExpr *exprA = dynamic_cast<const AtomicSyntaxExpr *>(constExpr);
    assert(exprA);

    AtomicSyntaxExprType atomicSyntaxExprType = exprA->getAtomicSyntaxExprType();

    ExprResultType exprResultType = vt_unknown;

    switch (atomicSyntaxExprType) {
    case STRING_VT: {
        if (resultTypeB == vt_string) {
            exprA->setExprResultType(vt_string);
            exprResultType = vt_string;
        }
    } break;
    case INT64_VT: { // 兼容逻辑
        if (resultTypeB == vt_int8 || resultTypeB == vt_uint8 || resultTypeB == vt_int16 || resultTypeB == vt_uint16 ||
            resultTypeB == vt_int32 || resultTypeB == vt_uint32 || resultTypeB == vt_int64 ||
            resultTypeB == vt_uint64 || resultTypeB == vt_float || resultTypeB == vt_double) {
            exprA->setExprResultType(resultTypeB);
            exprResultType = resultTypeB;
        }
    } break;
    case DOUBLE_VT: { // 兼容逻辑
        if (resultTypeB == vt_float || resultTypeB == vt_double) {
            exprA->setExprResultType(resultTypeB);
            exprResultType = resultTypeB;
        }
    } break;
        CHECK_CONST_EXPR_TYPE(INT8_VT, vt_int8)
        CHECK_CONST_EXPR_TYPE(UINT8_VT, vt_uint8)
        CHECK_CONST_EXPR_TYPE(INT16_VT, vt_int16)
        CHECK_CONST_EXPR_TYPE(UINT16_VT, vt_uint16)
        CHECK_CONST_EXPR_TYPE(INT32_VT, vt_int32)
        CHECK_CONST_EXPR_TYPE(UINT32_VT, vt_uint32)
        CHECK_CONST_EXPR_TYPE(UINT64_VT, vt_uint64)
    default: {
        AUTIL_LOG(WARN, "atomicSyntaxExprType is %d", atomicSyntaxExprType);
    } break;
    }

    if (exprResultType == vt_unknown) {
        stringstream ss;
        ss << "const value[type:" << atomicSyntaxExprType << "] not match[type:" << resultTypeB << "]";

        _errorCode = ERROR_CONST_EXPRESSION_TYPE;
        _errorMsg = ss.str();
    } else {
        if ((atomicSyntaxExprType == INT64_VT || atomicSyntaxExprType == DOUBLE_VT) && !checkConstExprValue(exprA)) {
            stringstream ss;
            ss << "const value invalid, type: " << vt2TypeName(exprResultType)
               << ", value: " << exprA->getValueString();
            _errorCode = ERROR_CONST_EXPRESSION_VALUE;
            _errorMsg = ss.str();
            exprResultType = vt_unknown;
        }
    }
    return exprResultType;
}

bool SyntaxExprValidator::checkConstExprValue(const AtomicSyntaxExpr *expr) {
    assert(expr->getAtomicSyntaxExprType() != STRING_VT && expr->getAtomicSyntaxExprType() != UNKNOWN &&
           expr->getAtomicSyntaxExprType() != ATTRIBUTE_NAME);

#define VALIDATE_VALUE(vt)                                                                                             \
    case vt: {                                                                                                         \
        typedef VariableTypeTraits<vt, false>::AttrExprType NumberType;                                                \
        NumberType t;                                                                                                  \
        return StringUtil::numberFromString<NumberType>(expr->getValueString(), t);                                    \
    } break

    switch (expr->getExprResultType()) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER(VALIDATE_VALUE);
    default:
        return true;
    }
    return false;
}

ExprResultType SyntaxExprValidator::checkExprType(const SyntaxExpr *exprA, const SyntaxExpr *exprB) {
    ExprResultType resultTypeA = exprA->getExprResultType();
    ExprResultType resultTypeB = exprB->getExprResultType();
    AUTIL_LOG(TRACE3, "ExprResultTypeA : %d , ExprResultTypeB : %d", resultTypeA, resultTypeB);

    SyntaxExprType exprTypeA = exprA->getSyntaxExprType();
    SyntaxExprType exprTypeB = exprB->getSyntaxExprType();
    AUTIL_LOG(TRACE3, "SyntaxExprTypeA : %d , SyntaxExprTypeB : %d", exprTypeA, exprTypeB);

    if (exprTypeA == SYNTAX_EXPR_TYPE_CONST_VALUE && exprTypeB == SYNTAX_EXPR_TYPE_CONST_VALUE) {
        if (resultTypeA == resultTypeB) {
            return resultTypeA;
        } else {
            AUTIL_LOG(WARN, "the ConsExprResultType is not matched in two sides");
            _errorCode = ERROR_EXPRESSION_LR_TYPE_NOT_MATCH;
            ostringstream oss;
            oss << "--leftExpr:[" << dynamic_cast<const AtomicSyntaxExpr *>(exprA)->getExprString() << "]"
                << "--leftExprResultType:[" << exprA->getExprResultType() << "]"
                << "--rightExpr:[" << dynamic_cast<const AtomicSyntaxExpr *>(exprB)->getExprString() << "]"
                << "--rightExprResultType:[" << exprB->getExprResultType() << "]";
            _errorMsg = oss.str();
            return vt_unknown;
        }
    }

    if (exprTypeA != SYNTAX_EXPR_TYPE_CONST_VALUE && exprTypeB != SYNTAX_EXPR_TYPE_CONST_VALUE) {
        if (resultTypeA == resultTypeB) {
            return resultTypeA;
        } else {
            AUTIL_LOG(WARN, "the ExprResultType is not matched in two side syntax expression");
            _errorCode = ERROR_EXPRESSION_LR_TYPE_NOT_MATCH;
            ostringstream oss;
            oss << "--leftExprResultType:[" << exprA->getExprResultType() << "]"
                << "--rightExprResultType:[" << exprB->getExprResultType() << "]";
            _errorMsg = oss.str();
            return vt_unknown;
        }
    }

    if (exprTypeA == SYNTAX_EXPR_TYPE_CONST_VALUE) {
        return checkConstExprType(exprA, exprB);
    }

    if (exprTypeB == SYNTAX_EXPR_TYPE_CONST_VALUE) {
        return checkConstExprType(exprB, exprA);
    }

    return vt_unknown;
}

bool SyntaxExprValidator::checkBinaryExprWithMultiValue(const SyntaxExpr *syntaxExpr) {
    if (syntaxExpr->isMultiValue()) {
        _errorCode = ERROR_EXPRESSION_WITH_MULTI_VALUE;
        _errorMsg = "binary expression with multivalue is not supported";
        AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
        return false;
    }
    return true;
}

bool SyntaxExprValidator::checkBinaryExprWithStringType(const SyntaxExpr *syntaxExpr) {
    if (syntaxExpr->getExprResultType() == vt_string) {
        _errorCode = ERROR_STRING_IN_BINARY_EXPRESSION;
        _errorMsg = "string in binary expression is not supported.";
        AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
        return false;
    }
    return true;
}

void SyntaxExprValidator::visitAtomicSyntaxExpr(const AtomicSyntaxExpr *syntaxExpr) {
    SyntaxExprType syntaxExprType = syntaxExpr->getSyntaxExprType();
    if (SYNTAX_EXPR_TYPE_ATOMIC_ATTR == syntaxExprType) {
        const string &attrName = syntaxExpr->getValueString();
        ExprResultType exprResultType = vt_unknown;
        bool isMultiValue = false;
        bool isSubExpression = false;

        // syntaxExprType may change
        validateAttributeAndGetInfo(attrName, syntaxExprType, exprResultType, isMultiValue, isSubExpression);

        syntaxExpr->setSyntaxExprType(syntaxExprType);
        syntaxExpr->setExprResultType(exprResultType);
        syntaxExpr->setMultiValue(isMultiValue);
        syntaxExpr->setIsSubExpression(isSubExpression);
    } else if (syntaxExprType == SYNTAX_EXPR_TYPE_CONST_VALUE || syntaxExprType == SYNTAX_EXPR_TYPE_FUNC_ARGUMENT) {
        checkConstExprType(syntaxExpr);
        if (getErrorCode() != ERROR_NONE) {
            return;
        }
    }
}

void SyntaxExprValidator::visitAddSyntaxExpr(const AddSyntaxExpr *syntaxExpr) {
    ExprResultType resultType = validateBinarySyntaxExpr(syntaxExpr);
    checkBinaryExprWithMultiValue(syntaxExpr);
    checkBinaryExprWithStringType(syntaxExpr);
    syntaxExpr->setExprResultType(resultType);
}

void SyntaxExprValidator::visitMinusSyntaxExpr(const MinusSyntaxExpr *syntaxExpr) {
    ExprResultType resultType = validateBinarySyntaxExpr(syntaxExpr);
    checkBinaryExprWithMultiValue(syntaxExpr);
    checkBinaryExprWithStringType(syntaxExpr);
    syntaxExpr->setExprResultType(resultType);
}

void SyntaxExprValidator::visitMulSyntaxExpr(const MulSyntaxExpr *syntaxExpr) {
    ExprResultType resultType = validateBinarySyntaxExpr(syntaxExpr);
    checkBinaryExprWithMultiValue(syntaxExpr);
    checkBinaryExprWithStringType(syntaxExpr);
    syntaxExpr->setExprResultType(resultType);
}

void SyntaxExprValidator::visitDivSyntaxExpr(const DivSyntaxExpr *syntaxExpr) {
    ExprResultType resultType = validateBinarySyntaxExpr(syntaxExpr);
    checkBinaryExprWithMultiValue(syntaxExpr);
    checkBinaryExprWithStringType(syntaxExpr);
    syntaxExpr->setExprResultType(resultType);
}

void SyntaxExprValidator::visitPowerSyntaxExpr(const PowerSyntaxExpr *syntaxExpr) {
    ExprResultType resultType = validateBinarySyntaxExpr(syntaxExpr);
    checkBinaryExprWithMultiValue(syntaxExpr);
    checkBinaryExprWithStringType(syntaxExpr);
    syntaxExpr->setExprResultType(resultType);
}

void SyntaxExprValidator::visitLogSyntaxExpr(const LogSyntaxExpr *syntaxExpr) {
    ExprResultType resultType = validateBinarySyntaxExpr(syntaxExpr);
    checkBinaryExprWithMultiValue(syntaxExpr);
    checkBinaryExprWithStringType(syntaxExpr);
    syntaxExpr->setExprResultType(resultType);
}

void SyntaxExprValidator::visitEqualSyntaxExpr(const EqualSyntaxExpr *syntaxExpr) {
    validateBinarySyntaxExpr(syntaxExpr);
    syntaxExpr->setExprResultType(vt_bool);
    syntaxExpr->setMultiValue(false);
}

void SyntaxExprValidator::visitNotEqualSyntaxExpr(const NotEqualSyntaxExpr *syntaxExpr) {
    validateBinarySyntaxExpr(syntaxExpr);
    syntaxExpr->setExprResultType(vt_bool);
    syntaxExpr->setMultiValue(false);
}

void SyntaxExprValidator::visitLessSyntaxExpr(const LessSyntaxExpr *syntaxExpr) {
    validateBinarySyntaxExpr(syntaxExpr);
    checkBinaryExprWithMultiValue(syntaxExpr);
    syntaxExpr->setExprResultType(vt_bool);
}

void SyntaxExprValidator::visitGreaterSyntaxExpr(const GreaterSyntaxExpr *syntaxExpr) {
    validateBinarySyntaxExpr(syntaxExpr);
    checkBinaryExprWithMultiValue(syntaxExpr);
    syntaxExpr->setExprResultType(vt_bool);
}

void SyntaxExprValidator::visitLessEqualSyntaxExpr(const LessEqualSyntaxExpr *syntaxExpr) {
    validateBinarySyntaxExpr(syntaxExpr);
    checkBinaryExprWithMultiValue(syntaxExpr);
    syntaxExpr->setExprResultType(vt_bool);
}

void SyntaxExprValidator::visitGreaterEqualSyntaxExpr(const GreaterEqualSyntaxExpr *syntaxExpr) {
    validateBinarySyntaxExpr(syntaxExpr);
    checkBinaryExprWithMultiValue(syntaxExpr);
    syntaxExpr->setExprResultType(vt_bool);
}

void SyntaxExprValidator::visitAndSyntaxExpr(const AndSyntaxExpr *syntaxExpr) {
    ExprResultType resultType = validateBinarySyntaxExpr(syntaxExpr);
    checkBinaryExprWithMultiValue(syntaxExpr);
    if (_errorCode == ERROR_NONE && resultType != vt_bool) {
        _errorCode = ERROR_LOGICAL_EXPR_LR_TYPE_ERROR;
        _errorMsg = "left & right ExprResultType is not vt_bool.";
        AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
    }
    syntaxExpr->setExprResultType(vt_bool);
}

void SyntaxExprValidator::visitOrSyntaxExpr(const OrSyntaxExpr *syntaxExpr) {
    ExprResultType resultType = validateBinarySyntaxExpr(syntaxExpr);
    checkBinaryExprWithMultiValue(syntaxExpr);
    if (_errorCode == ERROR_NONE && resultType != vt_bool) {
        _errorCode = ERROR_LOGICAL_EXPR_LR_TYPE_ERROR;
        _errorMsg = "left & right ExprResultType is not vt_bool.";
        AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
    }
    syntaxExpr->setExprResultType(vt_bool);
}

void SyntaxExprValidator::visitConditionalSyntaxExpr(const ConditionalSyntaxExpr *syntaxExpr) {
    const SyntaxExpr *conditionalExpr = syntaxExpr->getConditionalExpr();
    const SyntaxExpr *trueExpr = syntaxExpr->getTrueExpr();
    const SyntaxExpr *falseExpr = syntaxExpr->getFalseExpr();
    assert(conditionalExpr);
    assert(trueExpr);
    assert(falseExpr);

    syntaxExpr->setExprResultType(vt_unknown);
    syntaxExpr->setMultiValue(false);
    syntaxExpr->setIsSubExpression(false);

    conditionalExpr->accept(this);
    if (getErrorCode() != ERROR_NONE) {
        AUTIL_LOG(WARN, "validate conditionalExpr error:[%s]", _errorMsg.c_str());
        return;
    }
    trueExpr->accept(this);
    if (getErrorCode() != ERROR_NONE) {
        AUTIL_LOG(WARN, "validate trueExpr error:[%s]", _errorMsg.c_str());
        return;
    }
    falseExpr->accept(this);
    if (getErrorCode() != ERROR_NONE) {
        AUTIL_LOG(WARN, "validate falseExpr error:[%s]", _errorMsg.c_str());
        return;
    }

    // for virtual attribute
    const SyntaxExpr *actualConditionalExpr = findActualExpr(conditionalExpr);
    const SyntaxExpr *actualTrueExpr = findActualExpr(trueExpr);
    const SyntaxExpr *actualFalseExpr = findActualExpr(falseExpr);
    if (NULL == actualConditionalExpr || NULL == actualTrueExpr || NULL == actualFalseExpr) {
        _errorCode = ERROR_STRING_IN_BINARY_EXPRESSION;
        _errorMsg = "actualConditionalExpr or actualLeftExpr or actualRightExpr is NULL";
        AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
        return;
    }

    // for multiValue
    syntaxExpr->setMultiValue(actualTrueExpr->isMultiValue() || actualFalseExpr->isMultiValue());
    if (!checkBinaryExprWithMultiValue(syntaxExpr)) {
        return;
    }

    // for result type
    syntaxExpr->setIsSubExpression(actualTrueExpr->isSubExpression() || actualFalseExpr->isSubExpression());
    syntaxExpr->setExprResultType(checkExprType(actualTrueExpr, actualFalseExpr));

    // type conditionnalExpr check
    if (vt_bool != actualConditionalExpr->getExprResultType()) {
        _errorCode = ERROR_LOGICAL_EXPR_TYPE;
        _errorMsg = "result type of condition expression must be bool.";
        AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
        return;
    }
}

void SyntaxExprValidator::visitMultiSyntaxExpr(const MultiSyntaxExpr *syntaxExpr) {
    bool isSub = false;
    for (auto expr : syntaxExpr->getSyntaxExprs()) {
        expr->accept(this);
        if (getErrorCode() != ERROR_NONE) {
            return;
        }
        const SyntaxExpr *actualExpr = findActualExpr(expr);
        if (NULL == actualExpr) {
            _errorCode = ERROR_STRING_MULTI_SYNTAXEXPR;
            _errorMsg = "actualExpr is NULL";
            return;
        }
        if (actualExpr->isMultiValue()) {
            _errorCode = ERROR_EXPRESSION_WITH_MULTI_VALUE;
            _errorMsg = "multi syntax expr with multivalue is not supported.";
            return;
        }
        if (actualExpr->getSyntaxExprType() == SYNTAX_EXPR_TYPE_CONST_VALUE) {
            _errorCode = ERROR_CONST_EXPRESSION_VALUE;
            _errorMsg = "multi syntax expr with const value is not supported.";
            return;
        }
        if (actualExpr->isSubExpression()) {
            isSub = true;
        }
    }
    syntaxExpr->setMultiValue(false);
    syntaxExpr->setExprResultType(vt_uint64);
    syntaxExpr->setIsSubExpression(isSub);
    return;
}

bool SyntaxExprValidator::checkBitOpResultType(ExprResultType resultType) {
    switch (resultType) {
    case vt_int8:
    case vt_int16:
    case vt_int32:
    case vt_int64:
    case vt_uint8:
    case vt_uint16:
    case vt_uint32:
    case vt_uint64:
        return true;
    case vt_float:
        _errorMsg = "[float type]";
        break;
    case vt_double:
        _errorMsg = "[double type]";
        break;
    case vt_string:
        _errorMsg = "[string type]";
        break;
    default:
        _errorMsg = "[unknown type]";
    }

    _errorCode = ERROR_BIT_UNSUPPORT_TYPE_ERROR;
    AUTIL_LOG(WARN, "%s %s", "bit expression type is not supported", _errorMsg.c_str());
    return false;
}

void SyntaxExprValidator::checkBitOpSyntaxExpr(const BinarySyntaxExpr *syntaxExpr) {
    ExprResultType resultType = validateBinarySyntaxExpr(syntaxExpr);
    checkBinaryExprWithMultiValue(syntaxExpr);
    if (_errorCode == ERROR_NONE) {
        checkBitOpResultType(resultType);
    }
    syntaxExpr->setExprResultType(resultType);
}

void SyntaxExprValidator::visitBitAndSyntaxExpr(const BitAndSyntaxExpr *syntaxExpr) {
    checkBitOpSyntaxExpr(syntaxExpr);
}

void SyntaxExprValidator::visitBitXorSyntaxExpr(const BitXorSyntaxExpr *syntaxExpr) {
    checkBitOpSyntaxExpr(syntaxExpr);
}

void SyntaxExprValidator::visitBitOrSyntaxExpr(const BitOrSyntaxExpr *syntaxExpr) { checkBitOpSyntaxExpr(syntaxExpr); }

void SyntaxExprValidator::visitRankSyntaxExpr(const RankSyntaxExpr *syntaxExpr) {}

void SyntaxExprValidator::visitFuncSyntaxExpr(const FuncSyntaxExpr *syntaxExpr) {
    const FuncSyntaxExpr::SubExprType &exprs = syntaxExpr->getParam();
    for (FuncSyntaxExpr::SubExprType::const_iterator i = exprs.begin(); i != exprs.end(); ++i) {
        (*i)->accept(this);
        if (getErrorCode() != ERROR_NONE) {
            AUTIL_LOG(WARN, "validate subExpr error:[%s]", _errorMsg.c_str());
            return;
        }
    }
    const std::string &funcName = syntaxExpr->getFuncName();
    const FunctionProtoInfo *protoInfo = validateFuncName(funcName);

    bool isCavaFunction = false;
    FunctionProtoInfoPtr protoInfoPtr;
    if (protoInfo == NULL) {
        isCavaFunction = true;
        protoInfoPtr = validateCavaFuncName(syntaxExpr);
        if (protoInfoPtr) {
            protoInfo = protoInfoPtr.get();
        } else {
            AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
            return;
        }
    }

    if (protoInfo->argCount != FUNCTION_UNLIMITED_ARGUMENT_COUNT && protoInfo->argCount != exprs.size()) {
        _errorMsg = "Invalid argument count for :" + funcName +
                    ",expected count:" + autil::StringUtil::toString(protoInfo->argCount) +
                    ",actual count:" + autil::StringUtil::toString(exprs.size());

        AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
        _errorCode = ERROR_FUNCTION_ARGCOUNT;
        return;
    }

    if (protoInfo->resultType == vt_unknown) {
        if (0 == exprs.size()) {
            _errorMsg = "Invalid argument count for :" + funcName + ",argument count should not be zero";
            AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
            _errorCode = ERROR_FUNCTION_ARGCOUNT;
            return;
        }
        SyntaxExpr *expr = *(exprs.begin());
        syntaxExpr->setExprResultType(expr->getExprResultType());
        syntaxExpr->setMultiValue(expr->isMultiValue());
    } else if (protoInfo->resultType == vt_unknown_max_type) {
        if (0 == exprs.size()) {
            _errorMsg = "Invalid argument count for :" + funcName + ",argument count should not be zero";
            AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
            _errorCode = ERROR_FUNCTION_ARGCOUNT;
            return;
        }
        int16_t maxTypeOrder = 0;
        for (const auto &expr : exprs) {
            auto type = expr->getExprResultType();
            auto order = matchdoc::getBuiltinTypeOrder(type);
            if (expr->isMultiValue() || order == 0) {
                AUTIL_LOG(ERROR, "Only support number type, invalid value type[%d]", (int)type);
                _errorCode = ERROR_FUNCTION_PARAM_TYPE_UNSUPPORT;
                return;
            }
            if (order > maxTypeOrder) {
                syntaxExpr->setExprResultType(type);
                syntaxExpr->setMultiValue(expr->isMultiValue());
                maxTypeOrder = order;
            }
        }
    } else if (protoInfo->resultType == vt_func_template) {
        if (0 == exprs.size()) {
            _errorMsg = "Invalid argument count for :" + funcName + ",argument count should not be zero";
            AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
            _errorCode = ERROR_FUNCTION_ARGCOUNT;
            return;
        }
        SyntaxExpr *expr = *(exprs.begin());
        // get type from first expr, but get multi value flag from proto
        syntaxExpr->setExprResultType(expr->getExprResultType());
        syntaxExpr->setMultiValue(protoInfo->isMulti);
    } else if (protoInfo->resultType == vt_hamming_type) {
        if (2 != exprs.size()) {
            _errorMsg = funcName + " argument count should be 2, but get: " + StringUtil::toString(exprs.size());
            AUTIL_LOG(ERROR, "%s", _errorMsg.c_str());
            _errorCode = ERROR_FUNCTION_ARGCOUNT;
            return;
        }
        SyntaxExpr *expr = exprs[1];
        syntaxExpr->setExprResultType(matchdoc::bt_uint16);
        syntaxExpr->setMultiValue(expr->isMultiValue());
    } else {
        syntaxExpr->setExprResultType(protoInfo->resultType);
        syntaxExpr->setMultiValue(protoInfo->isMulti);
    }

    FuncActionScopeType scopeType = protoInfo->scopeType;
    if (scopeType == FUNC_ACTION_SCOPE_ADAPTER) {
        scopeType = FUNC_ACTION_SCOPE_MAIN_DOC;
        for (size_t i = 0; i < exprs.size(); i++) {
            if (exprs[i]->isSubExpression()) {
                scopeType = FUNC_ACTION_SCOPE_SUB_DOC;
                break;
            }
        }
    }
    if (scopeType == FUNC_ACTION_SCOPE_SUB_DOC) {
        if (_isSubDocQuery) {
            syntaxExpr->setIsSubExpression(true);
        } else {
            if (!isCavaFunction) {
                _errorMsg = "function:" + funcName + " scope is sub doc, but subDoc display type is not set.";
                _errorCode = ERROR_FUNCTION_SCOPE_IS_NOT_CORRECT;
                AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
                return;
            }
        }
    }
}

const FunctionProtoInfo *SyntaxExprValidator::validateFuncName(const string &funcName) {
    const FunctionProtoInfo *protoInfo = NULL;
    for (auto funcInfoMap : _funcInfoMaps) {
        auto fIt = funcInfoMap->find(funcName);
        if (fIt == funcInfoMap->end()) {
            _errorMsg = "Unsupported function:" + funcName;
            _errorCode = ERROR_FUNCTION_NOT_DEFINED;
            return NULL;
        }
        if (NULL == protoInfo) {
            protoInfo = &(fIt->second);
        } else if (*protoInfo != fIt->second) {
            _errorMsg = "incompatible function parameter, function name: " + funcName;
            _errorCode = ERROR_FUNCTION_INCOMPATIBLE_PARAMETER;
            return NULL;
        }
    }
    return protoInfo;
}

FunctionProtoInfoPtr SyntaxExprValidator::validateCavaFuncName(const FuncSyntaxExpr *syntaxExpr) {
    std::string processFuncName =
        syntaxExpr->getFuncName() + CAVA_INNER_METHOD_SEP + CAVA_INNER_METHOD_NAME; // "_inner_process"
    const FuncSyntaxExpr::SubExprType &exprs = syntaxExpr->getParam();
    for (FuncSyntaxExpr::SubExprType::const_iterator i = exprs.begin(); i != exprs.end(); ++i) {
        if (suez::turing::SYNTAX_EXPR_TYPE_FUNC_ARGUMENT == (*i)->getSyntaxExprType()) { //常量参数 不参与计算函数签名
            continue;
        }

        ExprResultType type = (*i)->getExprResultType();
        bool isMulti = (*i)->isMultiValue();

#define TYPE_CASE_HELPER(type)                                                                                         \
    case type:                                                                                                         \
        if (isMulti) {                                                                                                 \
            typedef matchdoc::MatchDocBuiltinType2CppType<type, true>::CppType cppType;                                \
            processFuncName += CAVA_INNER_METHOD_SEP + ha3::CppType2CavaTypeName<cppType>();                           \
        } else {                                                                                                       \
            typedef matchdoc::MatchDocBuiltinType2CppType<type, false>::CppType cppType;                               \
            processFuncName += CAVA_INNER_METHOD_SEP + ha3::CppType2CavaTypeName<cppType>();                           \
        }                                                                                                              \
        break;

        switch (type) {
            NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL_AND_STRING(TYPE_CASE_HELPER);
        default:
            _errorMsg = "unsupport type for param:" + (*i)->getExprString() + ", function name: " + processFuncName;
            _errorCode = ERROR_FUNCTION_PARAM_TYPE_UNSUPPORT;
            AUTIL_LOG(WARN, "unsupport type[%d] for param [%s]", (int32_t)type, (*i)->getExprString().c_str());
            return FunctionProtoInfoPtr();
        }
    }

    FunctionProtoInfoPtr protoInfoPtr;
    for (auto &cavaPluginManager : _cavaPluginManagerVec) {
        CavaModuleInfoPtr moduleInfoPtr = cavaPluginManager->getCavaModuleInfo(processFuncName);
        if (!moduleInfoPtr) {
            _errorMsg = "Unsupported cava function:" + processFuncName;
            _errorCode = ERROR_FUNCTION_NOT_DEFINED;
            return FunctionProtoInfoPtr();
        }
        CavaFunctionModuleInfo *funcModuleInfo = static_cast<CavaFunctionModuleInfo *>(moduleInfoPtr.get());
        if (!protoInfoPtr) {
            protoInfoPtr = funcModuleInfo->functionProto;
        } else if (*protoInfoPtr != *(funcModuleInfo->functionProto)) {
            _errorMsg = "incompatible cava function parameter, function name: " + processFuncName;
            _errorCode = ERROR_FUNCTION_INCOMPATIBLE_RETTYPE;
            return FunctionProtoInfoPtr();
        }
    }

    //如果成功找到, 则之前的errorcode 需要清空
    if (protoInfoPtr) {
        reset();
    }
    return protoInfoPtr;
}

void SyntaxExprValidator::reset() {
    _errorCode = ERROR_NONE;
    _errorMsg = "";
}

} // namespace turing
} // namespace suez
