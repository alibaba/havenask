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
#include <string>
#include <vector>

#include "autil/Log.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/function/FunctionMap.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"
#include "suez/turing/expression/syntax/SyntaxExprVisitor.h"
#include "suez/turing/expression/util/VirtualAttribute.h"

namespace suez {
namespace turing {
class AddSyntaxExpr;
class AndSyntaxExpr;
class AtomicSyntaxExpr;
class AttributeInfos;
class BinarySyntaxExpr;
class BitAndSyntaxExpr;
class BitOrSyntaxExpr;
class BitXorSyntaxExpr;
class DivSyntaxExpr;
class EqualSyntaxExpr;
class FuncSyntaxExpr;
class GreaterEqualSyntaxExpr;
class GreaterSyntaxExpr;
class LessEqualSyntaxExpr;
class LessSyntaxExpr;
class LogSyntaxExpr;
class MinusSyntaxExpr;
class MulSyntaxExpr;
class MultiSyntaxExpr;
class NotEqualSyntaxExpr;
class OrSyntaxExpr;
class PowerSyntaxExpr;
class RankSyntaxExpr;
class ConditionalSyntaxExpr;

enum SyntaxErrorCode {
    ERROR_NONE = 0,
    ERROR_SYNTAX_EXPRESSION_SUB_DOC_NOT_SUPPORT = 1999,
    ERROR_ATTRIBUTE_NOT_EXIST = 2000,
    ERROR_EXPRESSION_LR_TYPE_NOT_MATCH = 2001,
    ERROR_EXPRESSION_LR_ALL_CONST_VALUE = 2002,
    ERROR_CONST_EXPRESSION_TYPE = 2003,
    ERROR_LOGICAL_EXPR_LR_TYPE_ERROR = 2004,
    ERROR_STRING_IN_BINARY_EXPRESSION = 2005,
    ERROR_EXPRESSION_RESULT_UNKNOWN_TYPE = 2006,
    ERROR_EXPRESSION_WITH_MULTI_VALUE = 2007,
    ERROR_BIT_UNSUPPORT_TYPE_ERROR = 2010,
    ERROR_CONST_EXPRESSION_VALUE = 2011,
    ERROR_UNKNONW_ATOMIC_SYNTAX_EXPRESSION = 2012,
    ERROR_LOGICAL_EXPR_TYPE = 2013,

    ERROR_FUNCTION_NOT_DEFINED = 2100,
    ERROR_FUNCTION_ARGCOUNT = 2101,
    ERROR_FUNCTION_INCOMPATIBLE_PARAMETER = 2102,

    ERROR_FUNCTION_SCOPE_IS_NOT_CORRECT = 2104,
    ERROR_FUNCTION_PARAM_TYPE_UNSUPPORT = 2105,
    ERROR_FUNCTION_INCOMPATIBLE_RETTYPE = 2106,

    ERROR_STRING_MULTI_SYNTAXEXPR = 2109,
};

class SyntaxExprValidator : public SyntaxExprVisitor {
public:
    SyntaxExprValidator(const AttributeInfos *attrInfos,
                        const VirtualAttributes &virtualAttributes,
                        bool isSubDocQuery);
    ~SyntaxExprValidator();

public:
    bool validate(const SyntaxExpr *expr) {
        reset();
        expr->accept(this);
        return _errorCode == ERROR_NONE;
    }
    SyntaxErrorCode getErrorCode() const { return _errorCode; }
    const std::string &getErrorMsg() const { return _errorMsg; }
    void addFuncInfoMap(const FuncInfoMap *funcInfoMap) {
        assert(funcInfoMap);
        _funcInfoMaps.push_back(funcInfoMap);
    }
    void addCavaPluginManager(const CavaPluginManager *cavaPluginManager) {
        assert(cavaPluginManager);
        _cavaPluginManagerVec.push_back(cavaPluginManager);
    }
    void addVirtualAttribute(VirtualAttribute *va) { _virtualAttributes.push_back(va); }
    virtual void visitAtomicSyntaxExpr(const AtomicSyntaxExpr *syntaxExpr);
    virtual void visitAddSyntaxExpr(const AddSyntaxExpr *syntaxExpr);
    virtual void visitMinusSyntaxExpr(const MinusSyntaxExpr *syntaxExpr);
    virtual void visitMulSyntaxExpr(const MulSyntaxExpr *syntaxExpr);
    virtual void visitDivSyntaxExpr(const DivSyntaxExpr *syntaxExpr);
    virtual void visitPowerSyntaxExpr(const PowerSyntaxExpr *syntaxExpr);
    virtual void visitLogSyntaxExpr(const LogSyntaxExpr *syntaxExpr);
    virtual void visitEqualSyntaxExpr(const EqualSyntaxExpr *syntaxExpr);
    virtual void visitNotEqualSyntaxExpr(const NotEqualSyntaxExpr *syntaxExpr);
    virtual void visitLessSyntaxExpr(const LessSyntaxExpr *syntaxExpr);
    virtual void visitGreaterSyntaxExpr(const GreaterSyntaxExpr *syntaxExpr);
    virtual void visitLessEqualSyntaxExpr(const LessEqualSyntaxExpr *syntaxExpr);
    virtual void visitGreaterEqualSyntaxExpr(const GreaterEqualSyntaxExpr *syntaxExpr);
    virtual void visitAndSyntaxExpr(const AndSyntaxExpr *syntaxExpr);
    virtual void visitOrSyntaxExpr(const OrSyntaxExpr *syntaxExpr);
    virtual void visitRankSyntaxExpr(const RankSyntaxExpr *syntaxExpr);
    virtual void visitFuncSyntaxExpr(const FuncSyntaxExpr *syntaxExpr);
    virtual void visitBitAndSyntaxExpr(const BitAndSyntaxExpr *syntaxExpr);
    virtual void visitBitXorSyntaxExpr(const BitXorSyntaxExpr *syntaxExpr);
    virtual void visitBitOrSyntaxExpr(const BitOrSyntaxExpr *syntaxExpr);
    virtual void visitConditionalSyntaxExpr(const ConditionalSyntaxExpr *syntaxExpr);
    virtual void visitMultiSyntaxExpr(const MultiSyntaxExpr *syntaxExpr);

private:
    const FunctionProtoInfo *validateFuncName(const std::string &funcName);
    FunctionProtoInfoPtr validateCavaFuncName(const FuncSyntaxExpr *syntaxExpr);
    void validateAttributeAndGetInfo(const std::string &attrName,
                                     SyntaxExprType &syntaxExprType,
                                     ExprResultType &resultType,
                                     bool &isMultiValue,
                                     bool &isSubExpression);
    const SyntaxExpr *findActualExpr(const SyntaxExpr *syntaxExpr);
    ExprResultType validateBinarySyntaxExpr(const BinarySyntaxExpr *syntaxExpr);

    void checkConstExprType(const SyntaxExpr *constExpr);
    ExprResultType checkConstExprType(const SyntaxExpr *exprA, const SyntaxExpr *exprB);
    bool checkConstExprValue(const AtomicSyntaxExpr *expr);
    ExprResultType checkExprType(const SyntaxExpr *exprA, const SyntaxExpr *exprB);

    bool checkBinaryExprWithMultiValue(const SyntaxExpr *syntaxExpr);
    bool checkBinaryExprWithStringType(const SyntaxExpr *syntaxExpr);
    bool
    checkStringValueInBinaryExpr(const SyntaxExpr *syntaxExpr, const SyntaxExpr *leftExpr, const SyntaxExpr *rightExpr);

    bool supportStringAttribute(SyntaxExprType syntaxExprType) const;

    void checkBitOpSyntaxExpr(const BinarySyntaxExpr *syntaxExpr);
    bool checkBitOpResultType(ExprResultType resultType);
    void reset();

private:
    const AttributeInfos *_attrInfos = nullptr;
    FuncInfoMaps _funcInfoMaps;
    std::vector<const CavaPluginManager *> _cavaPluginManagerVec;
    VirtualAttributes _virtualAttributes;
    SyntaxErrorCode _errorCode;
    std::string _errorMsg;
    bool _isSubDocQuery;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
