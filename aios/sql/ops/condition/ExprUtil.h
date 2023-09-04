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

#include <map>
#include <stddef.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/legacy/RapidJsonCommon.h"
#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/common/common.h"
#include "sql/ops/condition/CaseExpression.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/syntax/AtomicSyntaxExpr.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"
#include "suez/turing/expression/syntax/SyntaxParser.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace suez {
namespace turing {
class AttributeExpression;
} // namespace turing
} // namespace suez

namespace sql {

struct ExprEntity {
    std::string exprStr;
    std::string exprJson;
};

class ExprUtil {
public:
    ExprUtil() {}

public:
    static suez::turing::AttributeExpression *
    createConstExpression(autil::mem_pool::Pool *pool,
                          const suez::turing::AtomicSyntaxExpr *syntaxExpr,
                          suez::turing::VariableType type);
    static suez::turing::AttributeExpression *
    createConstExpression(autil::mem_pool::Pool *pool,
                          const std::string &constValue,
                          const std::string &exprStr,
                          suez::turing::VariableType type);
    static bool parseOutputExprs(autil::mem_pool::Pool *pool,
                                 const std::string &exprsStr,
                                 std::map<std::string, ExprEntity> &exprsMap);
    static bool parseOutputExprs(autil::mem_pool::Pool *pool,
                                 const std::string &exprsJson,
                                 std::map<std::string, ExprEntity> &exprsMap,
                                 std::unordered_map<std::string, std::string> &renameMap);

    static std::pair<suez::turing::VariableType, bool>
    transSqlTypeToVariableType(const std::string &sqlTypeStr);
    static std::string transVariableTypeToSqlType(suez::turing::VariableType vt);

public:
    static bool isUdf(const autil::SimpleValue &simpleVal);
    static bool isSameUdf(const autil::SimpleValue &simpleVal, const std::string &udfType);
    static bool isInOp(const autil::SimpleValue &simpleVal);
    static bool isCaseOp(const autil::SimpleValue &simpleVal);
    static std::string toExprString(const autil::SimpleValue &simpleVal,
                                    bool &hasError,
                                    std::string &errorInfo,
                                    std::unordered_map<std::string, std::string> &renameMap);
    static bool createExprString(const autil::SimpleValue &simpleVal, std::string &exprStr);
    template <typename T>
    static suez::turing::AttributeExpression *
    CreateCaseExpression(T &attributeExprCreator,
                         const std::string &outputFieldType,
                         const autil::SimpleValue &simpleVal,
                         bool &hasError,
                         std::string &errorInfo,
                         autil::mem_pool::Pool *pool);
    static void convertColumnName(std::string &columnName);
    static bool unCast(const autil::SimpleValue **simpleValueWrapper);
    static bool reverseOp(std::string &op);
    static bool isItem(const autil::SimpleValue &item);
    static bool
    parseItemVariable(const autil::SimpleValue &item, std::string &itemName, std::string &itemKey);
    static std::string getItemFullName(const std::string &itemName, const std::string &itemKey);
    static bool parseExprsJson(const std::string &exprsJson, autil::SimpleDocument &simpleDoc);
    static bool isBoolExpr(const autil::SimpleValue &simpleVal);
};

#define RETURN_NULL_AND_SET_ERROR(str)                                                             \
    hasError = true;                                                                               \
    errorInfo = str;                                                                               \
    return nullptr;

// if return nullptr, hasError must be true
template <typename T>
suez::turing::AttributeExpression *
ExprUtil::CreateCaseExpression(T &attributeExprCreator,
                               const std::string &outputFieldType,
                               const autil::SimpleValue &simpleVal,
                               bool &hasError,
                               std::string &errorInfo,
                               autil::mem_pool::Pool *pool) {
    suez::turing::VariableType exprType = transSqlTypeToVariableType(outputFieldType).first;
    const autil::SimpleValue &param = simpleVal[SQL_CONDITION_PARAMETER];
    std::vector<suez::turing::AttributeExpression *> caseParamExprs;
    for (size_t i = 0; i < param.Size(); ++i) {
        std::string exprStr;
        if (!createExprString(param[i], exprStr)) {
            RETURN_NULL_AND_SET_ERROR(" create expr string failed");
        }
        suez::turing::SyntaxExpr *syntaxExpr = suez::turing::SyntaxParser::parseSyntax(exprStr);
        if (!syntaxExpr) {
            RETURN_NULL_AND_SET_ERROR(" parse syntax " + exprStr + "failed");
        }

        suez::turing::AttributeExpression *caseParamExpr = nullptr;
        auto syntaxExprType = syntaxExpr->getSyntaxExprType();
        if (syntaxExprType == suez::turing::SYNTAX_EXPR_TYPE_CONST_VALUE) {
            suez::turing::AtomicSyntaxExpr *atomicExpr
                = dynamic_cast<suez::turing::AtomicSyntaxExpr *>(syntaxExpr);
            if (atomicExpr) {
                caseParamExpr = ExprUtil::createConstExpression(pool, atomicExpr, exprType);
                if (caseParamExpr) {
                    attributeExprCreator->addNeedDeleteExpr(caseParamExpr);
                }
            }
        } else {
            caseParamExpr = attributeExprCreator->createAttributeExpression(exprStr);
        }
        delete syntaxExpr;
        if (caseParamExpr == nullptr) {
            RETURN_NULL_AND_SET_ERROR("create case param expr failed");
        }
        caseParamExprs.emplace_back(caseParamExpr);
    }
    suez::turing::AttributeExpression *caseExpr
        = CaseExpressionCreator::createCaseExpression(exprType, caseParamExprs, pool);
    if (caseExpr == nullptr) {
        RETURN_NULL_AND_SET_ERROR(" create case attribute expression failed");
    }
    attributeExprCreator->addNeedDeleteExpr(caseExpr);
    return caseExpr;
}

#undef RETURN_NULL_AND_SET_ERROR

} // namespace sql
