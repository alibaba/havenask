#pragma once

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/mem_pool/Pool.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <ha3/sql/ops/condition/CaseExpression.h>
#include <suez/turing/expression/syntax/SyntaxParser.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <autil/legacy/RapidJsonCommon.h>
#include <map>
#include <string>

BEGIN_HA3_NAMESPACE(sql);

struct ExprEntity
{
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
                                 std::map<std::string, std::string> &renameMap);


    static suez::turing::VariableType transSqlTypeToVariableType(const std::string &sqlTypeStr);
    static std::string transVariableTypeToSqlType(suez::turing::VariableType vt);
public:
    static bool isUdf(const autil_rapidjson::SimpleValue &simpleVal);
    static bool isCastUdf(const autil_rapidjson::SimpleValue &simpleVal);
    static bool isContainUdf(const autil_rapidjson::SimpleValue &simpleVal);
    static bool isHaInUdf(const autil_rapidjson::SimpleValue &simpleVal);
    static bool isMultiCastUdf(const autil_rapidjson::SimpleValue &simpleVal);
    static bool isAnyUdf(const autil_rapidjson::SimpleValue &simpleVal);
    static bool isCaseOp(const autil_rapidjson::SimpleValue &simpleVal);
    static std::string toExprString(const autil_rapidjson::SimpleValue &simpleVal,
                                    bool &hasError, std::string &errorInfo,
                                    std::map<std::string, std::string> &renameMap);
    template<typename T>
    static suez::turing::AttributeExpression*
    CreateCaseExpression(T& attributeExprCreator,
                         const std::string &outputFieldType,
                         const autil_rapidjson::SimpleValue &simpleVal,
                         bool &hasError, std::string &errorInfo,
                         std::map<std::string, std::string> &renameMap,
                         autil::mem_pool::Pool* pool);
    template<typename T>
    static suez::turing::AttributeExpression *createCaseParamExpr(
        T& attributeExprCreator,
        const std::string &exprStr,
        suez::turing::VariableType exprType,
        suez::turing::SyntaxExpr *syntaxExpr,
        autil::mem_pool::Pool* pool,
        bool &hasError,
        std::string &errorInfo);
    static void convertColumnName(std::string &columnName);
    static bool unCast(const autil_rapidjson::SimpleValue **simpleValueWrapper);
    static bool reverseOp(std::string &op);
    static bool isItem(const autil_rapidjson::SimpleValue &item);
    static bool parseItemVariable(const autil_rapidjson::SimpleValue &item,
                                  std::string &itemName,
                                  std::string &itemKey);
    static std::string getItemFullName(
            const std::string &itemName,
            const std::string &itemKey);
    static bool parseExprsJson(const std::string& exprsJson,
                               autil_rapidjson::SimpleDocument &simpleDoc);
    static bool isBoolExpr(const autil_rapidjson::SimpleValue &simpleVal);

private:
    HA3_LOG_DECLARE();
};

//if return NULL, hasError must be true
template<typename T>
suez::turing::AttributeExpression *ExprUtil::CreateCaseExpression(
        T& attributeExprCreator,
        const std::string &outputFieldType,
        const autil_rapidjson::SimpleValue &simpleVal,
        bool &hasError, std::string &errorInfo,
        std::map<std::string, std::string> &renameMap,
        autil::mem_pool::Pool* pool)
{
    suez::turing::VariableType exprType;
    if (outputFieldType == "CONDITION_BOOLEAN") {
        exprType = vt_bool;
    } else {
        exprType = transSqlTypeToVariableType(outputFieldType);
    }

    const autil_rapidjson::SimpleValue &param = simpleVal[SQL_CONDITION_PARAMETER];
    std::vector<suez::turing::AttributeExpression*> caseParamExprs;
    for (size_t i = 0; i < param.Size(); ++i)
    {
        std::string exprStr = toExprString(param[i], hasError, errorInfo, renameMap);
        if (hasError)
        {
            return NULL;
        }
        suez::turing::SyntaxExpr *syntaxExpr = suez::turing::SyntaxParser::parseSyntax(exprStr);
        if (!syntaxExpr) {
            hasError = true;
            errorInfo = " parse syntax " + exprStr + "failed";
            return NULL;
        }

        suez::turing::AttributeExpression *caseParamExpr = createCaseParamExpr(
                attributeExprCreator, exprStr, exprType,
                syntaxExpr, pool, hasError, errorInfo);
        delete syntaxExpr;
        if (caseParamExpr == nullptr) {
            return nullptr;
        }
        caseParamExprs.push_back(caseParamExpr);
    }
    suez::turing::AttributeExpression *caseExpr = CaseExpressionCreator::createCaseExpression(exprType, caseParamExprs, pool);
    if (unlikely(caseExpr == NULL))
    {
        hasError = true;
        errorInfo = " create case attribute expression failed";
        return NULL;
    }
    attributeExprCreator->addNeedDeleteExpr(caseExpr);
    return caseExpr;
}

template<typename T>
suez::turing::AttributeExpression *ExprUtil::createCaseParamExpr(
        T& attributeExprCreator,
        const std::string &exprStr,
        suez::turing::VariableType exprType,
        suez::turing::SyntaxExpr *syntaxExpr,
        autil::mem_pool::Pool* pool,
        bool &hasError,
        std::string &errorInfo)
{
    suez::turing::AttributeExpression *caseParamExpr = nullptr;
    if (syntaxExpr->getSyntaxExprType() == suez::turing::SYNTAX_EXPR_TYPE_CONST_VALUE) {
        suez::turing::AtomicSyntaxExpr *atomicExpr =
            dynamic_cast<suez::turing::AtomicSyntaxExpr*>(syntaxExpr);
        if (atomicExpr == NULL) {
            hasError = true;
            errorInfo = " syntax expression of " + exprStr + " is not atomic ";
            return NULL;
        }
        caseParamExpr = ExprUtil::createConstExpression(pool, atomicExpr, exprType);
        if (caseParamExpr) {
            attributeExprCreator->addNeedDeleteExpr(caseParamExpr);
        } else {
            hasError = true;
            errorInfo = " create const expression " + exprStr + " failed";
            return NULL;
        }
    } else {
        caseParamExpr = attributeExprCreator->createAttributeExpression(exprStr);
    }
    if (unlikely(caseParamExpr == NULL))
    {
        hasError = true;
        errorInfo = " create case param attribute expression failed["  + exprStr + "]";
        return NULL;
    }
    return caseParamExpr;
}

END_HA3_NAMESPACE(sql);
