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
#include "sql/ops/condition/ExprUtil.h"

#include <assert.h>
#include <cstddef>
#include <rapidjson/error/en.h>
#include <stdint.h>
#include <stdio.h>
#include <unordered_map>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "autil/legacy/RapidJsonHelper.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "matchdoc/Trait.h"
#include "rapidjson/document.h"
#include "sql/common/Log.h"
#include "sql/ops/condition/ConstExpression.h"
#include "sql/ops/condition/ExprGenerateVisitor.h"
#include "sql/ops/condition/SqlJsonUtil.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"
#include "suez/turing/expression/syntax/AtomicSyntaxExpr.h"

using namespace std;
using namespace matchdoc;
using namespace autil;
using namespace suez::turing;

namespace sql {

AttributeExpression *ExprUtil::createConstExpression(autil::mem_pool::Pool *pool,
                                                     const AtomicSyntaxExpr *atomicSyntaxExpr,
                                                     suez::turing::VariableType type) {
    return createConstExpression(
        pool, atomicSyntaxExpr->getValueString(), atomicSyntaxExpr->getExprString(), type);
}

AttributeExpression *ExprUtil::createConstExpression(autil::mem_pool::Pool *pool,
                                                     const std::string &constValue,
                                                     const std::string &exprStr,
                                                     suez::turing::VariableType type) {
#define CREATE_CONST_ATTR_EXPR_HELPER(vt)                                                          \
    case vt: {                                                                                     \
        typedef VariableTypeTraits<vt, false>::AttrExprType T;                                     \
        T exprValue;                                                                               \
        if (StringUtil::numberFromString<T>(constValue, exprValue)) {                              \
            ret = POOL_NEW_CLASS(pool, ConstExpression<T>, exprValue);                             \
        } else {                                                                                   \
            SQL_LOG(WARN, "createConstAttrExpr for number failed, value: %s", constValue.c_str()); \
        }                                                                                          \
        break;                                                                                     \
    }

    AttributeExpression *ret = nullptr;
    switch (type) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL(CREATE_CONST_ATTR_EXPR_HELPER);
    case vt_string: {
        typedef ConstExpression<MultiChar> ConstStringExpression;
        MultiChar mc(
            MultiValueCreator::createMultiValueBuffer(constValue.data(), constValue.size(), pool));
        ret = POOL_NEW_CLASS(pool, ConstStringExpression, mc);
        break;
    }
    default:
        SQL_LOG(WARN, "Does not support this type:%d", (int32_t)type);
        assert(false);
    }
#undef CREATE_CONST_ATTR_EXPR_HELPER
    if (ret) {
        ret->setOriginalString(exprStr);
    }
    return ret;
}

bool ExprUtil::parseOutputExprs(autil::mem_pool::Pool *pool,
                                const string &exprsJson,
                                map<string, ExprEntity> &exprsMap) {
    std::unordered_map<std::string, std::string> renameMap;
    return parseOutputExprs(pool, exprsJson, exprsMap, renameMap);
}

bool ExprUtil::parseOutputExprs(autil::mem_pool::Pool *pool,
                                const std::string &exprsJson,
                                std::map<std::string, ExprEntity> &exprsMap,
                                std::unordered_map<std::string, std::string> &renameMap) {
    if (exprsJson.empty()) {
        return true;
    }
    AutilPoolAllocator allocator(pool);
    SimpleDocument simpleDoc(&allocator);
    if (unlikely(!ExprUtil::parseExprsJson(exprsJson, simpleDoc))) {
        SQL_LOG(WARN, "parse failed, exprsJson[%s]", exprsJson.c_str());
        return false;
    }
    for (SimpleValue::ConstMemberIterator itr = simpleDoc.MemberBegin();
         itr != simpleDoc.MemberEnd();
         ++itr) {
        const string &key = SqlJsonUtil::isColumn(itr->name) ? SqlJsonUtil::getColumnName(itr->name)
                                                             : itr->name.GetString();
        ExprEntity exprEntity;
        bool hasError = false;
        string errorInfo;
        if (ExprUtil::isCaseOp(itr->value)) {
            exprEntity.exprJson = RapidJsonHelper::SimpleValue2Str(itr->value);
            exprsMap.emplace(key, std::move(exprEntity));
        } else {
            string value = ExprUtil::toExprString(itr->value, hasError, errorInfo, renameMap);
            if (hasError) {
                SQL_LOG(WARN, "to expression string failed [%s]", errorInfo.c_str());
                return false;
            }
            auto iter = renameMap.find(value);
            if (iter != renameMap.end() && iter->second == key) {
                // pass
                // ignore pure ITEM operator:
                //     tags['host'] = ITEM(tags, host)
            } else {
                exprEntity.exprStr = std::move(value);
                exprsMap.emplace(key, std::move(exprEntity));
            }
        }
    }
    return true;
}

std::pair<suez::turing::VariableType, bool>
ExprUtil::transSqlTypeToVariableType(const string &sqlTypeStr) {
    static std::unordered_map<std::string, suez::turing::VariableType> const singleValueTypes
        = {{"CONDITION_BOOLEAN", vt_bool},
           {"BOOLEAN", vt_bool},
           {"TINYINT", vt_int8},
           {"SMALLINT", vt_int16},
           {"INTEGER", vt_int32},
           {"BIGINT", vt_int64},
           {"DECIMAL", vt_float},
           {"FLOAT", vt_float},
           {"REAL", vt_float},
           {"DOUBLE", vt_double},
           {"VARCHAR", vt_string},
           {"CHAR", vt_string}};

    static std::unordered_map<std::string, suez::turing::VariableType> const multiValueTypes
        = {{"ARRAY(CONDITION_BOOLEAN)", vt_bool},
           {"ARRAY(BOOLEAN)", vt_bool},
           {"ARRAY(TINYINT)", vt_int8},
           {"ARRAY(SMALLINT)", vt_int16},
           {"ARRAY(INTEGER)", vt_int32},
           {"ARRAY(BIGINT)", vt_int64},
           {"ARRAY(DECIMAL)", vt_float},
           {"ARRAY(FLOAT)", vt_float},
           {"ARRAY(REAL)", vt_float},
           {"ARRAY(DOUBLE)", vt_double},
           {"ARRAY(VARCHAR)", vt_string},
           {"ARRAY(CHAR)", vt_string}};

    auto iter = singleValueTypes.find(sqlTypeStr);
    if (iter != singleValueTypes.end()) {
        return pair(iter->second, false);
    }
    auto iter2 = multiValueTypes.find(sqlTypeStr);
    if (iter2 != multiValueTypes.end()) {
        return pair(iter2->second, true);
    }

    return pair(vt_unknown, false);
}

std::string ExprUtil::transVariableTypeToSqlType(suez::turing::VariableType vt) {
    static std::map<suez::turing::VariableType, std::string> const table = {{vt_bool, "BOOLEAN"},
                                                                            {vt_int8, "TINYINT"},
                                                                            {vt_int16, "SMALLINT"},
                                                                            {vt_int32, "INTEGER"},
                                                                            {vt_int64, "BIGINT"},
                                                                            {vt_float, "FLOAT"},
                                                                            {vt_double, "DOUBLE"},
                                                                            {vt_string, "VARCHAR"}};
    auto iter = table.find(vt);
    if (iter != table.end()) {
        return iter->second;
    } else {
        return "";
    }
}

bool ExprUtil::isUdf(const SimpleValue &simpleVal) {
    if (!simpleVal.IsObject()) {
        return false;
    }
    if (!simpleVal.HasMember(SQL_CONDITION_OPERATOR)) {
        return false;
    }
    if (!simpleVal.HasMember(SQL_CONDITION_PARAMETER)) {
        return false;
    }
    if (!simpleVal.HasMember(SQL_CONDITION_TYPE)) {
        return false;
    }
    const SimpleValue &param = simpleVal[SQL_CONDITION_PARAMETER];
    if (!param.IsArray()) {
        return false;
    }
    const SimpleValue &type = simpleVal[SQL_CONDITION_TYPE];
    if (!type.IsString()) {
        return false;
    }
    string categroy(type.GetString());
    return categroy == SQL_UDF_OP;
}

bool ExprUtil::isSameUdf(const SimpleValue &simpleVal, const std::string &udfType) {
    if (!isUdf(simpleVal)) {
        return false;
    }
    string op(simpleVal[SQL_CONDITION_OPERATOR].GetString());
    return op == udfType;
}

bool ExprUtil::isInOp(const SimpleValue &simpleVal) {
    if (!simpleVal.IsObject()) {
        return false;
    }
    if (!simpleVal.HasMember(SQL_CONDITION_OPERATOR)) {
        return false;
    }
    if (string(simpleVal[SQL_CONDITION_OPERATOR].GetString()) != SQL_IN_OP) {
        return false;
    }
    if (!simpleVal.HasMember(SQL_CONDITION_PARAMETER)) {
        return false;
    }
    const SimpleValue &param = simpleVal[SQL_CONDITION_PARAMETER];
    if (!param.IsArray()) {
        return false;
    }
    return true;
}

bool ExprUtil::isCaseOp(const autil::SimpleValue &simpleVal) {
    if (simpleVal.IsObject() && simpleVal.HasMember(SQL_CONDITION_OPERATOR)) {
        string op(simpleVal[SQL_CONDITION_OPERATOR].GetString());
        return op == SQL_CASE_OP;
    } else {
        return false;
    }
}

bool ExprUtil::createExprString(const autil::SimpleValue &simpleVal, std::string &exprStr) {
    std::unordered_map<std::string, std::string> renameMap;
    ExprGenerateVisitor visitor(&renameMap);
    visitor.visit(simpleVal);
    if (visitor.isError()) {
        SQL_LOG(WARN, "to expr string %s", visitor.errorInfo().c_str());
        return false;
    }
    exprStr = visitor.getExpr();
    return true;
}

string ExprUtil::toExprString(const SimpleValue &simpleVal,
                              bool &hasError,
                              string &errorInfo,
                              unordered_map<string, string> &renameMap) {
    ExprGenerateVisitor visitor(&renameMap);
    visitor.visit(simpleVal);
    hasError = visitor.isError();
    if (hasError) {
        errorInfo = visitor.errorInfo();
        SQL_LOG(WARN, "to expr string %s", errorInfo.c_str());
        return "";
    }
    return visitor.getExpr();
}

void ExprUtil::convertColumnName(string &columnName) {
    static const string replaceStr = "$()+-*/,|^&@><='[]!`";
    for (size_t i = 0; i < replaceStr.size(); i++) {
        if (columnName.find(replaceStr[i]) != std::string::npos) {
            char buf[64];
            snprintf(buf, 64, "tmp_%lx", std::hash<std::string> {}(columnName));
            SQL_LOG(TRACE3, "rename column name[%s] with[%s]", columnName.c_str(), buf);
            columnName = buf;
            break;
        }
    }
}

bool ExprUtil::unCast(const autil::SimpleValue **simpleValueWrapper) {
    auto &simpleValue = **simpleValueWrapper;
    bool isCast = ExprUtil::isSameUdf(simpleValue, SQL_UDF_CAST_OP);
    if (isCast) {
        if (!simpleValue.IsObject() || !simpleValue.HasMember(SQL_CONDITION_PARAMETER)) {
            return false;
        }
        const SimpleValue &param = simpleValue[SQL_CONDITION_PARAMETER];
        if (!param.IsArray() || param.Size() != 1) {
            return false;
        }
        *simpleValueWrapper = &param[0];
    }
    return true;
}

static std::map<std::string, std::string> reverseOpMap = {
    {SQL_EQUAL_OP, SQL_EQUAL_OP},
    {SQL_LIKE_OP, SQL_LIKE_OP},
    {SQL_NOT_EQUAL_OP, SQL_NOT_EQUAL_OP},
    {SQL_GT_OP, SQL_LT_OP},
    {SQL_GE_OP, SQL_LE_OP},
    {SQL_LT_OP, SQL_GT_OP},
    {SQL_LE_OP, SQL_GE_OP},
};

bool ExprUtil::reverseOp(std::string &op) {
    auto iter = reverseOpMap.find(op);
    if (iter != reverseOpMap.end()) {
        op = iter->second;
        return true;
    }
    return false;
}

bool ExprUtil::isItem(const SimpleValue &item) {
    if (!item.IsObject() || !item.HasMember(SQL_CONDITION_OPERATOR)
        || !item.HasMember(SQL_CONDITION_PARAMETER)) {
        return false;
    }
    const SimpleValue &op = item[SQL_CONDITION_OPERATOR];
    if (!op.IsString() || op.GetString() != string(SQL_ITEM_OP)) {
        return false;
    }
    const SimpleValue &param = item[SQL_CONDITION_PARAMETER];
    if (!param.IsArray() || param.Size() != 2) {
        return false;
    }
    return SqlJsonUtil::isColumn(param[0]) && param[1].IsString();
}

bool ExprUtil::parseItemVariable(const SimpleValue &item, string &itemName, string &itemKey) {
    if (!isItem(item)) {
        return false;
    }
    const SimpleValue &param = item[SQL_CONDITION_PARAMETER];
    itemName = param[0].GetString();
    itemKey = param[1].GetString();
    return true;
}

string ExprUtil::getItemFullName(const string &itemName, const string &itemKey) {
    string columnName = itemName + "['" + itemKey + "']";
    return columnName;
}

bool ExprUtil::parseExprsJson(const std::string &exprsJson, autil::SimpleDocument &simpleDoc) {
    simpleDoc.Parse(exprsJson.c_str());
    if (simpleDoc.HasParseError()) {
        SQL_LOG(WARN,
                "parse output exprs error, error [%s (%lu)], exprsJson[%s]",
                rapidjson::GetParseError_En(simpleDoc.GetParseError()),
                simpleDoc.GetErrorOffset(),
                exprsJson.c_str());
        return false;
    }
    if (!simpleDoc.IsObject()) {
        SQL_LOG(WARN, "parsed document must be object, exprsJson[%s]", exprsJson.c_str());
        return false;
    }
    return true;
}

bool ExprUtil::isBoolExpr(const autil::SimpleValue &simpleVal) {
    if (!simpleVal.IsBool()) {
        return false;
    }
    return true;
}

} // namespace sql
