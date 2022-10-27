#include <ha3/sql/ops/condition/ExprUtil.h>
#include <autil/legacy/any.h>
#include <autil/StringUtil.h>
#include <autil/MultiValueCreator.h>
#include <ha3/sql/ops/condition/ConstExpression.h>
#include <matchdoc/MatchDocAllocator.h>
#include <ha3/sql/ops/util/SqlJsonUtil.h>
#include <ha3/sql/ops/condition/ConditionVisitor.h>
#include <ha3/sql/ops/condition/ExprGenerateVisitor.h>
#include <autil/legacy/RapidJsonHelper.h>
using namespace std;
using namespace matchdoc;
using namespace autil;
using namespace autil_rapidjson;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, ExprUtil);

AttributeExpression *
ExprUtil::createConstExpression(autil::mem_pool::Pool *pool,
                                const AtomicSyntaxExpr *atomicSyntaxExpr,
                                suez::turing::VariableType type) {
    return createConstExpression(pool,
                                 atomicSyntaxExpr->getValueString(),
                                 atomicSyntaxExpr->getExprString(),
                                 type);
}

AttributeExpression *
ExprUtil::createConstExpression(autil::mem_pool::Pool *pool,
                                const std::string &constValue,
                                const std::string &exprStr,
                                suez::turing::VariableType type) {
    AttributeExpression *ret = NULL;

#define CREATE_CONST_ATTR_EXPR_HELPER(vt)                               \
    case vt:                                                            \
        {                                                               \
            typedef VariableTypeTraits<vt, false>::AttrExprType T;      \
            T exprValue;                                                \
            if (StringUtil::numberFromString<T>(constValue, exprValue)) \
            {                                                           \
                ret = POOL_NEW_CLASS(pool, ConstExpression<T>, exprValue); \
            } else {                                                    \
                SQL_LOG(WARN, "createConstAttrExpr for number failed, value: %s", \
                        constValue.c_str());                            \
            }                                                           \
            break;                                                      \
        }

    switch (type) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL(CREATE_CONST_ATTR_EXPR_HELPER);
    case vt_string: {
        typedef ConstExpression<MultiChar> ConstStringExpression;
        MultiChar mc;
        mc.init(MultiValueCreator::createMultiValueBuffer(constValue.data(),
                        constValue.size(), pool));
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


bool ExprUtil::parseOutputExprs(autil::mem_pool::Pool *pool, const string &exprsJson,
                                map<string, ExprEntity> &exprsMap)
{
    std::map<std::string, std::string> renameMap;
    return parseOutputExprs(pool, exprsJson, exprsMap, renameMap);
}

bool ExprUtil::parseOutputExprs(autil::mem_pool::Pool *pool,
                      const std::string &exprsJson,
                      std::map<std::string, ExprEntity> &exprsMap,
                      std::map<std::string, std::string> &renameMap)
{
    if (exprsJson.empty()) {
        return true;
    }
    AutilPoolAllocator allocator(pool);
    SimpleDocument simpleDoc(&allocator);
    if (unlikely(!ExprUtil::parseExprsJson(exprsJson, simpleDoc)))
    {
        return false;
    }
    for (SimpleValue::ConstMemberIterator itr = simpleDoc.MemberBegin();
         itr != simpleDoc.MemberEnd(); ++itr) {
        string key = SqlJsonUtil::isColumn(itr->name) ?
            SqlJsonUtil::getColumnName(itr->name) : itr->name.GetString();
        ExprEntity exprEntity;
        bool hasError = false;
        string errorInfo;
        if (ExprUtil::isCaseOp(itr->value))
        {
            exprEntity.exprJson = RapidJsonHelper::SimpleValue2Str(itr->value);
            exprsMap[key] = exprEntity;
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
                exprEntity.exprStr = value;
                exprsMap[key] = exprEntity;
            }
        }
    }
    return true;
}



suez::turing::VariableType ExprUtil::transSqlTypeToVariableType(const string &sqlTypeStr) {
    static std::unordered_map<std::string, suez::turing::VariableType> const table = {
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
        {"CHAR", vt_string}
    };
    auto iter = table.find(sqlTypeStr);
    if (iter != table.end()) {
        return iter->second;
    } else {
        return vt_unknown;
    }
}

std::string ExprUtil::transVariableTypeToSqlType(suez::turing::VariableType vt) {
    static std::map<suez::turing::VariableType, std::string> const table = {
        {vt_bool, "BOOLEAN"},
        {vt_int8, "TINYINT"},
        {vt_int16, "SMALLINT"},
        {vt_int32, "INTEGER"},
        {vt_int64, "BIGINT"},
        {vt_float, "FLOAT"},
        {vt_double, "DOUBLE"},
        {vt_string, "VARCHAR"}
    };
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

bool ExprUtil::isCastUdf(const SimpleValue &simpleVal) {
    if (!isUdf(simpleVal)) {
        return false;
    }
    string op(simpleVal[SQL_CONDITION_OPERATOR].GetString());
    return op == SQL_UDF_CAST_OP ;
}

bool ExprUtil::isContainUdf(const SimpleValue &simpleVal) {
    if (!isUdf(simpleVal)) {
        return false;
    }
    string op(simpleVal[SQL_CONDITION_OPERATOR].GetString());
    return op == SQL_UDF_CONTAIN_OP ;
}

bool ExprUtil::isHaInUdf(const SimpleValue &simpleVal) {
    if (!isUdf(simpleVal)) {
        return false;
    }
    string op(simpleVal[SQL_CONDITION_OPERATOR].GetString());
    return op == SQL_UDF_HA_IN_OP ;
}

bool ExprUtil::isMultiCastUdf(const SimpleValue &simpleVal) {
    if (!isUdf(simpleVal)) {
        return false;
    }
    string op(simpleVal[SQL_CONDITION_OPERATOR].GetString());
    return op == SQL_UDF_MULTI_CAST_OP ;
}

bool ExprUtil::isAnyUdf(const SimpleValue &simpleVal) {
    if (!isUdf(simpleVal)) {
        return false;
    }
    string op(simpleVal[SQL_CONDITION_OPERATOR].GetString());
    return op == SQL_UDF_ANY_OP ;
}

bool ExprUtil::isCaseOp(const autil_rapidjson::SimpleValue &simpleVal)
{
    if (simpleVal.IsObject() && simpleVal.HasMember(SQL_CONDITION_OPERATOR)){
        string op(simpleVal[SQL_CONDITION_OPERATOR].GetString());
        return op == SQL_CASE_OP;
    } else {
        return false;
    }
}

string ExprUtil::toExprString(const SimpleValue &simpleVal,
                              bool &hasError, string &errorInfo,
                              map<string, string> &renameMap)
{
    ExprGenerateVisitor visitor;
    visitor.visit(simpleVal);
    hasError = visitor.isError();
    if (hasError) {
        errorInfo = visitor.errorInfo();
        SQL_LOG(WARN, "to expr string %s", errorInfo.c_str());
        return "";
    }
    renameMap.insert(visitor.getRenameMap().begin(), visitor.getRenameMap().end());
    return visitor.getExpr();
}

void ExprUtil::convertColumnName(string &columnName) {
    string replaceStr = "$()+-*/,|^&@><=.'[]";
    for (size_t i = 0; i < replaceStr.size(); i++) {
        std::replace(columnName.begin(), columnName.end(), replaceStr[i], '_');
    }
}


bool ExprUtil::unCast(const autil_rapidjson::SimpleValue **simpleValueWrapper) {
    auto &simpleValue = **simpleValueWrapper;
    bool isCast = ExprUtil::isCastUdf(simpleValue);
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

bool ExprUtil::isItem(const SimpleValue &item)
{
    if (!item.IsObject() || !item.HasMember(SQL_CONDITION_OPERATOR)
        || !item.HasMember(SQL_CONDITION_PARAMETER))
    {
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

bool ExprUtil::parseItemVariable(const SimpleValue &item,
                                 string &itemName,
                                 string &itemKey)
{
    if (!isItem(item)) {
        return false;
    }
    const SimpleValue &param = item[SQL_CONDITION_PARAMETER];
    itemName = param[0].GetString();
    itemKey = param[1].GetString();
    return true;
}

string ExprUtil::getItemFullName(const string &itemName,
                                 const string &itemKey)
{
    string columnName = itemName + "['" + itemKey + "']";
    return columnName;
}

bool ExprUtil::parseExprsJson(const std::string& exprsJson,
                              autil_rapidjson::SimpleDocument &simpleDoc)
{
    simpleDoc.Parse(exprsJson.c_str());
    if (simpleDoc.HasParseError()) {
        SQL_LOG(WARN, "parse output exprs error, jsonStr [%s]", exprsJson.c_str());
        return false;
    }
    if (!simpleDoc.IsObject()) {
        return false;
    }
    return true;
}

bool ExprUtil::isBoolExpr(const autil_rapidjson::SimpleValue &simpleVal) {
    if (!simpleVal.IsBool()) {
        return false;
    }
    return true;
}

END_HA3_NAMESPACE(sql);
