#include <ha3/sql/ops/condition/ExprGenerateVisitor.h>
#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/util/SqlJsonUtil.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/ops/condition/ExprUtil.h>
#include <autil/ConstString.h>
#include <autil/legacy/RapidJsonHelper.h>

using namespace std;
using namespace autil_rapidjson;
using namespace autil;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(search, ExprGenerateVisitor);

ExprGenerateVisitor::ExprGenerateVisitor() {
}

ExprGenerateVisitor::~ExprGenerateVisitor() {
}

#define VISIT_AND_CHECK(value)                  \
    visit((value));                             \
    if (isError()) {                            \
        return;                                 \
    }

void ExprGenerateVisitor::visitCastUdf(const autil_rapidjson::SimpleValue &value) {
    const SimpleValue &param = value[SQL_CONDITION_PARAMETER];
    VISIT_AND_CHECK(param[0]);
}

void ExprGenerateVisitor::visitMultiCastUdf(const autil_rapidjson::SimpleValue &value) {
    const SimpleValue &param = value[SQL_CONDITION_PARAMETER];
    VISIT_AND_CHECK(param[0]);
}

void ExprGenerateVisitor::visitNormalUdf(const autil_rapidjson::SimpleValue &value) {
    string functionName(value[SQL_CONDITION_OPERATOR].GetString());
    const SimpleValue &param = value[SQL_CONDITION_PARAMETER];
    if (functionName == SQL_UDF_HA_IN_OP) {
        functionName = "in";
    }
    string tmpExpr = functionName + "(";
    for (size_t i = 0; i < param.Size(); i++) {
        tmpExpr += i != 0 ? "," : "";
        VISIT_AND_CHECK(param[i]);
        tmpExpr += _expr;
    }
    _expr = tmpExpr + ")";
}

void ExprGenerateVisitor::parseInOrNotIn(
        const autil_rapidjson::SimpleValue &value,
        const std::string &op)
{
    const SimpleValue &param = value[SQL_CONDITION_PARAMETER];
    string tmpExpr = op + "(";
    VISIT_AND_CHECK(param[0]);
    tmpExpr += _expr + ",\"";
    for (size_t i = 1; i < param.Size(); i++) {
        tmpExpr += i != 1 ? "|" : "";
        if (param[i].IsString()) {
            tmpExpr += string(param[i].GetString());
        } else {
            VISIT_AND_CHECK(param[i]);
            tmpExpr += _expr;
        }
    }
    _expr = tmpExpr + "\")";
}

void ExprGenerateVisitor::visitInOp(const autil_rapidjson::SimpleValue &value) {
    parseInOrNotIn(value, "in");
}

void ExprGenerateVisitor::visitNotInOp(const autil_rapidjson::SimpleValue &value) {
    parseInOrNotIn(value, "notin");
}

void ExprGenerateVisitor::visitCaseOp(const autil_rapidjson::SimpleValue &value) {
    const SimpleValue &param = value[SQL_CONDITION_PARAMETER];
    string tmpExpr = "case(";
    for (size_t i = 0; i < param.Size(); i++) {
        tmpExpr += i != 0 ? "," : "";
        if (param[i].IsString()) {
            tmpExpr += string(param[i].GetString());
        } else {
            VISIT_AND_CHECK(param[i]);
            tmpExpr += _expr;
        }
    }
    _expr = tmpExpr + "\")";
}

void ExprGenerateVisitor::visitItemOp(const autil_rapidjson::SimpleValue &value) {
    const SimpleValue &param = value[SQL_CONDITION_PARAMETER];
    string fullName = ExprUtil::getItemFullName(param[0].GetString(), param[1].GetString());
    KernelUtil::stripName(fullName);
    string newFullName = fullName;
    ExprUtil::convertColumnName(newFullName);
    assert(newFullName != fullName);
    auto iter = _renameMap.find(newFullName);
    if (iter != _renameMap.end()) {
        if (iter->second != fullName) {
            setErrorInfo("rename map conflict: [%s] vs [%s]",
                         iter->second.c_str(), fullName.c_str());
            return;
        }
    }
    _renameMap.insert(make_pair(newFullName, fullName));
    _expr = newFullName;
}

void ExprGenerateVisitor::visitOtherOp(const autil_rapidjson::SimpleValue &value) {
    string tmpExpr = "(";
    string op(value[SQL_CONDITION_OPERATOR].GetString());
    if (op == SQL_NOT_EQUAL_OP) {
        op = HA3_NOT_EQUAL_OP;
    }
    const SimpleValue &param = value[SQL_CONDITION_PARAMETER];
    for (size_t i = 0; i < param.Size(); i++) {
        tmpExpr += i != 0 ? op : "";
        VISIT_AND_CHECK(param[i]);
        tmpExpr += _expr;
    }
    _expr = tmpExpr + ")";
}

void ExprGenerateVisitor::visitInt64(const autil_rapidjson::SimpleValue &value) {
    _expr = to_string(value.GetInt64());
}

void ExprGenerateVisitor::visitDouble(const autil_rapidjson::SimpleValue &value) {
    _expr = to_string((float) value.GetDouble());
}

void ExprGenerateVisitor::visitBool(const autil_rapidjson::SimpleValue &value) {
    _expr = value.GetBool() ? "1" : "0";
}

void ExprGenerateVisitor::visitColumn(const autil_rapidjson::SimpleValue &value) {
    string name = SqlJsonUtil::getColumnName(value);
    string newName = name;
    ExprUtil::convertColumnName(newName);
    if (newName != name) {
        auto iter = _renameMap.find(newName);
        if (iter != _renameMap.end()) {
            if (iter->second != name) {
                setErrorInfo("rename map conflict: [%s] vs [%s]",
                        iter->second.c_str(), name.c_str());
                return;
            }
        }
        _renameMap.insert(make_pair(newName, name));
    }
    _expr = newName;
}

void ExprGenerateVisitor::visitString(const autil_rapidjson::SimpleValue &value) {
    _expr =  "\"" + string(value.GetString()) + "\"";
}

END_HA3_NAMESPACE(sql);
