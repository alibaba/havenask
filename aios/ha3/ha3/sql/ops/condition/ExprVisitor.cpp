#include <ha3/sql/ops/condition/ExprVisitor.h>
#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/condition/ExprUtil.h>
#include <ha3/sql/ops/util/SqlJsonUtil.h>
#include <autil/legacy/RapidJsonHelper.h>

using namespace std;
using namespace autil_rapidjson;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, ExprVisitor);

ExprVisitor::ExprVisitor()
    : _hasError(false)
{
}

ExprVisitor::~ExprVisitor() {
}

void ExprVisitor::setErrorInfo(const char* fmt, ...)
{
    const size_t maxMessageLength = 1024;
    char buffer[maxMessageLength];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buffer, maxMessageLength, fmt, ap);
    va_end(ap);

    assert(!_hasError);
    _hasError = true;
    _errorInfo = buffer;
}

void ExprVisitor::visit(const SimpleValue &value)
{
    assert(!isError());
    if (value.IsObject() && value.HasMember(SQL_CONDITION_OPERATOR)
        && value.HasMember(SQL_CONDITION_PARAMETER))
    {
        if (ExprUtil::isUdf(value)) {
            if (ExprUtil::isCastUdf(value)) {
                visitCastUdf(value);
            } else if (ExprUtil::isMultiCastUdf(value)) {
                visitMultiCastUdf(value);
            } else {
                visitNormalUdf(value);
            }
        } else {
            string op(value[SQL_CONDITION_OPERATOR].GetString());
            const SimpleValue &param = value[SQL_CONDITION_PARAMETER];
            if (!param.IsArray()) {
                setErrorInfo("invalid syntax expression, not json array");
                return;
            }
            if (op == SQL_IN_OP) {
                if (param.Size() < 2) {
                    setErrorInfo("in op param size must >= 2");
                    return;
                }
                visitInOp(value);
            } else if (op == SQL_NOT_IN_OP) {
                if (param.Size() < 2) {
                    setErrorInfo("not_in op param size must >= 2");
                    return;
                }
                visitNotInOp(value);
            } else if (op == SQL_CASE_OP) {
                if (param.Size() < 3) {
                    setErrorInfo("case op param size must >= 3");
                    return;
                }
                visitCaseOp(value);
            } else if (op == SQL_ITEM_OP) {
                if (!ExprUtil::isItem(value)) {
                    setErrorInfo("invalid ITEM op format: %s",
                            RapidJsonHelper::SimpleValue2Str(value).c_str());
                    return;
                }
                visitItemOp(value);
            } else {
                visitOtherOp(value);
            }
        }
    } else if (value.IsInt64()) {
        visitInt64(value);
    } else if (value.IsDouble()) {
        visitDouble(value);
    } else if (value.IsBool()) {
        visitBool(value);
    } else if (value.IsString()) {
        if (SqlJsonUtil::isColumn(value)) {
            visitColumn(value);
        } else {
            visitString(value);
        }
    } else if (value.IsNull()) {
        // do nothing
    } else {
        setErrorInfo("not supported: %s", RapidJsonHelper::SimpleValue2Str(value).c_str());
        return;
    }
}

void ExprVisitor::visitCastUdf(const autil_rapidjson::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitMultiCastUdf(const autil_rapidjson::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitNormalUdf(const autil_rapidjson::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitInOp(const autil_rapidjson::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitNotInOp(const autil_rapidjson::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitCaseOp(const autil_rapidjson::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitItemOp(const autil_rapidjson::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitOtherOp(const autil_rapidjson::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitInt64(const autil_rapidjson::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitDouble(const autil_rapidjson::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitBool(const autil_rapidjson::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitColumn(const autil_rapidjson::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitString(const autil_rapidjson::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

END_HA3_NAMESPACE(sql);
