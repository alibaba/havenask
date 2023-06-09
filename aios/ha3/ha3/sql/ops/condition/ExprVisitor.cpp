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
#include "ha3/sql/ops/condition/ExprVisitor.h"

#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <iosfwd>

#include "autil/TimeUtility.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "autil/legacy/RapidJsonHelper.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/condition/ExprUtil.h"
#include "ha3/sql/ops/condition/SqlJsonUtil.h"
#include "rapidjson/document.h"

using namespace std;
using namespace autil;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, ExprVisitor);

ExprVisitor::ExprVisitor()
    : _hasError(false) {}

ExprVisitor::~ExprVisitor() {}

void ExprVisitor::setErrorInfo(const char *fmt, ...) {
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

void ExprVisitor::visit(const SimpleValue &value) {
    assert(!isError());
    if (value.IsObject() && value.HasMember(SQL_CONDITION_OPERATOR)
        && value.HasMember(SQL_CONDITION_PARAMETER)) {
        if (ExprUtil::isUdf(value)) {
            if (ExprUtil::isSameUdf(value, SQL_UDF_CAST_OP)) {
                visitCastUdf(value);
            } else if (ExprUtil::isSameUdf(value, SQL_UDF_MULTI_CAST_OP)) {
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

void ExprVisitor::visitCastUdf(const autil::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitMultiCastUdf(const autil::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitNormalUdf(const autil::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitInOp(const autil::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitNotInOp(const autil::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitCaseOp(const autil::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitItemOp(const autil::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitOtherOp(const autil::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitInt64(const autil::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitDouble(const autil::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitBool(const autil::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitColumn(const autil::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

void ExprVisitor::visitString(const autil::SimpleValue &value) {
    setErrorInfo("`%s` is not implemented", __FUNCTION__);
}

} // namespace sql
} // namespace isearch
