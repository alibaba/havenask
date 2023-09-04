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
#include "sql/ops/condition/ExprGenerateVisitor.h"

#include <assert.h>
#include <cstddef>
#include <utility>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "rapidjson/document.h"
#include "sql/common/common.h"
#include "sql/ops/condition/ExprUtil.h"
#include "sql/ops/condition/SqlJsonUtil.h"

using namespace std;
using namespace autil;

namespace sql {

static const std::string FIELD_PREFIX = string(1, SQL_COLUMN_PREFIX);

ExprGenerateVisitor::ExprGenerateVisitor(VisitorMap *renameMap)
    : _renameMapPtr(renameMap) {}

ExprGenerateVisitor::~ExprGenerateVisitor() {}

#define VISIT_AND_CHECK(value)                                                                     \
    visit((value));                                                                                \
    if (isError()) {                                                                               \
        return;                                                                                    \
    }

void ExprGenerateVisitor::visitCastUdf(const autil::SimpleValue &value) {
    const SimpleValue &param = value[SQL_CONDITION_PARAMETER];
    VISIT_AND_CHECK(param[0]);
}

void ExprGenerateVisitor::visitMultiCastUdf(const autil::SimpleValue &value) {
    const SimpleValue &param = value[SQL_CONDITION_PARAMETER];
    VISIT_AND_CHECK(param[0]);
}

void ExprGenerateVisitor::visitNormalUdf(const autil::SimpleValue &value) {
    string functionName(value[SQL_CONDITION_OPERATOR].GetString());
    const SimpleValue &param = value[SQL_CONDITION_PARAMETER];
    string tmpExpr = functionName + "(";
    for (size_t i = 0; i < param.Size(); i++) {
        tmpExpr += i != 0 ? "," : "";
        VISIT_AND_CHECK(param[i]);
        tmpExpr += _expr;
    }
    _expr = tmpExpr + ")";
}

void ExprGenerateVisitor::parseInOrNotIn(const autil::SimpleValue &value, const std::string &op) {
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

void ExprGenerateVisitor::visitInOp(const autil::SimpleValue &value) {
    parseInOrNotIn(value, "in");
}

void ExprGenerateVisitor::visitNotInOp(const autil::SimpleValue &value) {
    parseInOrNotIn(value, "notin");
}

void ExprGenerateVisitor::visitCaseOp(const autil::SimpleValue &value) {
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

void ExprGenerateVisitor::visitItemOp(const autil::SimpleValue &value) {
    const SimpleValue &param = value[SQL_CONDITION_PARAMETER];
    string fullName = ExprUtil::getItemFullName(param[0].GetString(), param[1].GetString());
    if (autil::StringUtil::startsWith(fullName, FIELD_PREFIX)) {
        fullName = fullName.substr(1);
    }
    string newFullName = fullName;
    ExprUtil::convertColumnName(newFullName);
    assert(newFullName != fullName);
    auto iter = _renameMapPtr->find(newFullName);
    if (iter != _renameMapPtr->end()) {
        if (iter->second != fullName) {
            setErrorInfo("rename map key[%s] conflict: [%s] vs [%s]",
                         newFullName.c_str(),
                         iter->second.c_str(),
                         fullName.c_str());
            return;
        }
    }
    _renameMapPtr->insert(make_pair(newFullName, fullName));
    _expr = newFullName;
}

void ExprGenerateVisitor::visitOtherOp(const autil::SimpleValue &value) {
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

void ExprGenerateVisitor::visitInt64(const autil::SimpleValue &value) {
    _expr = to_string(value.GetInt64());
}

void ExprGenerateVisitor::visitDouble(const autil::SimpleValue &value) {
    _expr = to_string((float)value.GetDouble());
}

void ExprGenerateVisitor::visitBool(const autil::SimpleValue &value) {
    _expr = value.GetBool() ? "1" : "0";
}

void ExprGenerateVisitor::visitColumn(const autil::SimpleValue &value) {
    string name = SqlJsonUtil::getColumnName(value);
    string newName = name;
    ExprUtil::convertColumnName(newName);
    if (newName != name) {
        auto iter = _renameMapPtr->find(newName);
        if (iter != _renameMapPtr->end()) {
            if (iter->second != name) {
                setErrorInfo("rename map key[%s] conflict: [%s] vs [%s]",
                             newName.c_str(),
                             iter->second.c_str(),
                             name.c_str());
                return;
            }
        }
        _renameMapPtr->insert(make_pair(newName, name));
    }
    _expr = newName;
}

void ExprGenerateVisitor::visitString(const autil::SimpleValue &value) {
    _expr = "\"" + string(value.GetString()) + "\"";
}

} // namespace sql
