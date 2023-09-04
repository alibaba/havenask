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
#include "sql/ops/scan/PrimaryKeyScanConditionVisitor.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>
#include <utility>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/RapidJsonHelper.h"
#include "rapidjson/document.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/condition/ExprUtil.h"
#include "sql/ops/condition/SqlJsonUtil.h"

using namespace std;
using namespace autil;

namespace sql {

PrimaryKeyScanConditionVisitor::PrimaryKeyScanConditionVisitor(const std::string &keyFieldName,
                                                               const std::string &keyIndexName)
    : _keyFieldName(keyFieldName)
    , _keyIndexName(keyIndexName)
    , _hasQuery(false)
    , _needFilter(false) {}

PrimaryKeyScanConditionVisitor::~PrimaryKeyScanConditionVisitor() {}

void PrimaryKeyScanConditionVisitor::visitAndCondition(AndCondition *condition) {
    if (isError()) {
        return;
    }
    const vector<ConditionPtr> &children = condition->getChildCondition();
    bool hasQuery = false;
    for (size_t i = 0; i < children.size(); i++) {
        children[i]->accept(this);
        if (stealHasQuery()) {
            hasQuery = true;
        }
    }
    _hasQuery = hasQuery;
}

void PrimaryKeyScanConditionVisitor::visitOrCondition(OrCondition *condition) {
    if (isError()) {
        return;
    }
    const vector<ConditionPtr> &children = condition->getChildCondition();
    bool hasQuery = true;
    for (size_t i = 0; i < children.size(); i++) {
        children[i]->accept(this);
        if (!stealHasQuery()) {
            hasQuery = false;
        }
    }
    _hasQuery = hasQuery;
}

void PrimaryKeyScanConditionVisitor::visitNotCondition(NotCondition *condition) {
    if (isError()) {
        return;
    }
    const vector<ConditionPtr> &children = condition->getChildCondition();
    assert(children.size() == 1);
    children[0]->accept(this);
    _hasQuery = false;
}

void PrimaryKeyScanConditionVisitor::visitLeafCondition(LeafCondition *condition) {
    if (isError()) {
        return;
    }
    const SimpleValue &leafCondition = condition->getCondition();
    if (ExprUtil::isUdf(leafCondition)) {
        // get PK in UDF for fetch document
        // UDF white list: contain, notcontain, ha_in, not_ha_in
        // do not get PK from notcontain, not_ha_in
        if (ExprUtil::isSameUdf(leafCondition, SQL_UDF_CONTAIN_OP)
            || ExprUtil::isSameUdf(leafCondition, SQL_UDF_NOT_CONTAIN_OP)
            || ExprUtil::isSameUdf(leafCondition, SQL_UDF_HA_IN_OP)
            || ExprUtil::isSameUdf(leafCondition, SQL_UDF_NOT_HA_IN_OP)) {
            _hasQuery = parseUdf(leafCondition);
            if (_hasQuery) {
                SQL_LOG(DEBUG,
                        "convert udf [%s] to query",
                        RapidJsonHelper::SimpleValue2Str(leafCondition).c_str());
            }
        } else {
            setErrorInfo("This UDF is not supported: %s",
                         RapidJsonHelper::SimpleValue2Str(leafCondition).c_str());
            return;
        }
    } else {
        _hasQuery = parseKey(leafCondition);
    }
    if (!_hasQuery && !_needFilter) {
        _needFilter = true;
    }
}

bool PrimaryKeyScanConditionVisitor::parseUdf(const SimpleValue &condition) {
    if (!condition.IsObject() || !condition.HasMember(SQL_CONDITION_OPERATOR)
        || !condition.HasMember(SQL_CONDITION_PARAMETER)) {
        return false;
    }
    string op(condition[SQL_CONDITION_OPERATOR].GetString());
    if (op != SQL_UDF_CONTAIN_OP && op != SQL_UDF_HA_IN_OP) {
        return false;
    }
    const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
    if ((!param.IsArray()) || param.Size() > 3 || param.Size() < 2) {
        return false;
    }
    string columnName;
    if (SqlJsonUtil::isColumn(param[0])) {
        columnName = SqlJsonUtil::getColumnName(param[0]);
    } else {
        columnName = param[0].GetString();
    }
    if (columnName != _keyFieldName && columnName != _keyIndexName) {
        return false;
    }
    const string &rawKeyStr = param[1].GetString();
    string sepStr = "|";
    if (param.Size() == 3) {
        sepStr = param[2].GetString();
    }
    const vector<string> &rawKeys = StringUtil::split(rawKeyStr, sepStr, true);
    if (rawKeys.empty()) {
        return true;
    }
    for (const auto &key : rawKeys) {
        auto pair = _rawKeySet.insert(key);
        if (pair.second) {
            _rawKeyVec.emplace_back(key);
        }
    }
    return true;
}

bool PrimaryKeyScanConditionVisitor::parseIn(const SimpleValue &condition) {
    if (!condition.IsObject() || !condition.HasMember(SQL_CONDITION_OPERATOR)
        || !condition.HasMember(SQL_CONDITION_PARAMETER)) {
        return false;
    }
    string op(condition[SQL_CONDITION_OPERATOR].GetString());
    if (op != SQL_IN_OP) {
        return false;
    }
    const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
    if ((!param.IsArray()) || param.Size() < 2) {
        return false;
    }
    if (!SqlJsonUtil::isColumn(param[0])) {
        return false;
    }
    string columnName = SqlJsonUtil::getColumnName(param[0]);
    if (columnName != _keyFieldName) {
        return false;
    }
    for (size_t i = 1; i < param.Size(); ++i) {
        if (!parseEqual(param[0], param[i])) {
            SQL_LOG(ERROR, "parseEqual failed, index [%ld]", i);
            return false;
        }
    }
    return true;
}

bool PrimaryKeyScanConditionVisitor::parseKey(const SimpleValue &condition) {
    if (!condition.IsObject() || !condition.HasMember(SQL_CONDITION_OPERATOR)
        || !condition.HasMember(SQL_CONDITION_PARAMETER)) {
        return false;
    }
    string op(condition[SQL_CONDITION_OPERATOR].GetString());
    if (op == SQL_IN_OP) {
        return parseIn(condition);
    } else if (op != SQL_EQUAL_OP) {
        return false;
    }
    const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
    if ((!param.IsArray()) || param.Size() != 2) {
        return false;
    }
    const SimpleValue &left = param[0];
    const SimpleValue &right = param[1];
    return parseEqual(left, right);
}

bool PrimaryKeyScanConditionVisitor::parseEqual(const SimpleValue &left, const SimpleValue &right) {
    bool leftIsCast = ExprUtil::isSameUdf(left, SQL_UDF_CAST_OP);
    bool rightIsCast = ExprUtil::isSameUdf(right, SQL_UDF_CAST_OP);
    if (leftIsCast && rightIsCast) {
        const SimpleValue &paramLeft = left[SQL_CONDITION_PARAMETER];
        if ((!paramLeft.IsArray()) || paramLeft.Size() == 0) {
            return false;
        }
        const SimpleValue &paramRight = right[SQL_CONDITION_PARAMETER];
        if ((!paramRight.IsArray()) || paramRight.Size() == 0) {
            return false;
        }
        return doParseEqual(paramLeft[0], paramRight[0]);
    } else if (leftIsCast) {
        const SimpleValue &param = left[SQL_CONDITION_PARAMETER];
        if ((!param.IsArray()) || param.Size() == 0) {
            return false;
        }
        return doParseEqual(param[0], right);
    } else if (rightIsCast) {
        const SimpleValue &param = right[SQL_CONDITION_PARAMETER];
        if ((!param.IsArray()) || param.Size() == 0) {
            return false;
        }
        return doParseEqual(left, param[0]);
    }
    return doParseEqual(left, right);
}

bool PrimaryKeyScanConditionVisitor::doParseEqual(const SimpleValue &left,
                                                  const SimpleValue &right) {
    if ((!left.IsNumber() && !left.IsString()) || (!right.IsNumber() && !right.IsString())) {
        return false;
    }
    bool leftHasColumn = SqlJsonUtil::isColumn(left);
    bool rightHasColumn = SqlJsonUtil::isColumn(right);
    if (leftHasColumn && rightHasColumn) {
        return false;
    } else if (leftHasColumn) {
        return tryAddKeyValue(left, right);
    } else if (rightHasColumn) {
        return tryAddKeyValue(right, left);
    } else {
        return false;
    }
}

bool PrimaryKeyScanConditionVisitor::tryAddKeyValue(const SimpleValue &attr,
                                                    const SimpleValue &value) {
    const string &attrName = SqlJsonUtil::getColumnName(attr);
    if (attrName != _keyFieldName) {
        return false;
    }
    string pkStr;
    if (value.IsInt64()) {
        pkStr = StringUtil::toString(value.GetInt64());
    } else if (value.IsString()) {
        pkStr = value.GetString();
    }
    if (pkStr.empty()) {
        return false;
    }
    auto pair = _rawKeySet.insert(pkStr);
    if (pair.second) {
        _rawKeyVec.emplace_back(pkStr);
    }
    return true;
}

} // namespace sql
