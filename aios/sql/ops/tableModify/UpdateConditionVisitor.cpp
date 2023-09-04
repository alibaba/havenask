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
#include "sql/ops/tableModify/UpdateConditionVisitor.h"

#include <algorithm>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <iterator>
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
AUTIL_LOG_SETUP(sql, UpdateConditionVisitor);

UpdateConditionVisitor::UpdateConditionVisitor(const std::set<std::string> &fetchFields)
    : _fetchFields(fetchFields) {}

UpdateConditionVisitor::~UpdateConditionVisitor() {}

void UpdateConditionVisitor::visitAndCondition(AndCondition *condition) {
    if (isError()) {
        return;
    }
    const vector<ConditionPtr> &children = condition->getChildCondition();
    std::vector<std::map<std::string, std::string>> values;
    for (size_t i = 0; i < children.size(); i++) {
        children[i]->accept(this);
        if (isError()) {
            return;
        }
        if (_values.size() != 1) {
            setErrorInfo("Update failed: `and` only support single key");
            return;
        }
        if (values.empty()) {
            values.emplace_back(_values[0]);
        } else {
            if (hasSameFields(values[0], _values[0])) {
                setErrorInfo("Update failed: `and` not support same key, [%s] [%s]",
                             StringUtil::toString(_values[0]).c_str(),
                             StringUtil::toString(values[0]).c_str());
                return;
            } else {
                values[0].insert(_values[0].begin(), _values[0].end());
            }
        }
    }
    std::swap(_values, values);
}

void UpdateConditionVisitor::visitOrCondition(OrCondition *condition) {
    if (isError()) {
        return;
    }
    const vector<ConditionPtr> &children = condition->getChildCondition();
    std::vector<std::map<std::string, std::string>> values;
    for (size_t i = 0; i < children.size(); i++) {
        children[i]->accept(this);
        if (isError()) {
            return;
        }
        for (size_t i = 0; i < _values.size(); ++i) {
            if (values.empty()) {
                values.emplace_back(_values[i]);
            } else {
                if (isSameFields(values[0], _values[i])) {
                    values.emplace_back(_values[i]);
                } else {
                    setErrorInfo("Update failed: [%s] can not insert to [%s]",
                                 StringUtil::toString(_values[i]).c_str(),
                                 StringUtil::toString(values[0]).c_str());
                    return;
                }
            }
        }
    }
    std::swap(_values, values);
}

bool UpdateConditionVisitor::hasSameFields(const std::map<std::string, std::string> &a,
                                           const std::map<std::string, std::string> &b) {
    for (auto pair : a) {
        if (b.find(pair.first) != b.end()) {
            return true;
        }
    }
    return false;
}

bool UpdateConditionVisitor::isIncludeFields(const std::map<std::string, std::string> &a,
                                             const std::map<std::string, std::string> &b) {
    for (auto pair : b) {
        if (a.find(pair.first) == a.end()) {
            return false;
        }
    }
    return true;
}

bool UpdateConditionVisitor::isSameFields(const std::map<std::string, std::string> &a,
                                          const std::map<std::string, std::string> &b) {
    if (a.size() != b.size()) {
        return false;
    }
    return isIncludeFields(a, b);
}

void UpdateConditionVisitor::visitNotCondition(NotCondition *condition) {
    if (isError()) {
        return;
    }
    setErrorInfo("Update not supported: `not` expr");
}

void UpdateConditionVisitor::visitLeafCondition(LeafCondition *condition) {
    if (isError()) {
        return;
    }
    const SimpleValue &leafCondition = condition->getCondition();
    if (parseKey(leafCondition)) {
        SQL_LOG(DEBUG,
                "convert udf [%s] to query",
                RapidJsonHelper::SimpleValue2Str(leafCondition).c_str());
    } else {
        setErrorInfo("Update not supported: %s",
                     RapidJsonHelper::SimpleValue2Str(leafCondition).c_str());
        return;
    }
}

bool UpdateConditionVisitor::parseKey(const SimpleValue &condition) {
    if (!condition.IsObject() || !condition.HasMember(SQL_CONDITION_OPERATOR)
        || !condition.HasMember(SQL_CONDITION_PARAMETER)) {
        return false;
    }
    string op(condition[SQL_CONDITION_OPERATOR].GetString());
    if (op == SQL_IN_OP) {
        return parseIn(condition);
    } else if (op == SQL_EQUAL_OP) {
        const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
        if ((!param.IsArray()) || param.Size() != 2) {
            SQL_LOG(ERROR,
                    "[%s] op `=` params size != 2",
                    RapidJsonHelper::SimpleValue2Str(condition).c_str())
            return false;
        }
        const SimpleValue &left = param[0];
        const SimpleValue &right = param[1];
        return parseEqual(left, right);
    } else {
        SQL_LOG(ERROR,
                "Update only supported `IN` OR `=`: %s",
                RapidJsonHelper::SimpleValue2Str(condition).c_str());
        return false;
    }
}

bool UpdateConditionVisitor::parseIn(const SimpleValue &condition) {
    const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
    if ((!param.IsArray()) || param.Size() < 2) {
        return false;
    }
    if (!SqlJsonUtil::isColumn(param[0])) {
        SQL_LOG(ERROR, "`In` args1 is not column");
        return false;
    }
    string columnName = SqlJsonUtil::getColumnName(param[0]);
    std::vector<std::map<std::string, std::string>> values;
    for (size_t i = 1; i < param.Size(); ++i) {
        if (!parseEqual(param[0], param[i])) {
            SQL_LOG(ERROR, "parseEqual failed, index [%ld]", i);
            return false;
        }
        values.push_back(_values[0]);
    }
    std::swap(_values, values);
    return true;
}

bool UpdateConditionVisitor::parseEqual(const SimpleValue &left, const SimpleValue &right) {
    bool leftIsCast = ExprUtil::isSameUdf(left, SQL_UDF_CAST_OP);
    bool rightIsCast = ExprUtil::isSameUdf(right, SQL_UDF_CAST_OP);
    if (leftIsCast && rightIsCast) {
        const SimpleValue &paramLeft = left[SQL_CONDITION_PARAMETER];
        if ((!paramLeft.IsArray()) || paramLeft.Size() == 0) {
            SQL_LOG(
                ERROR, "[%s] params is not array", RapidJsonHelper::SimpleValue2Str(left).c_str());
            return false;
        }
        const SimpleValue &paramRight = right[SQL_CONDITION_PARAMETER];
        if ((!paramRight.IsArray()) || paramRight.Size() == 0) {
            SQL_LOG(
                ERROR, "[%s] params is not array", RapidJsonHelper::SimpleValue2Str(right).c_str());
            return false;
        }
        return doParseEqual(paramLeft[0], paramRight[0]);
    } else if (leftIsCast) {
        const SimpleValue &param = left[SQL_CONDITION_PARAMETER];
        if ((!param.IsArray()) || param.Size() == 0) {
            SQL_LOG(
                ERROR, "[%s] params is not array", RapidJsonHelper::SimpleValue2Str(left).c_str());
            return false;
        }
        return doParseEqual(param[0], right);
    } else if (rightIsCast) {
        const SimpleValue &param = right[SQL_CONDITION_PARAMETER];
        if ((!param.IsArray()) || param.Size() == 0) {
            SQL_LOG(
                ERROR, "[%s] params is not array", RapidJsonHelper::SimpleValue2Str(right).c_str());
            return false;
        }
        return doParseEqual(left, param[0]);
    }
    return doParseEqual(left, right);
}

bool UpdateConditionVisitor::doParseEqual(const SimpleValue &left, const SimpleValue &right) {
    if ((!left.IsNumber() && !left.IsString()) || (!right.IsNumber() && !right.IsString())) {
        SQL_LOG(ERROR,
                "unexpected type, [%s] [%s]",
                RapidJsonHelper::SimpleValue2Str(left).c_str(),
                RapidJsonHelper::SimpleValue2Str(right).c_str());
        return false;
    }
    bool leftHasColumn = SqlJsonUtil::isColumn(left);
    bool rightHasColumn = SqlJsonUtil::isColumn(right);
    if (leftHasColumn && rightHasColumn) {
        SQL_LOG(ERROR,
                "not found column, [%s] [%s]",
                RapidJsonHelper::SimpleValue2Str(left).c_str(),
                RapidJsonHelper::SimpleValue2Str(right).c_str());
        return false;
    } else if (leftHasColumn) {
        return tryAddKeyValue(left, right);
    } else if (rightHasColumn) {
        return tryAddKeyValue(right, left);
    } else {
        SQL_LOG(ERROR,
                "unexpected both column, [%s] [%s]",
                RapidJsonHelper::SimpleValue2Str(left).c_str(),
                RapidJsonHelper::SimpleValue2Str(right).c_str());
        return false;
    }
}

bool UpdateConditionVisitor::tryAddKeyValue(const SimpleValue &attr, const SimpleValue &value) {
    const string &attrName = SqlJsonUtil::getColumnName(attr);
    string valueStr;
    if (value.IsInt64()) {
        valueStr = StringUtil::toString(value.GetInt64());
    } else if (value.IsString()) {
        valueStr = value.GetString();
    }
    if (valueStr.empty()) {
        SQL_LOG(ERROR, "fetch value [%s] failed", RapidJsonHelper::SimpleValue2Str(value).c_str());
        return false;
    }
    _values = {{{attrName, valueStr}}};
    return true;
}

bool UpdateConditionVisitor::fetchValues(std::vector<std::map<std::string, std::string>> &values) {
    if (isError()) {
        SQL_LOG(ERROR, "fetch values has error, [%s]", errorInfo().c_str());
        return false;
    }
    if (!checkFetchValues()) {
        return false;
    }
    std::swap(_values, values);
    return true;
}

bool UpdateConditionVisitor::checkFetchValues() {
    std::map<std::string, std::string> fetchFields;
    std::transform(_fetchFields.begin(),
                   _fetchFields.end(),
                   std::inserter(fetchFields, begin(fetchFields)),
                   [](const std::string &arg) { return std::make_pair(arg, ""); });
    for (size_t i = 0; i < _values.size(); ++i) {
        if (!isSameFields(_values[i], fetchFields)) {
            SQL_LOG(ERROR,
                    "UPDATE: fetch values [%s] not same as expected [%s]",
                    StringUtil::toString(_values).c_str(),
                    StringUtil::toString(_fetchFields).c_str());
            return false;
        }
    }
    return true;
}

} // namespace sql
