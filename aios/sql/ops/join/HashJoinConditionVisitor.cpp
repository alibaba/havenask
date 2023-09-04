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
#include "sql/ops/join/HashJoinConditionVisitor.h"

#include <iosfwd>
#include <memory>

#include "autil/TimeUtility.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "rapidjson/document.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/condition/ExprUtil.h"
#include "sql/ops/condition/SqlJsonUtil.h"
#include "suez/turing/expression/common.h"

using namespace std;
using namespace autil;
using namespace suez::turing;

namespace sql {

HashJoinConditionVisitor::HashJoinConditionVisitor(
    const map<string, pair<string, bool>> &output2InputMap)
    : _output2InputMap(output2InputMap) {}

HashJoinConditionVisitor::~HashJoinConditionVisitor() {}

void HashJoinConditionVisitor::visitAndCondition(AndCondition *condition) {
    if (isError()) {
        return;
    }
    vector<ConditionPtr> conditions = condition->getChildCondition();
    vector<string> left;
    vector<string> right;
    for (auto condition : conditions) {
        condition->accept(this);
        stealJoinColumns(left, right);
        if (isError()) {
            break;
        }
    }
    if (isError()) {
        return;
    }
    _leftColumns = std::move(left);
    _rightColumns = std::move(right);
}

void HashJoinConditionVisitor::visitOrCondition(OrCondition *condition) {
    string errorInfo
        = "Hash join condition not support OR condition [" + condition->toString() + "].";
    SQL_LOG(WARN, "%s", errorInfo.c_str());
    setErrorInfo(errorInfo);
}

void HashJoinConditionVisitor::visitNotCondition(NotCondition *condition) {
    string errorInfo
        = "Hash join condition not support NOT condition [" + condition->toString() + "].";
    SQL_LOG(WARN, "%s", errorInfo.c_str());
    setErrorInfo(errorInfo);
}

void HashJoinConditionVisitor::visitLeafCondition(LeafCondition *condition) {
    if (isError()) {
        return;
    }
    const SimpleValue &leafCondition = condition->getCondition();
    if (ExprUtil::isUdf(leafCondition)) {
        string errorInfo = "not support udf [" + condition->toString() + "].";
        SQL_LOG(WARN, "%s", errorInfo.c_str());
        setErrorInfo(errorInfo);
    } else {
        string left, right;
        if (leafCondition.IsBool()) {
            if (leafCondition.GetBool() == false) {
                string errorInfo = "unsupported join condition [" + condition->toString() + "]";
                SQL_LOG(WARN, "%s", errorInfo.c_str());
                setErrorInfo(errorInfo);
                return;
            }
            SQL_LOG(INFO, "ignore bool expr [%s]", condition->toString().c_str());
            return;
        }
        if (!parseJoinColumns(leafCondition, left, right)) {
            string errorInfo = "parse join columns failed [" + condition->toString() + "].";
            SQL_LOG(WARN, "%s", errorInfo.c_str());
            setErrorInfo(errorInfo);
            return;
        }
        if (!transOutputToInput(left) || !transOutputToInput(right)) {
            return;
        }
        if (_leftColumns.size() != _rightColumns.size()) {
            string errorInfo = "join field count not equal.";
            SQL_LOG(WARN, "%s", errorInfo.c_str());
            setErrorInfo(errorInfo);
            return;
        }
    }
}

bool HashJoinConditionVisitor::transOutputToInput(const string &outputFiled) {
    auto iter = _output2InputMap.find(outputFiled);
    if (iter == _output2InputMap.end()) {
        string errorInfo = "field [" + outputFiled + "] not found in output fields.";
        SQL_LOG(WARN, "%s", errorInfo.c_str());
        setErrorInfo(errorInfo);
        return false;
    } else {
        if (iter->second.second) {
            _leftColumns.emplace_back(iter->second.first);
        } else {
            _rightColumns.emplace_back(iter->second.first);
        }
    }
    return true;
}

bool HashJoinConditionVisitor::parseJoinColumns(const SimpleValue &condition,
                                                string &left,
                                                string &right) {
    if (!condition.IsObject() || !condition.HasMember(SQL_CONDITION_OPERATOR)
        || !condition.HasMember(SQL_CONDITION_PARAMETER)) {
        return false;
    }
    string op(condition[SQL_CONDITION_OPERATOR].GetString());
    if (op != SQL_EQUAL_OP && op != SQL_MULTI_EQUAL_OP) {
        SQL_LOG(ERROR, "op is %s", op.c_str());
        return false;
    }
    const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
    if ((!param.IsArray()) || param.Size() != 2) {
        return false;
    }
    bool leftHasAnyUdf = false;
    if (!parseJoinColumn(param[0], left, leftHasAnyUdf)) {
        return false;
    }
    bool rightHasAnyUdf = false;
    if (!parseJoinColumn(param[1], right, rightHasAnyUdf)) {
        return false;
    }
    if (leftHasAnyUdf && rightHasAnyUdf) {
        left.clear();
        right.clear();
        return false;
    }
    return true;
}

bool HashJoinConditionVisitor::parseJoinColumn(const SimpleValue &value,
                                               string &column,
                                               bool &hasAnyUdf) {
    if (ExprUtil::isSameUdf(value, SQL_UDF_ANY_OP)) {
        const SimpleValue &param = value[SQL_CONDITION_PARAMETER];
        if ((!param.IsArray()) || param.Size() != 1) {
            return false;
        }
        if (!param[0].IsString() || !SqlJsonUtil::isColumn(param[0])) {
            return false;
        } else {
            column = SqlJsonUtil::getColumnName(param[0]);
            hasAnyUdf = true;
        }
    } else {
        if (!value.IsString() || !SqlJsonUtil::isColumn(value)) {
            return false;
        } else {
            column = SqlJsonUtil::getColumnName(value);
            hasAnyUdf = false;
        }
    }
    return true;
}

void HashJoinConditionVisitor::stealJoinColumns(vector<string> &left, vector<string> &right) {
    left.insert(left.end(), _leftColumns.begin(), _leftColumns.end());
    right.insert(right.end(), _rightColumns.begin(), _rightColumns.end());
    _leftColumns.clear();
    _rightColumns.clear();
}

} // namespace sql
