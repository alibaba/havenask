#include <ha3/sql/ops/join/HashJoinConditionVisitor.h>
#include <ha3/sql/common/common.h>
#include <autil/legacy/RapidJsonHelper.h>
#include <ha3/sql/ops/util/SqlJsonUtil.h>
#include <ha3/sql/ops/condition/ExprUtil.h>
#include <memory>

using namespace std;
using namespace autil;
using namespace autil_rapidjson;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(sql);

HashJoinConditionVisitor::HashJoinConditionVisitor(
        const map<string, pair<string, bool>> &output2InputMap)
    : _output2InputMap(output2InputMap)
{}

HashJoinConditionVisitor::~HashJoinConditionVisitor() {
}

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
    _leftColumns = left;
    _rightColumns = right;
}

void HashJoinConditionVisitor::visitOrCondition(OrCondition *condition) {
    string errorInfo = "Hash join condition not support OR condition ["
                       + condition->toString() +"].";
    SQL_LOG(WARN, "%s", errorInfo.c_str());
    setErrorInfo(errorInfo);
}

void HashJoinConditionVisitor::visitNotCondition(NotCondition *condition) {
    string errorInfo = "Hash join condition not support NOT condition ["
                       + condition->toString() +"].";
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
                string errorInfo = "unsupported join condition [" +
                                   condition->toString() + "]";
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

bool HashJoinConditionVisitor::parseJoinColumns(const SimpleValue& condition,
        string &left, string &right)
{
    if (!condition.IsObject() || !condition.HasMember(SQL_CONDITION_OPERATOR)
        || !condition.HasMember(SQL_CONDITION_PARAMETER))
    {
        return false;
    }
    string op(condition[SQL_CONDITION_OPERATOR].GetString());
    if (op != SQL_EQUAL_OP) {
        return false;
    }
    const SimpleValue& param = condition[SQL_CONDITION_PARAMETER];
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

bool HashJoinConditionVisitor::parseJoinColumn(const SimpleValue& value,
        string &column, bool &hasAnyUdf)
{
    if (ExprUtil::isAnyUdf(value)) {
        const SimpleValue& param = value[SQL_CONDITION_PARAMETER];
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


END_HA3_NAMESPACE(sql);
