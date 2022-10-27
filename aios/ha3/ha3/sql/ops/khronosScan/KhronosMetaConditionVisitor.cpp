#include <ha3/sql/ops/khronosScan/KhronosMetaConditionVisitor.h>
#include <ha3/sql/ops/khronosScan/KhronosCommon.h>
#include <ha3/sql/ops/khronosScan/KhronosUtil.h>
#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/util/SqlJsonUtil.h>
#include <ha3/sql/ops/condition/ExprUtil.h>
#include <autil/legacy/RapidJsonHelper.h>


using namespace std;
using namespace autil_rapidjson;
using namespace autil;
using namespace khronos::search;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(search, KhronosMetaConditionVisitor);

#define CALL_AND_CHECK(expr)                    \
    expr;                                       \
    if (isError()) {                            \
        return;                                 \
    }

KhronosMetaConditionVisitor::KhronosMetaConditionVisitor(
        const IndexInfoMapType &indexInfos)
    : _indexInfos(indexInfos)
    , _tsRange(TimeRange::createAllTimeRange())
{
}

KhronosMetaConditionVisitor::~KhronosMetaConditionVisitor() {
}

void KhronosMetaConditionVisitor::visitAndCondition(AndCondition *condition) {
    vector<ConditionPtr> children = condition->getChildCondition();
    for (size_t i = 0; i < children.size(); i++) {
        CALL_AND_CHECK(children[i]->accept(this));
    }
}

void KhronosMetaConditionVisitor::visitOrCondition(OrCondition *condition) {
    setErrorInfo("[OR] cond not supported");
}

void KhronosMetaConditionVisitor::visitNotCondition(NotCondition *condition) {
    setErrorInfo("[NOT] cond not supported");
}

void KhronosMetaConditionVisitor::visitLeafCondition(LeafCondition *condition) {
    const SimpleValue &leafCondition = condition->getCondition();
    CALL_AND_CHECK(parseLeaf(leafCondition));
}

bool KhronosMetaConditionVisitor::isIndexColumn(const autil_rapidjson::SimpleValue& param)
{
    return SqlJsonUtil::isColumn(param)
        && (_indexInfos.find(param.GetString()) != _indexInfos.end());
}

void KhronosMetaConditionVisitor::handleParams(
        const SimpleValue &param, std::string &op,
        const SimpleValue **left, const SimpleValue **right)
{
    if (!param.IsArray()) {
        setErrorInfo("param [%s] is not array in op [%s]",
                     RapidJsonHelper::SimpleValue2Str(param).c_str(), op.c_str());
        return;
    }
    if (param.Size() != 2) {
        setErrorInfo("param [%s] size is not 2 in op [%s]",
                     RapidJsonHelper::SimpleValue2Str(param).c_str(), op.c_str());
        return;
    }

    if (isIndexColumn(param[0]) && !isIndexColumn(param[1])) {
        *left = &param[0];
        *right = &param[1];
    } else if (isIndexColumn(param[1]) && !isIndexColumn(param[0])) {
        *left = &param[1];
        *right = &param[0];
        if (!ExprUtil::reverseOp(op)) {
            setErrorInfo("reverse op [%s] failed", op.c_str());
            return;
        }
    } else {
        setErrorInfo("param [%s] is invalid in op [%s]",
                     RapidJsonHelper::SimpleValue2Str(param).c_str(), op.c_str());
        return;
    }
    const SimpleValue &rawRight = **right;
    if (!ExprUtil::unCast(right)) {
        setErrorInfo("unCast [%s] failed",
                     RapidJsonHelper::SimpleValue2Str(rawRight).c_str());
        return;
    }
}

void KhronosMetaConditionVisitor::parseLeaf(const SimpleValue &condition) {
    if (!condition.IsObject() || !condition.HasMember(SQL_CONDITION_OPERATOR)
        || !condition.HasMember(SQL_CONDITION_PARAMETER))
    {
        setErrorInfo("leaf cond is invalid: [%s]",
                     RapidJsonHelper::SimpleValue2Str(condition).c_str());
        return;
    }
    string rawOp = condition[SQL_CONDITION_OPERATOR].GetString();
    const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
    string op = rawOp;
    const SimpleValue *leftOut = nullptr;
    const SimpleValue *rightOut = nullptr;
    CALL_AND_CHECK(handleParams(param, op, &leftOut, &rightOut));

    const SimpleValue &left = *leftOut;
    const SimpleValue &right = *rightOut;

    std::string leftValue = left.GetString();
    if (KhronosUtil::typeMatch(_indexInfos, leftValue, KHRONOS_TIMESTAMP_TYPE)) {
        // timestamp
        if (!right.IsInt64()) {
            setErrorInfo("oprand [%s] type is not supportd for KHRONOS_TIMESTAMP_TYPE field",
                         RapidJsonHelper::SimpleValue2Str(right).c_str());
            return;
        }
        int64_t rightValue = right.GetInt64();
        if (!KhronosUtil::updateTimeRange(op, rightValue, _tsRange)) {
            setErrorInfo("KHRONOS_TIMESTAMP_TYPE op [%s] not supported", rawOp.c_str());
            return;
        }
    } else if (KhronosUtil::typeMatch(_indexInfos, leftValue, KHRONOS_METRIC_TYPE)) {
        // metric
        CALL_AND_CHECK(parseAsMetric(op, rawOp, right));
    } else {
        // tagk,tagv
        if (!right.IsString()) {
            setErrorInfo("oprand is not string: %s",
                         RapidJsonHelper::SimpleValue2Str(right).c_str());
            return;
        }
        std::string rightValue = right.GetString();
        CALL_AND_CHECK(parseTagkTagvFromString(op, rawOp, leftValue, rightValue));
    }
}

void KhronosMetaConditionVisitor::parseAsMetric(
        const string &op,
        const string &rawOp,
        const SimpleValue &right)
{
    if (op == SQL_EQUAL_OP) {
        if (right.IsString()) {
            string rightValue = right.GetString();
            if (rightValue.find(KHRONOS_MATCH_ALL) != string::npos) {
                setErrorInfo("\"*\" is not supported by metric op [=], metric [%s]",
                        rightValue.c_str());
                return;
            }
            _metricVec.emplace_back(rightValue, SearchInterface::METRIC_PLAIN_TEXT);
        } else if (ExprUtil::isUdf(right)) {
            CALL_AND_CHECK(parseMetricFromUDFWithEqualOp(right));
        } else {
            setErrorInfo("metric expr not supported in op [=]: [%s]",
                         RapidJsonHelper::SimpleValue2Str(right).c_str());
            return;
        }
    } else if (op == SQL_LIKE_OP) {
        if (!right.IsString()) {
            setErrorInfo("op [LIKE] only support string type oprand: [%s]",
                         RapidJsonHelper::SimpleValue2Str(right).c_str());
            return;
        }
        string rightValue = right.GetString();
        auto firstPos = rightValue.find(KHRONOS_MATCH_ALL);
        if (firstPos == string::npos) { // no wildcard
            _metricVec.emplace_back(rightValue, SearchInterface::METRIC_PLAIN_TEXT);
        } else if (firstPos == rightValue.length() - 1) {
            rightValue.pop_back();
            _metricVec.emplace_back(rightValue, SearchInterface::METRIC_PREFIX);
        } else {
            rightValue = KhronosUtil::makeWildcardToRegex(rightValue);
            _metricVec.emplace_back(rightValue, SearchInterface::METRIC_REGEX);
        }
    } else {
        setErrorInfo("metric not support op [%s]", rawOp.c_str());
        return;
    }
}

void KhronosMetaConditionVisitor::parseMetricFromUDFWithEqualOp(const SimpleValue &value) {
    if (!ExprUtil::isUdf(value)) {
        setErrorInfo("not udf for metric [%s]",
                     RapidJsonHelper::SimpleValue2Str(value).c_str());
        return;
    }
    const string funcName = value[SQL_CONDITION_OPERATOR].GetString();
    const SimpleValue &funcParam = value[SQL_CONDITION_PARAMETER];
    if (funcParam.Size() != 1) {
        setErrorInfo("UDF param size must be 1 for metric [%s]",
                     RapidJsonHelper::SimpleValue2Str(funcParam).c_str());
        return;
    }
    if (funcName == SQL_UDF_CAST_OP) {
        CALL_AND_CHECK(parseAsMetric(SQL_EQUAL_OP, SQL_EQUAL_OP, funcParam[0]));
        return;
    }
    const SimpleValue &firstParam = funcParam[0];
    if (!firstParam.IsString()) {
        setErrorInfo("func [%s] param [%s] type must be string", funcName.c_str(),
                     RapidJsonHelper::SimpleValue2Str(firstParam).c_str());
        return;
    }

    std::string merticStr;
    SearchInterface::METRIC_MATCH_TYPE metricMatchType;
    if (funcName == KHRONOS_UDF_WILDCARD) {
        metricMatchType = SearchInterface::METRIC_REGEX;
        merticStr = KhronosUtil::makeWildcardToRegex(firstParam.GetString());
    } else if (funcName == KHRONOS_UDF_IWILDCARD) {
        metricMatchType = SearchInterface::METRIC_REGEX_I;
        merticStr = KhronosUtil::makeWildcardToRegex(firstParam.GetString());
    } else if (funcName == KHRONOS_UDF_REGEXP) {
        metricMatchType = SearchInterface::METRIC_REGEX;
        merticStr = firstParam.GetString();
    } else {
        setErrorInfo("UDF [%s] is not supported for metric op [=]", funcName.c_str());
        return;
    }
    _metricVec.emplace_back(merticStr, metricMatchType);
}

void KhronosMetaConditionVisitor::parseTagkTagvFromString(
        const std::string &op,
        const std::string &rawOp,
        const std::string &leftValue,
        const std::string &rightValue)
{
    auto firstPos = rightValue.find(KHRONOS_MATCH_ALL);
    if (op == SQL_EQUAL_OP) {
        if (firstPos != string::npos) {
            setErrorInfo("\"*\" is not supported by op [=], field=[%s], value=[%s]",
                         leftValue.c_str(), rightValue.c_str());
            return;
        }
        // pass
    } else if (op == SQL_LIKE_OP) {
        // pass
    } else {
        setErrorInfo("field [%s] not support op [%s]",
                     leftValue.c_str(), rawOp.c_str());
        return;
    }
    if (KhronosUtil::typeMatch(_indexInfos, leftValue, KHRONOS_TAG_KEY_TYPE)) {
        _tagkVec.emplace_back(rightValue);
    } else if (KhronosUtil::typeMatch(_indexInfos, leftValue, KHRONOS_TAG_VALUE_TYPE)) {
        _tagvVec.emplace_back(rightValue);
    } else {
        setErrorInfo("field [%s] not supported", leftValue.c_str());
        return;
    }
}

END_HA3_NAMESPACE(sql);
