#include <ha3/sql/ops/khronosScan/KhronosDataPointConditionVisitor.h>
#include <ha3/sql/ops/khronosScan/KhronosCommon.h>
#include <ha3/sql/ops/khronosScan/KhronosUtil.h>
#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/util/SqlJsonUtil.h>
#include <ha3/sql/ops/condition/ExprUtil.h>
#include <autil/ConstString.h>
#include <autil/legacy/RapidJsonHelper.h>

using namespace std;
using namespace autil_rapidjson;
using namespace autil;
using namespace khronos::search;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(search, KhronosDataPointConditionVisitor);

#define CALL_AND_CHECK(expr)                    \
    expr;                                       \
    if (isError()) {                            \
        return;                                 \
    }


KhronosDataPointConditionVisitor::KhronosDataPointConditionVisitor(
        autil::mem_pool::Pool *pool,
        const IndexInfoMapType &indexInfos)
    : KhronosDataConditionVisitor(pool, indexInfos)
{
}

KhronosDataPointConditionVisitor::~KhronosDataPointConditionVisitor() {
}

void KhronosDataPointConditionVisitor::visitLeafCondition(LeafCondition *condition) {
    assert(!isError());
    const SimpleValue &leafCondition = condition->getCondition();
    if (!leafCondition.IsObject() || !leafCondition.HasMember(SQL_CONDITION_OPERATOR)
        || !leafCondition.HasMember(SQL_CONDITION_PARAMETER))
    {
        setErrorInfo("leaf cond is invalid: %s",
                     RapidJsonHelper::SimpleValue2Str(leafCondition).c_str());
        return;
    }
    string rawOp = leafCondition[SQL_CONDITION_OPERATOR].GetString();
    const SimpleValue &param = leafCondition[SQL_CONDITION_PARAMETER];
    string op = rawOp;

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
    if (isIndexColumn(param[0])) {
        CALL_AND_CHECK(parseAsTimestamp(param[0], param[1], op, rawOp));
    } else if (isIndexColumn(param[1])) {
        if (!ExprUtil::reverseOp(op)) {
            setErrorInfo("reverse op [%s] failed", op.c_str());
            return;
        }
        CALL_AND_CHECK(parseAsTimestamp(param[1], param[0], op, rawOp));
    } else if (ExprUtil::isItem(param[0])) {
        CALL_AND_CHECK(parseAsItem(param[0], param[1], op));
    } else if (ExprUtil::isItem(param[1])) {
        if (!ExprUtil::reverseOp(op)) {
            setErrorInfo("reverse op [%s] failed", op.c_str());
            return;
        }
        CALL_AND_CHECK(parseAsItem(param[1], param[0], op));
    } else {
        setErrorInfo("LeafCondition pattern can not be recognized: %s",
                     RapidJsonHelper::SimpleValue2Str(leafCondition).c_str());
        return;
    }
}

void KhronosDataPointConditionVisitor::parseAsTimestamp(const SimpleValue &leftIn,
        const SimpleValue &rightIn,
        const string &op,
        const string &rawOp)
{
    std::string leftValue = leftIn.GetString();
    if (!KhronosUtil::typeMatch(_indexInfos, leftValue, KHRONOS_TIMESTAMP_TYPE)) {
        setErrorInfo("[%s] is not KHRONOS_TIMESTAMP_TYPE", leftValue.c_str());
        return;
    }

    const SimpleValue *rightUnCastContainer = &rightIn;
    const SimpleValue **rightUnCastOut = &rightUnCastContainer;
    if (!ExprUtil::unCast(rightUnCastOut)) {
        setErrorInfo("unCast oprand failed: %s", RapidJsonHelper::SimpleValue2Str(rightIn).c_str());
        return;
    }

    const SimpleValue &right = **rightUnCastOut;
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
}

END_HA3_NAMESPACE(sql);
