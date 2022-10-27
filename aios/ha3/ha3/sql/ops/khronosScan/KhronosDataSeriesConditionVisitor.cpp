#include <ha3/sql/ops/khronosScan/KhronosDataSeriesConditionVisitor.h>
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
HA3_LOG_SETUP(search, KhronosDataSeriesConditionVisitor);

#define CALL_AND_CHECK(expr)                    \
    expr;                                       \
    if (isError()) {                            \
        return;                                 \
    }


KhronosDataSeriesConditionVisitor::KhronosDataSeriesConditionVisitor(
        autil::mem_pool::Pool *pool,
        const IndexInfoMapType &indexInfos)
    : KhronosDataConditionVisitor(pool, indexInfos)
{
}

KhronosDataSeriesConditionVisitor::~KhronosDataSeriesConditionVisitor() {
}

void KhronosDataSeriesConditionVisitor::visitLeafCondition(LeafCondition *condition) {
    assert(!isError());
    const SimpleValue &leafCondition = condition->getCondition();
    if (ExprUtil::isUdf(leafCondition)) {
        CALL_AND_CHECK(parseAsTimestampUdf(leafCondition));
    } else {
        if (!leafCondition.IsObject() || !leafCondition.HasMember(SQL_CONDITION_OPERATOR)
            || !leafCondition.HasMember(SQL_CONDITION_PARAMETER))
        {
            setErrorInfo("leaf cond is invalid: %s",
                         RapidJsonHelper::SimpleValue2Str(leafCondition).c_str());
            return;
        }
        string op = leafCondition[SQL_CONDITION_OPERATOR].GetString();
        const SimpleValue &param = leafCondition[SQL_CONDITION_PARAMETER];
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
        if (ExprUtil::isItem(param[0])) {
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
}

void KhronosDataSeriesConditionVisitor::parseAsTimestampUdf(
        const SimpleValue &leafCondition)
{
    const std::string &funcName = leafCondition[SQL_CONDITION_OPERATOR].GetString();
    const std::string &op = KhronosUtil::timestampUdf2Op(funcName);
    if (op == SQL_UNKNOWN_OP) {
        setErrorInfo("udf [%s] is not supported", funcName.c_str());
        return;
    }
    const SimpleValue &param = leafCondition[SQL_CONDITION_PARAMETER];
    if (param.Size() != 1) {
        setErrorInfo("param [%s] size is not 1 in udf [%s]",
                     RapidJsonHelper::SimpleValue2Str(param).c_str(), funcName.c_str());
        return;
    }
    if (!param[0].IsInt64()) {
        setErrorInfo("param [%s] must be int type in udf [%s]",
                     RapidJsonHelper::SimpleValue2Str(param).c_str(), funcName.c_str());
        return;
    }
    if (!KhronosUtil::updateTimeRange(op, param[0].GetInt64(), _tsRange))
    {
        setErrorInfo("KHRONOS_TIMESTAMP_UDF [%s] not supported", funcName.c_str());
        return;
    }
}

END_HA3_NAMESPACE(sql);
