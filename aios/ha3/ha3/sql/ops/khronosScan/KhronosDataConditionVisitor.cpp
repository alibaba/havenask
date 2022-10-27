#include <ha3/sql/ops/khronosScan/KhronosDataConditionVisitor.h>
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
HA3_LOG_SETUP(search, KhronosDataConditionVisitor);

#define CALL_AND_CHECK(expr)                    \
    expr;                                       \
    if (isError()) {                            \
        return;                                 \
    }


KhronosDataConditionVisitor::KhronosDataConditionVisitor(
        autil::mem_pool::Pool *pool,
        const IndexInfoMapType &indexInfos)
    : _pool(pool)
    , _indexInfos(indexInfos)
    , _tsRange(TimeRange::createAllTimeRange())
{
}

KhronosDataConditionVisitor::~KhronosDataConditionVisitor() {
}

void KhronosDataConditionVisitor::visitAndCondition(AndCondition *condition) {
    assert(!isError());
    vector<ConditionPtr> children = condition->getChildCondition();
    for (size_t i = 0; i < children.size(); i++) {
        CALL_AND_CHECK(children[i]->accept(this));
    }
}

void KhronosDataConditionVisitor::visitOrCondition(OrCondition *condition) {
    setErrorInfo("[OR] cond not supported");
}

void KhronosDataConditionVisitor::visitNotCondition(NotCondition *condition) {
    setErrorInfo("[NOT] cond not supported");
}

bool KhronosDataConditionVisitor::isIndexColumn(const autil_rapidjson::SimpleValue& param)
{
    return SqlJsonUtil::isColumn(param)
        && (_indexInfos.find(param.GetString()) != _indexInfos.end());
}

void KhronosDataConditionVisitor::parseAsItem(
        const SimpleValue &leftIn,
        const SimpleValue &rightIn,
        const string &op)
{
    string itemName;
    string tagk;
    if (!ExprUtil::parseItemVariable(leftIn, itemName, tagk)) {
        setErrorInfo("oprand is not ITEM type: %s",
                     RapidJsonHelper::SimpleValue2Str(leftIn).c_str());
        return;
    }
    if (op != SQL_EQUAL_OP && op != SQL_LIKE_OP) {
        setErrorInfo("tags condition only support [= or like] operator: tagk=[%s], op=[%s]",
                     tagk.c_str(), op.c_str());
        return;
    }

    if (rightIn.IsString()) {
        auto tagvMatchType = SearchInterface::LITERAL;
        ConstString tagv(rightIn.GetString(), _pool);
        _tagKVInfos.push_back(std::make_pair(ConstString(tagk.c_str(), _pool),
                        SearchInterface::TagvMatchInfo(tagv, tagvMatchType)));
    } else if (ExprUtil::isUdf(rightIn)) {
        CALL_AND_CHECK(parseTagsFromUDF(tagk, rightIn));
    } else {
        setErrorInfo("oprand type can not be recognized for tagk=[%s]: %s",
                     tagk.c_str(),
                     RapidJsonHelper::SimpleValue2Str(rightIn).c_str());

    }
}

void KhronosDataConditionVisitor::parseTagsFromUDF(
        const std::string &tagk,
        const SimpleValue &value)
{
    SearchInterface::TAGV_MATCH_TYPE tagvMatchType;
    ConstString tagv;
    if (!ExprUtil::isUdf(value)) {
        setErrorInfo("not udf for tagk=[%s], value is %s",
                     tagk.c_str(),
                     RapidJsonHelper::SimpleValue2Str(value).c_str());
        return;
    }
    const string funcName = value[SQL_CONDITION_OPERATOR].GetString();
    const SimpleValue &funcParam = value[SQL_CONDITION_PARAMETER];
    if (funcParam.Size() != 1) {
        setErrorInfo("UDF param size must be 1 for tagk=[%s], param is %s",
                     tagk.c_str(),
                     RapidJsonHelper::SimpleValue2Str(funcParam).c_str());
        return;
    }
    if (funcName == SQL_UDF_CAST_OP) {
        CALL_AND_CHECK(parseTagsFromUDF(tagk, funcParam[0]));
        return;
    }
    const SimpleValue &firstParam = funcParam[0];
    if (!firstParam.IsString()) {
        setErrorInfo("func [%s] param [%s] type must be string", funcName.c_str(),
                     RapidJsonHelper::SimpleValue2Str(firstParam).c_str());
        return;
    }

    if (funcName == KHRONOS_UDF_LITERAL_OR) {
        tagvMatchType = SearchInterface::LITERAL_OR;
        tagv = ConstString(firstParam.GetString(), _pool);
    } else if (funcName == KHRONOS_UDF_ILITERAL_OR) {
        tagvMatchType = SearchInterface::ILITERAL_OR;
        tagv = ConstString(firstParam.GetString(), _pool);
    } else if (funcName == KHRONOS_UDF_NOT_LITERAL_OR) {
        tagvMatchType = SearchInterface::NOT_LITERAL_OR;
        tagv = ConstString(firstParam.GetString(), _pool);
    } else if (funcName == KHRONOS_UDF_NOT_ILITERAL_OR) {
        tagvMatchType = SearchInterface::NOT_ILITERAL_OR;
        tagv = ConstString(firstParam.GetString(), _pool);
    } else if (funcName == KHRONOS_UDF_WILDCARD) {
        tagvMatchType = SearchInterface::WILD_CARD;
        tagv = ConstString(firstParam.GetString(), _pool);
    } else if (funcName == KHRONOS_UDF_IWILDCARD) {
        tagvMatchType = SearchInterface::WILD_CARD_I;
        tagv = ConstString(firstParam.GetString(), _pool);
    } else if (funcName == KHRONOS_UDF_REGEXP) {
        tagvMatchType = SearchInterface::REGEX;
        tagv = ConstString(firstParam.GetString(), _pool);
    } else {
        setErrorInfo("UDF [%s] is not supported for tagk=[%s] tagv=[%s]",
                     funcName.c_str(), tagk.c_str(), firstParam.GetString());
        return;
    }
    _tagKVInfos.push_back(std::make_pair(ConstString(tagk.c_str(), _pool),
                    SearchInterface::TagvMatchInfo(tagv, tagvMatchType)));
}

END_HA3_NAMESPACE(sql);
