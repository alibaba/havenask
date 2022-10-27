#include <ha3/sql/ops/condition/OutputFieldsVisitor.h>
#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/util/SqlJsonUtil.h>
#include <ha3/sql/ops/condition/ExprUtil.h>
#include <autil/ConstString.h>
#include <autil/legacy/RapidJsonHelper.h>

using namespace std;
using namespace autil_rapidjson;
using namespace autil;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(search, OutputFieldsVisitor);

OutputFieldsVisitor::OutputFieldsVisitor() {
}

OutputFieldsVisitor::~OutputFieldsVisitor() {
}

void OutputFieldsVisitor::visitParams(const SimpleValue &value) {
    const SimpleValue &params = value[SQL_CONDITION_PARAMETER];
    assert(params.IsArray());
    for (size_t i = 0; i < params.Size(); ++i) {
        visit(params[i]);
        if (isError()) {
            return;
        }
    }
}

void OutputFieldsVisitor::visitCastUdf(const SimpleValue &value) {
    visitParams(value);
}

void OutputFieldsVisitor::visitMultiCastUdf(const SimpleValue &value) {
    visitParams(value);
}

void OutputFieldsVisitor::visitNormalUdf(const SimpleValue &value) {
    visitParams(value);
}

void OutputFieldsVisitor::visitOtherOp(const SimpleValue &value) {
    visitParams(value);
}

void OutputFieldsVisitor::visitItemOp(const SimpleValue &value) {
    string itemName;
    string itemKey;
    if (!ExprUtil::parseItemVariable(value, itemName, itemKey)) {
        setErrorInfo("parse item variable failed: %s",
                     RapidJsonHelper::SimpleValue2Str(value).c_str());
        return;
    }
    _usedFieldsItemSet.insert(make_pair(itemName, itemKey));
}

void OutputFieldsVisitor::visitColumn(const SimpleValue &value) {
    _usedFieldsColumnSet.insert(value.GetString());
}

END_HA3_NAMESPACE(sql);
