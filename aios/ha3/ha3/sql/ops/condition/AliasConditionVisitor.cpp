#include <ha3/sql/ops/condition/AliasConditionVisitor.h>
#include <ha3/sql/ops/condition/ExprUtil.h>
#include <memory>

using namespace std;
using namespace autil;
using namespace autil_rapidjson;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(sql);

AliasConditionVisitor::AliasConditionVisitor() {
}

AliasConditionVisitor::~AliasConditionVisitor() {
}

void AliasConditionVisitor::visitAndCondition(AndCondition *condition) {
    if (isError()) {
        return;
    }
    vector<ConditionPtr> children = condition->getChildCondition();
    for (size_t i = 0; i < children.size(); i++) {
        children[i]->accept(this);
    }
}

void AliasConditionVisitor::visitOrCondition(OrCondition *condition) {
    if (isError()) {
        return;
    }
    vector<ConditionPtr> children = condition->getChildCondition();
    for (size_t i = 0; i < children.size(); i++) {
        children[i]->accept(this);
    }
}

void AliasConditionVisitor::visitNotCondition(NotCondition *condition) {
    if (isError()) {
        return;
    }
    vector<ConditionPtr> children = condition->getChildCondition();
    assert(children.size() == 1);
    children[0]->accept(this);
}

void AliasConditionVisitor::visitLeafCondition(LeafCondition *condition) {
    if (isError()) {
        return;
    }
    const SimpleValue &leafCondition = condition->getCondition();
    bool hasError = isError();
    string errorInfo;
    string exprStr = ExprUtil::toExprString(leafCondition, hasError, errorInfo, _aliasMap);
    if (hasError) {
        setErrorInfo(errorInfo);
        return;
    }
}


END_HA3_NAMESPACE(sql);
