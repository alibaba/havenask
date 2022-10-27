#include <ha3/sql/ops/calc/CalcConditionVisitor.h>
#include <suez/turing/expression/framework/BinaryAttributeExpression.h>
#include <ha3/sql/ops/scan/NotExpressionWrapper.h>
#include <autil/legacy/RapidJsonHelper.h>
#include <ha3/sql/ops/util/SqlJsonUtil.h>
#include <autil/legacy/RapidJsonCommon.h>
#include <ha3/sql/ops/condition/ExprUtil.h>
#include <memory>

using namespace std;
using namespace autil;
using namespace autil_rapidjson;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(sql);

CalcConditionVisitor::CalcConditionVisitor(
        suez::turing::MatchDocsExpressionCreator *attrExprCreator,
        autil::mem_pool::Pool *pool)
    : _attrExpr(NULL)
    , _attrExprCreator(attrExprCreator)
    , _pool(pool)
{}

CalcConditionVisitor::~CalcConditionVisitor() {
    _attrExpr = NULL; // delete by attribute expression pool
}

void CalcConditionVisitor::visitAndCondition(AndCondition *condition) {
    if (isError()) {
        return;
    }
    vector<ConditionPtr> children = condition->getChildCondition();
    vector<AttributeExpression*> childExpr;
    for (size_t i = 0; i < children.size(); i++) {
        children[i]->accept(this);
        if (getAttributeExpresson()) {
            childExpr.push_back(stealAttributeExpression());
        }
        if (isError()) {
            break;
        }
    }
    if (isError()) {
        return;
    }
    if (childExpr.size() == 1) {
        _attrExpr = childExpr[0];
    } else if (childExpr.size() > 1) {
        AttributeExpression *leftExpr = childExpr[0];
        AttributeExpression *rightExpr = NULL;
        typedef BinaryAttributeExpression<std::logical_and, bool> AndAttrExpr;
        for (size_t i = 1; i < childExpr.size() ; i++ ) {
            rightExpr = childExpr[i];
            string oriString = leftExpr->getOriginalString() + " AND " + rightExpr->getOriginalString();
            leftExpr = POOL_NEW_CLASS(_pool, AndAttrExpr, leftExpr, rightExpr);
            leftExpr->setOriginalString(oriString);
            _attrExprCreator->addNeedDeleteExpr(leftExpr);
        }
        _attrExpr = leftExpr;
    }
}

void CalcConditionVisitor::visitOrCondition(OrCondition *condition) {
    if (isError()) {
        return;
    }
    vector<ConditionPtr> children = condition->getChildCondition();
    vector<AttributeExpression*> childExpr;
    for (size_t i = 0; i < children.size(); i++) {
        children[i]->accept(this);
        if (getAttributeExpresson() != NULL) {
            childExpr.push_back(stealAttributeExpression());
        }
        if (isError()) {
            break;
        }
    }
    if (isError()) {
        return;
    }
    if (childExpr.size() == 1) {
        _attrExpr = childExpr[0];
    } else if (childExpr.size() > 1) {
        AttributeExpression *leftExpr = childExpr[0];
        AttributeExpression *rightExpr = NULL;
        typedef BinaryAttributeExpression<std::logical_or, bool> OrAttrExpr;
        for (size_t i = 1; i < childExpr.size() ; i++ ) {
            rightExpr = childExpr[i];
            string oriString = "(" + leftExpr->getOriginalString() + " OR " + rightExpr->getOriginalString() + ")";
            leftExpr = POOL_NEW_CLASS(_pool, OrAttrExpr, leftExpr, rightExpr);
            leftExpr->setOriginalString(oriString);
            _attrExprCreator->addNeedDeleteExpr(leftExpr);
        }
        _attrExpr = leftExpr;
    }
}

void CalcConditionVisitor::visitNotCondition(NotCondition *condition) {
    if (isError()) {
        return;
    }
    vector<ConditionPtr> children = condition->getChildCondition();
    assert(children.size() == 1);
    children[0]->accept(this);
    AttributeExpression* childExpr = stealAttributeExpression();
    if (isError()) {
        return;
    }
    if (childExpr != NULL) {
        AttributeExpressionTyped<bool>* childExprTyped = dynamic_cast<AttributeExpressionTyped<bool>* >(childExpr);
        if (childExprTyped == NULL) {
            string errorInfo = "dynamic cast expr[" + childExpr->getOriginalString() + "] to bool expression failed.";
            setErrorInfo(errorInfo);
            return;
        }
        _attrExpr = POOL_NEW_CLASS(_pool, NotExpressionWrapper, childExprTyped);
        string oriString = "not (" + childExprTyped->getOriginalString() + ")";
        _attrExpr->setOriginalString(oriString);
        _attrExprCreator->addNeedDeleteExpr(_attrExpr);
    }
}

void CalcConditionVisitor::visitLeafCondition(LeafCondition *condition) {
    if (isError()) {
        return;
    }
    const SimpleValue &leafCondition = condition->getCondition();
    bool hasError = isError();
    string errorInfo;
    std::map<std::string, std::string> renameMap;
    if (ExprUtil::isCaseOp(leafCondition)) {
        _attrExpr = ExprUtil::CreateCaseExpression(_attrExprCreator, "CONDITION_BOOLEAN",
                                                   leafCondition, hasError, errorInfo, renameMap, _pool);
        if (!_attrExpr) {
            SQL_LOG(WARN, "%s", errorInfo.c_str());
            setErrorInfo(errorInfo);
        } else {
            SQL_LOG(TRACE1, "create leaf exprStr [case when]");
        }
    } else {
        string exprStr = ExprUtil::toExprString(leafCondition, hasError, errorInfo, renameMap);
        if (hasError) {
            setErrorInfo(errorInfo);
            return;
        }
        _attrExpr = _attrExprCreator->createAttributeExpression(exprStr);
        if (!_attrExpr) {
            string errorInfo = "parse expression string ["+ exprStr +"] failed.";
            SQL_LOG(WARN, "%s", errorInfo.c_str());
            setErrorInfo(errorInfo);
        } else {
            SQL_LOG(TRACE1, "create leaf exprStr [%s]", exprStr.c_str());
        }
    }
}

suez::turing::AttributeExpression* CalcConditionVisitor::stealAttributeExpression() {
    suez::turing::AttributeExpression *attrExpr = _attrExpr;
    _attrExpr = NULL;
    return attrExpr;
}

const suez::turing::AttributeExpression* CalcConditionVisitor::getAttributeExpresson() const {
    return _attrExpr;
}

END_HA3_NAMESPACE(sql);
