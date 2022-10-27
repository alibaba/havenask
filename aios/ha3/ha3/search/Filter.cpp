#include <ha3/search/Filter.h>
#include <suez/turing/expression/framework/AttributeExpressionCreatorBase.h>

USE_HA3_NAMESPACE(common);
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, Filter);

Filter::Filter(AttributeExpressionTyped<bool> *attributeExpr) 
    : _attributeExpr(attributeExpr)
{ 
}

Filter::~Filter() { 
    _attributeExpr = NULL;
}

void Filter::setAttributeExpression(
        AttributeExpressionTyped<bool> *attributeExpr) 
{
    _attributeExpr = attributeExpr;
}

Filter *Filter::createFilter(
        const FilterClause *filterClause,
        AttributeExpressionCreatorBase *attributeExpressionCreator,
        autil::mem_pool::Pool *pool)
{
    if (!filterClause) {
        return NULL;
    }
    AttributeExpression *attrExpr =
        attributeExpressionCreator->createAttributeExpression(
                filterClause->getRootSyntaxExpr());
    if (!attrExpr) {
        HA3_LOG(WARN, "create filter expression[%s] fail.",
                filterClause->getRootSyntaxExpr()->getExprString().c_str());
        return NULL;
    }
    AttributeExpressionTyped<bool> *boolAttrExpr =
        dynamic_cast<AttributeExpressionTyped<bool>*>(attrExpr);
    if (!boolAttrExpr) {
        HA3_LOG(WARN, "filter expression[%s] return type should be bool.",
                filterClause->getRootSyntaxExpr()->getExprString().c_str());
        return NULL;
    }
    return POOL_NEW_CLASS(pool, Filter, boolAttrExpr);
}

void Filter::updateExprEvaluatedStatus() {
    _attributeExpr->setEvaluated();
}

END_HA3_NAMESPACE(search);

