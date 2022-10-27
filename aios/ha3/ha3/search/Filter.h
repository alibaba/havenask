#ifndef ISEARCH_FILTER_H
#define ISEARCH_FILTER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/common/FilterClause.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <suez/turing/expression/framework/AttributeExpressionCreatorBase.h>

BEGIN_HA3_NAMESPACE(search);

class Filter
{
public:
    Filter(suez::turing::AttributeExpressionTyped<bool> *attributeExpr);
    ~Filter();
private:
    Filter(const Filter &filter);
public:
    inline bool pass(matchdoc::MatchDoc doc);

    inline bool needFilterSubDoc() const {
        return _attributeExpr->isSubExpression();
    }

    suez::turing::AttributeExpression* getAttributeExpression() const {
        return _attributeExpr;
    }
    void setAttributeExpression(suez::turing::AttributeExpressionTyped<bool> *attributeExpr);
    
    void updateExprEvaluatedStatus();
public:
    static Filter *createFilter(
            const common::FilterClause *filterClause,
            suez::turing::AttributeExpressionCreatorBase *attributeExpressionCreator,
            autil::mem_pool::Pool *pool);

private:
    suez::turing::AttributeExpressionTyped<bool> *_attributeExpr;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(Filter);

inline bool Filter::pass(matchdoc::MatchDoc doc) {
    assert(_attributeExpr);
    return _attributeExpr->evaluateAndReturn(doc);
}

END_HA3_NAMESPACE(search);

#endif //ISEARCH_FILTER_H
