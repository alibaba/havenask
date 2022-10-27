#ifndef ISEARCH_NOTEXPRESSIONWRAPPER_H
#define ISEARCH_NOTEXPRESSIONWRAPPER_H

#include <ha3/sql/common/common.h>
#include <suez/turing/expression/framework/AttributeExpression.h>

BEGIN_HA3_NAMESPACE(sql);

class NotExpressionWrapper : public suez::turing::AttributeExpressionTyped<bool>
{
public:
    NotExpressionWrapper(suez::turing::AttributeExpressionTyped<bool> *expression);
    ~NotExpressionWrapper();
private:
    NotExpressionWrapper(const NotExpressionWrapper &);
    NotExpressionWrapper& operator=(const NotExpressionWrapper &);
public:
    bool evaluateAndReturn(matchdoc::MatchDoc matchDoc) override;

private:
    suez::turing::AttributeExpressionTyped<bool> *_expression;
};

HA3_TYPEDEF_PTR(NotExpressionWrapper);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_NOTEXPRESSIONWRAPPER_H
