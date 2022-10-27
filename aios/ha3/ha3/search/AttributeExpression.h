#ifndef ISEARCH_ATTRIBUTEEXPRESSION_H
#define ISEARCH_ATTRIBUTEEXPRESSION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <suez/turing/expression/framework/AttributeExpression.h>

BEGIN_HA3_NAMESPACE(search);

typedef suez::turing::ExpressionType ExpressionType; 
typedef suez::turing::AttributeExpression AttributeExpression; 

typedef std::tr1::shared_ptr<AttributeExpression> AttributeExpressionPtr;
typedef std::vector<AttributeExpression*> ExpressionVector;
typedef std::set<AttributeExpression*> ExpressionSet;

template<typename T>
using AttributeExpressionTyped = suez::turing::AttributeExpressionTyped<T>;

END_HA3_NAMESPACE(search);

#endif //ISEARCH_ATTRIBUTEEXPRESSION_H
