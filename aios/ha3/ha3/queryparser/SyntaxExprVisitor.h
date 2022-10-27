#ifndef ISEARCH_SYNTAXEXPRVISITOR_H
#define ISEARCH_SYNTAXEXPRVISITOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/SyntaxExpr.h>
#include <ha3/search/AttributeExpression.h>
#include <suez/turing/expression/syntax/SyntaxExprVisitor.h>

BEGIN_HA3_NAMESPACE(queryparser);

typedef suez::turing::SyntaxExprVisitor SyntaxExprVisitor; 

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_SYNTAXEXPRVISITOR_H
