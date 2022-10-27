#ifndef ISEARCH_SYNTAXEXPRESSIONFACTORY_H_
#define ISEARCH_SYNTAXEXPRESSIONFACTORY_H_

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/ModuleFactory.h>
#include <ha3/config/ResourceReader.h>
#include <ha3/search/AttributeExpression.h>
#include <ha3/func_expression/FunctionInterface.h>
#include <ha3/func_expression/FunctionCreator.h>
#include <ha3/config/FunctionMap.h>
#include <suez/turing/expression/function/SyntaxExpressionFactory.h>

BEGIN_HA3_NAMESPACE(func_expression);

typedef suez::turing::SyntaxExpressionFactory SyntaxExpressionFactory; 

END_HA3_NAMESPACE(func_expression);

#endif //ISEARCH_SYNTAXEXPRESSIONFACTORY_H
