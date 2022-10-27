#ifndef ISEARCH_FUNCTIONINTERFACE_H
#define ISEARCH_FUNCTIONINTERFACE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/function/FunctionInterface.h>

BEGIN_HA3_NAMESPACE(func_expression);

typedef suez::turing::FunctionInterface FunctionInterface; 
typedef std::vector<suez::turing::AttributeExpression *> FunctionSubExprVec;

template<typename T>
using FunctionInterfaceTyped = suez::turing::FunctionInterfaceTyped<T>;

END_HA3_NAMESPACE(func_expression);

#endif //ISEARCH_FUNCTIONINTERFACE_H
