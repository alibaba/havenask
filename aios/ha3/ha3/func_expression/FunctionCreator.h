#ifndef ISEARCH_FUNCTIONCREATOR_H
#define ISEARCH_FUNCTIONCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/func_expression/FunctionInterface.h>
#include <ha3/func_expression/FunctionProvider.h>
#include <suez/turing/expression/function/FunctionCreator.h>

BEGIN_HA3_NAMESPACE(func_expression);

typedef suez::turing::FunctionCreator FunctionCreator; 

END_HA3_NAMESPACE(func_expression);

#endif //ISEARCH_FUNCTIONCREATOR_H
