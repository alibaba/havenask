#include "SumFuncInterface.h"
#include <ha3/common/VariableTypeTraits.h>
 
using namespace std;
using namespace suez::turing;
 
USE_HA3_NAMESPACE(search);
 
namespace pluginplatform {
namespace udf_plugins {
 
FunctionInterface *SumFuncInterfaceCreatorImpl::createFunction(
        const FunctionSubExprVec& funcSubExprVec)
{
    if (funcSubExprVec.size() == 0) {
        return NULL;
    }
    if (funcSubExprVec[0]->isMultiValue()) {
        return NULL;
    }
    VariableType vt = funcSubExprVec[0]->getType();
#define CREATE_SUM_FUNCTION_INTERFACE(type)                             \
    case type:                                                          \
    {                                                                   \
        typedef suez::turing::VariableTypeTraits<type, false>::AttrExprType T; \
        auto pAttrs = new vector<suez::turing::AttributeExpressionTyped<T>*>; \
        for (FunctionSubExprVec::const_iterator it = funcSubExprVec.begin(); \
             it != funcSubExprVec.end(); ++it)                          \
        {                                                               \
            pAttrs->push_back(dynamic_cast<suez::turing::AttributeExpressionTyped<T>*>(*it)); \
        }                                                               \
        return new SumFuncInterface<T>(pAttrs);                         \
    }
 
    switch(vt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER(CREATE_SUM_FUNCTION_INTERFACE);
    default:
        return NULL;
    }
#undef CREATE_SUM_FUNCTION_INTERFACE
    return NULL;
}
 
}}