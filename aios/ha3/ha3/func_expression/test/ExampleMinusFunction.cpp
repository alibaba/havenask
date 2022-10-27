#include <ha3/func_expression/test/ExampleMinusFunction.h>

using namespace std;

namespace suez {
namespace turing {


FunctionInterface *ExampleMinusFunctionCreatorImpl::createFunction(
        const FunctionSubExprVec& funcSubExprVec)
{
    if (funcSubExprVec.size() != 2) {
        return NULL;
    }
    bool isMulti = funcSubExprVec[0]->isMultiValue();
    if (isMulti) {
        return NULL;
    }
    auto vt = funcSubExprVec[0]->getType();
#define CREATE_MINUS_FUNCTION_INTERFACE(type)                           \
    {                                                                   \
        case type:                                                      \
            typedef VariableTypeTraits<type, false>::AttrExprType T;    \
            AttributeExpressionTyped<T> *minuendExpr = dynamic_cast<AttributeExpressionTyped<T>*>(funcSubExprVec[0]); \
            if (!minuendExpr) {                                         \
                return NULL;                                            \
            }                                                           \
            AttributeExpressionTyped<T> *subtrahendExpr = dynamic_cast<AttributeExpressionTyped<T>*>(funcSubExprVec[1]); \
            if (!subtrahendExpr) {                                      \
                return NULL;                                            \
            }                                                           \
            return new ExampleMinusFunction<T>(minuendExpr, subtrahendExpr); \
    }

    switch (vt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER(CREATE_MINUS_FUNCTION_INTERFACE);
    default:
        return NULL;
    }
#undef CREATE_MINUS_FUNCTION_INTERFACE
    return NULL;
}
}
}

