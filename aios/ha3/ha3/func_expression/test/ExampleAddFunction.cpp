#include <ha3/func_expression/test/ExampleAddFunction.h>

using namespace std;

namespace suez {
namespace turing {

FunctionInterface *ExampleAddFunctionCreatorImpl::createFunction(
        const FunctionSubExprVec &funcSubExprVec)
{
    if (funcSubExprVec.size() == 0) {
        return NULL;
    }
    bool isMulti = funcSubExprVec[0]->isMultiValue();
    if (isMulti) {
        return NULL;
    }
    auto bt = funcSubExprVec[0]->getType();
#define CREATE_ADD_FUNCTION_INTERFACE(type)                             \
    {                                                                   \
        case type:                                                      \
            typedef VariableTypeTraits<type, false>::AttrExprType T;    \
            auto pAttrVec = new ExampleAddFunction<T>::TypedAttrExprVec; \
            for (FunctionSubExprVec::const_iterator it = funcSubExprVec.begin(); \
                 it != funcSubExprVec.end(); ++it)                      \
            {                                                           \
                auto expr = dynamic_cast<AttributeExpressionTyped<T> *>(*it); \
                if (!expr) {                                            \
                    delete pAttrVec;                                    \
                    return NULL;                                        \
                }                                                       \
                pAttrVec->push_back(expr);                              \
            }                                                           \
            return new ExampleAddFunction<T>(pAttrVec);                 \
    }

    switch (bt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER(CREATE_ADD_FUNCTION_INTERFACE);
    default:
        return NULL;
    }
#undef CREATE_ADD_FUNCTION_INTERFACE
    return NULL;
}

}
}

