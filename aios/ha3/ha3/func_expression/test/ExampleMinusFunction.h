#ifndef SUEZ_TURING_EXAMPLEMINUSFUNCTION_H_
#define SUEZ_TURING_EXAMPLEMINUSFUNCTION_H_

#include <suez/turing/expression/common.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <suez/turing/expression/function/FunctionCreator.h>
#include <matchdoc/MatchDoc.h>

namespace suez {
namespace turing {

template<typename T>
class ExampleMinusFunction : public FunctionInterfaceTyped<T>
{
public:
    ExampleMinusFunction(AttributeExpressionTyped<T> *minuendExpr,
                         AttributeExpressionTyped<T> *subtrahendExpr);
    ~ExampleMinusFunction();
public:
    bool beginRequest(suez::turing::FunctionProvider *provider) override {
        return true;
    }
    T evaluate(matchdoc::MatchDoc matchDoc) override;
private:
    AttributeExpressionTyped<T> *_minuendExpr;
    AttributeExpressionTyped<T> *_subtrahendExpr;
private:
    AUTIL_LOG_DECLARE();
};

DECLARE_UNKNOWN_TYPE_FUNCTION_CREATOR(ExampleMinusFunction, "minus", 2);
class ExampleMinusFunctionCreatorImpl : public FUNCTION_CREATOR_CLASS_NAME(ExampleMinusFunction) {
public:
    FunctionInterface *createFunction(const FunctionSubExprVec& funcSubExprVec) override;
};

//////////////////////////////////////////////////////////////////

template<typename T>
ExampleMinusFunction<T>::ExampleMinusFunction(AttributeExpressionTyped<T> *minuendExpr,
        AttributeExpressionTyped<T> *subtrahendExpr)
    : _minuendExpr(minuendExpr)
    , _subtrahendExpr(subtrahendExpr)
{ 
}

template<typename T>
ExampleMinusFunction<T>::~ExampleMinusFunction() { 
}

template<typename T>
T ExampleMinusFunction<T>::evaluate(matchdoc::MatchDoc matchDoc) {
    assert(matchdoc::INVALID_MATCHDOC != matchDoc);

    // don't known how to deal with return false.
    (void)_minuendExpr->evaluate(matchDoc);
    (void)_subtrahendExpr->evaluate(matchDoc);
    return _minuendExpr->getValue(matchDoc) - _subtrahendExpr->getValue(matchDoc);
}

}
}


#endif //SUEZ_TURING_EXAMPLEMINUSFUNCTION_H_
