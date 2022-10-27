#ifndef SUEZ_TURING_EXAMPLEADDFUNCTION_H_
#define SUEZ_TURING_EXAMPLEADDFUNCTION_H_

#include <suez/turing/expression/common.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <suez/turing/expression/function/FunctionCreator.h>
#include <matchdoc/MatchDoc.h>

namespace suez {
namespace turing {


template<typename T>
class ExampleAddFunction : public FunctionInterfaceTyped<T>
{
public:
    typedef std::vector<AttributeExpressionTyped<T>*> TypedAttrExprVec;
public:
    ExampleAddFunction(TypedAttrExprVec *pExprsTyped);
    ~ExampleAddFunction();
public:
    bool beginRequest(FunctionProvider *dataProvider) override;
    void endRequest() override;
    T evaluate(matchdoc::MatchDoc matchDoc) override;
private:
    TypedAttrExprVec *_exprsTyped;
    T _sum;
    uint32_t _count;
    T *_avg;
private:
    AUTIL_LOG_DECLARE();
};

DECLARE_UNKNOWN_TYPE_FUNCTION_CREATOR(ExampleAddFunction, "add", FUNCTION_UNLIMITED_ARGUMENT_COUNT);
class ExampleAddFunctionCreatorImpl : public FUNCTION_CREATOR_CLASS_NAME(ExampleAddFunction) {
public:
    FunctionInterface *createFunction(
            const FunctionSubExprVec &funcSubExprVec) override;
};

////////////////////////////////////////////////////////////////////

template<typename T>
ExampleAddFunction<T>::ExampleAddFunction(
        std::vector<AttributeExpressionTyped<T> *> *pExprsTyped)
    : _exprsTyped(pExprsTyped)
    , _sum(T())
    , _count(0)
    , _avg(NULL)
{ 
}

template<typename T>
ExampleAddFunction<T>::~ExampleAddFunction() { 
    if (_exprsTyped) {
        delete _exprsTyped;
        _exprsTyped = NULL;
    }
}

template<typename T>
T ExampleAddFunction<T>::evaluate(matchdoc::MatchDoc matchDoc) {
    assert(matchDoc != matchdoc::INVALID_MATCHDOC);
    T resultValue = T();
    for(typename TypedAttrExprVec::const_iterator i = _exprsTyped->begin(); 
        i != _exprsTyped->end(); ++i) 
    {
        assert(*i);
        // don't known how to deal with return false.
        (void)(*i)->evaluate(matchDoc);
        resultValue += (*i)->getValue(matchDoc);
    }
    _sum += resultValue;
    ++_count;
    return resultValue;
}

template<typename T>
bool ExampleAddFunction<T>::beginRequest(FunctionProvider *dataProvider) {
    if (!dataProvider) {
        return false;
    }
    // _avg = dataProvider->declareGlobalVariable<T>("avg", true);
    return true;
}

template<typename T>
void ExampleAddFunction<T>::endRequest() {
    // if (_count) {
    //     (*_avg) = _sum / _count;
    // } else {
    //     (*_avg) = T();
    // }
}

}
}


#endif //SUEZ_TURING_EXAMPLEADDFUNCTION_H_
