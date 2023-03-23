#ifndef UDF_PLUGINS_SUMFUNCINTERFACE_H
#define UDF_PLUGINS_SUMFUNCINTERFACE_H
 
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/func_expression/FunctionInterface.h>
#include <ha3/search/AttributeExpression.h>
#include <ha3/func_expression/FunctionCreator.h>

namespace pluginplatform {
namespace udf_plugins {
 
template<typename T>
class SumFuncInterface : public isearch::func_expression::FunctionInterfaceTyped<T>
{
public:
    SumFuncInterface(std::vector<isearch::search::AttributeExpressionTyped<T>*> *pAttrVec)
        : _pAttrVec(pAttrVec)
    {
    }
    ~SumFuncInterface() {
        if (_pAttrVec) {
            delete _pAttrVec;
            _pAttrVec = NULL;
        }
    }
public:
    bool beginRequest(suez::turing::FunctionProvider *provider) {
        return true;
    }
    T evaluate(matchdoc::MatchDoc matchDoc) {
        T value = T();
        for (size_t i = 0; i < _pAttrVec->size(); ++i) {
            (void)(*_pAttrVec)[i]->evaluate(matchDoc);
            value += (*_pAttrVec)[i]->getValue(matchDoc);
        }
        return value;
    }
private:
    std::vector<isearch::search::AttributeExpressionTyped<T>*> *_pAttrVec;
private:
    HA3_LOG_DECLARE();
};
 
DECLARE_UNKNOWN_TYPE_FUNCTION_CREATOR(SumFuncInterface, "sumAll", suez::turing::FUNCTION_UNLIMITED_ARGUMENT_COUNT);
 
class SumFuncInterfaceCreatorImpl : public FUNCTION_CREATOR_CLASS_NAME(SumFuncInterface) {
public:
    /* override */ isearch::func_expression::FunctionInterface *createFunction(
            const isearch::func_expression::FunctionSubExprVec& funcSubExprVec);
};
 
}}
 
#endif //UDF_PLUGINS_SUMFUNCINTERFACE_H