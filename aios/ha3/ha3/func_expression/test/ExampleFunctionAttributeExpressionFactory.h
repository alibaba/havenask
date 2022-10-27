#ifndef SUEZ_TURING_EXAMPLEFUNCTIONATTRIBUTEEXPRESSIONFACTORY_H_
#define SUEZ_TURING_EXAMPLEFUNCTIONATTRIBUTEEXPRESSIONFACTORY_H_

#include <suez/turing/expression/common.h>
#include <suez/turing/expression/function/SyntaxExpressionFactory.h>
#include <suez/config/ResourceReader.h>

namespace suez {
namespace turing {

class ExampleFunctionAttributeExpressionFactory : public SyntaxExpressionFactory
{
public:
    ExampleFunctionAttributeExpressionFactory();
    ~ExampleFunctionAttributeExpressionFactory();
public:
    /* override */ bool registeFunctions();

public:
    void setFuncInfoMap(const FuncInfoMap &funcProtoInfoMap) {
        _funcProtoInfoMap = funcProtoInfoMap;
    }
private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(ExampleFunctionAttributeExpressionFactory);

}
}


#endif //SUEZ_TURING_EXAMPLEFUNCTIONATTRIBUTEEXPRESSIONFACTORY_H_
