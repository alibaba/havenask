#include <ha3/func_expression/test/ExampleFunctionAttributeExpressionFactory.h>
#include <ha3/func_expression/test/ExampleAddFunction.h>
#include <ha3/func_expression/test/ExampleMinusFunction.h>

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(func_expression, ExampleFunctionAttributeExpressionFactory);

ExampleFunctionAttributeExpressionFactory::ExampleFunctionAttributeExpressionFactory() { 
}

ExampleFunctionAttributeExpressionFactory::~ExampleFunctionAttributeExpressionFactory() { 
}

bool ExampleFunctionAttributeExpressionFactory::registeFunctions() {
    REGISTE_FUNCTION_CREATOR(ExampleMinusFunctionCreatorImpl);
    REGISTE_FUNCTION_CREATOR(ExampleAddFunctionCreatorImpl);
    return true;
}

extern "C" 
build_service::plugin::ModuleFactory* createFactory() {
    return new ExampleFunctionAttributeExpressionFactory;
}

extern "C" 
void destroyFactory(build_service::plugin::ModuleFactory *factory) {
    factory->destroy();
}

}
}
