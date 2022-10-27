#include <ha3_sdk/example/functions/PostageFunctionFactory.h>
#include <ha3_sdk/example/functions/PostageFunction.h>

BEGIN_HA3_NAMESPACE(func_expression);
HA3_LOG_SETUP(functions, PostageFunctionFactory);

PostageFunctionFactory::PostageFunctionFactory() { 
}

PostageFunctionFactory::~PostageFunctionFactory() { 
}

bool PostageFunctionFactory::registeFunctions() {
    REGISTE_FUNCTION_CREATOR(PostageFunctionCreatorImpl);
    return true;
}


extern "C" 
util::ModuleFactory* createFactory() {
    return new PostageFunctionFactory;
}

extern "C" 
void destroyFactory(util::ModuleFactory *factory) {
    factory->destroy();
}


END_HA3_NAMESPACE(func_expression);
