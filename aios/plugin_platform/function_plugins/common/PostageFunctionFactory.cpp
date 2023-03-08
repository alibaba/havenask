#include <common/PostageFunctionFactory.h>
#include <postage_demo/PostageFunction.h>

namespace pluginplatform {
namespace function_plugins {
HA3_LOG_SETUP(function_plugins, PostageFunctionFactory);

PostageFunctionFactory::PostageFunctionFactory() { 
}

PostageFunctionFactory::~PostageFunctionFactory() { 
}

bool PostageFunctionFactory::registeFunctions() {
    REGISTE_FUNCTION_CREATOR(PostageFunctionCreatorImpl);
    return true;
}


extern "C" 
isearch::util::ModuleFactory* createFactory() {
    return new PostageFunctionFactory;
}

extern "C" 
void destroyFactory(isearch::util::ModuleFactory *factory) {
    factory->destroy();
}

}}