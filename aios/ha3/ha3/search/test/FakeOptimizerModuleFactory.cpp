#include <ha3/search/test/FakeOptimizerModuleFactory.h>

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, FakeOptimizerModuleFactory);

Optimizer* FakeOptimizerModuleFactory::createOptimizer(const std::string &optimizerName,
        const OptimizerInitParam &param) 
{
    if (optimizerName != "invalidName") {
        return new FakeOptimizer(optimizerName);
    }
    return NULL;
}

extern "C"
util::ModuleFactory* createFactory_Optimizer() {
    return new FakeOptimizerModuleFactory();
}

extern "C"
void destroyFactory_Optimizer(util::ModuleFactory *factory) {
    delete factory;
}

END_HA3_NAMESPACE(search);
