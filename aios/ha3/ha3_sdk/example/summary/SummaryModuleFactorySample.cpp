#include <ha3_sdk/example/summary/SummaryModuleFactorySample.h>

USE_HA3_NAMESPACE(util);
BEGIN_HA3_NAMESPACE(summary);
HA3_LOG_SETUP(summary, SummaryModuleFactorySample);

SummaryModuleFactorySample::SummaryModuleFactorySample() { 
}

SummaryModuleFactorySample::~SummaryModuleFactorySample() { 
}

extern "C" 
ModuleFactory* createFactory() {
    return new SummaryModuleFactorySample;
}

extern "C" 
void destroyFactory(ModuleFactory *factory) {
    delete factory;
}

END_HA3_NAMESPACE(summary);

