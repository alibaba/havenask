#include <common/SummaryModuleFactorySample.h>

USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(summary);
namespace pluginplatform {
namespace summary_plugins {
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

}}

