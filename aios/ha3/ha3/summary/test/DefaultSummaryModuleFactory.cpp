#include <ha3/summary/test/DefaultSummaryModuleFactory.h>
#include <build_service/plugin/ModuleFactory.h>

using namespace build_service::plugin;
BEGIN_HA3_NAMESPACE(summary);
HA3_LOG_SETUP(summary, DefaultSummaryModuleFactory);

DefaultSummaryModuleFactory::DefaultSummaryModuleFactory() { 
}

DefaultSummaryModuleFactory::~DefaultSummaryModuleFactory() { 
}

extern "C" 
ModuleFactory* createFactory() {
    return new DefaultSummaryModuleFactory;
}

extern "C" 
void destroyFactory(ModuleFactory *factory) {
    delete factory;
}
END_HA3_NAMESPACE(summary);

