#include <ha3/search/test/FakeModuleFactory.h>
#include <ha3/search/test/FakePhaseScorer.h>
#include <build_service/plugin/ModuleFactory.h>
#include <ha3/rank/Scorer.h>

using namespace std;

using namespace build_service::plugin;
USE_HA3_NAMESPACE(rank);
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, FakeModuleFactory);

FakeModuleFactory::FakeModuleFactory() { 
}

FakeModuleFactory::~FakeModuleFactory() { 
}
bool FakeModuleFactory::init(const KeyValueMap &parameters) {
    return true;
}

void FakeModuleFactory::destroy() {
    
}

Scorer* FakeModuleFactory::createScorer(const char *scorerName, 
                                        const KeyValueMap &scorerParameters,
                                        suez::ResourceReader *resourceReader,
                                        suez::turing::CavaPluginManager *cavaPluginManager) 
{
    string id;
    if (strlen(scorerName) > 7) {
        id = string(scorerName + 7);
    } else {
        id = scorerName;
    }
    return new FakePhaseScorer(id);
}

extern "C" 
ModuleFactory* createFactory() {
    return new FakeModuleFactory;
}

extern "C" 
void destroyFactory(ModuleFactory *factory) {
    delete factory;
}

END_HA3_NAMESPACE(search);
