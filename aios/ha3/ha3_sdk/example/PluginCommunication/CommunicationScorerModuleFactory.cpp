#include "CommunicationScorerModuleFactory.h"
#include "CommunicationScorer.h"
#include <string.h>
#include <assert.h>

using namespace std;
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(rank);
//LOG_SETUP(rank, FakeScorer);

HA3_LOG_SETUP(rank, CommunicationScorerModuleFactory);

CommunicationScorerModuleFactory::CommunicationScorerModuleFactory() { 
}

CommunicationScorerModuleFactory::~CommunicationScorerModuleFactory() { 
}

bool CommunicationScorerModuleFactory::init(const KeyValueMap &factoryParameters) {
    return true;
}

Scorer* CommunicationScorerModuleFactory::createScorer(const char *scorerName,
        const KeyValueMap &scorerParameters,
        config::ResourceReader * resourceReader,
        suez::turing::CavaPluginManager *cavaPluginManager)
{
    if (0==strcmp(scorerName, "CommunicationScorer")) {
        return new CommunicationScorer(scorerName);
    }
    return NULL;
}

extern "C" 
ModuleFactory* createFactory() {
    return new CommunicationScorerModuleFactory;
}

extern "C" 
void destroyFactory(ModuleFactory *factory) {
    delete factory;
}


END_HA3_NAMESPACE(rank);

