#include <ha3_sdk/example/scorer/ScorerModuleFactorySample.h>
#include <ha3_sdk/example/scorer/ScorerSample.h>
#include <string.h>
#include <assert.h>

using namespace std;
using namespace build_service::plugin;

USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(rank);

HA3_LOG_SETUP(rank, ScorerModuleFactorySample);

ScorerModuleFactorySample::ScorerModuleFactorySample() { 
}

ScorerModuleFactorySample::~ScorerModuleFactorySample() { 
}

bool ScorerModuleFactorySample::init(const KeyValueMap &factoryParameters) {
    return true;
}

Scorer* ScorerModuleFactorySample::createScorer(const char *scorerName,
        const KeyValueMap &scorerParameters,
        config::ResourceReader * resourceReader,
        suez::turing::CavaPluginManager *cavaPluginManager)
{
    if (0==strcmp(scorerName, "ScorerSample")) {
        return new ScorerSample(scorerParameters, scorerName);
    }
    return NULL;
}

extern "C" 
ModuleFactory* createFactory() {
    return new ScorerModuleFactorySample;
}

extern "C" 
void destroyFactory(ModuleFactory *factory) {
    delete factory;
}


END_HA3_NAMESPACE(rank);

