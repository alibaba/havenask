#include <ha3/search/test/FakeFullCacheScorer.h>

using namespace suez::turing;
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, FakeFullCacheScorer);

FakeFullCacheScorer::FakeFullCacheScorer(const std::string &name) 
    : rank::Scorer(name)
{ 
    _sleepTime = 0;
}

FakeFullCacheScorer::~FakeFullCacheScorer() { 
}

score_t FakeFullCacheScorer::score(matchdoc::MatchDoc &matchDoc) {
    usleep(_sleepTime * 1000);
    return (score_t)1;
}

bool FakeFullCacheScorerModuleFactory::init(
        const KeyValueMap &factoryParameters) 
{
    return true;
}

Scorer* FakeFullCacheScorerModuleFactory::createScorer(
        const char *scorerName,
        const KeyValueMap &scorerParameters, 
        suez::ResourceReader* resourceReader,
        suez::turing::CavaPluginManager *cavaPluginManager)
{
    Scorer* scorer = NULL;
    if (0 == strcmp(scorerName, "FakeFullCacheScorer")) {
        FakeFullCacheScorer *fakeScorer = new FakeFullCacheScorer(scorerName);
        KeyValueMap::const_iterator it = scorerParameters.find("sleep_time");
        if (it != scorerParameters.end()) {
            fakeScorer->setSleepTime(atoi(it->second.c_str()));
        }
        scorer = fakeScorer;
    } 

    return scorer;
}

extern "C" 
build_service::plugin::ModuleFactory* createFactory() {
    return new FakeFullCacheScorerModuleFactory;
}

extern "C" 
void destroyFactory(build_service::plugin::ModuleFactory *factory) {
    delete factory;
}

END_HA3_NAMESPACE(search);

