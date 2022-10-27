#include <ha3/rank/test/FakeScorerModuleFactory.h>
#include <ha3/rank/test/StringAttributeScorer.h>
#include <build_service/plugin/ModuleFactory.h>

#include <string.h>
#include <assert.h>

using namespace std;
USE_HA3_NAMESPACE(search);
using namespace build_service::plugin;
BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, FakeScorer);

FakeScorer::~FakeScorer() { 
}

Scorer* FakeScorer::clone() {
    return new FakeScorer(*this);
}

bool  FakeScorer::beginRequest(suez::turing::ScoringProvider *provider){
    return true;
}
score_t FakeScorer::score(matchdoc::MatchDoc &matchDoc) {
    return 1.0f;
}
void FakeScorer::endRequest() {
}

void FakeScorer::destroy() {
    delete this;
}
//////////////////////////////////
HA3_LOG_SETUP(rank, FakeDefaultScorer);
FakeDefaultScorer::FakeDefaultScorer(const std::string &name) 
    : Scorer(name)
{
    _ref0 = NULL;
    _ref1 = NULL;
    _ref2 = NULL;
    _globalVal1 = NULL;
//    HA3_LOG(TRACE3, "Construct this: [%p], vtable:(%p)", this,*((void**)this));
}

Scorer* FakeDefaultScorer::clone() {
    return new FakeDefaultScorer(*this);
}

FakeDefaultScorer::~FakeDefaultScorer() { 
    HA3_LOG(TRACE3, "Destruction function. this: [%p]", this);
}

bool FakeDefaultScorer::beginRequest(suez::turing::ScoringProvider *provider_){
    auto provider = dynamic_cast<ScoringProvider *>(provider_);
    if (provider) {
        _ref0 = provider->declareVariable<double>("result");
        _ref1 = provider->requireAttribute<int32_t>("uid");
        _globalVal1 = provider->declareGlobalVariable<int32_t>("global_val1", true);
        _ref2 = provider->requireMatchData();
        *_globalVal1 = 0;
    }
    return true;
}
score_t FakeDefaultScorer::score(matchdoc::MatchDoc &matchDoc) {
    if (matchdoc::INVALID_MATCHDOC == matchDoc) {
        return 1.0f;
    }
    matchdoc::MatchDoc slab = matchDoc;
    assert(matchdoc::INVALID_MATCHDOC != slab);
    (void) slab;
    (*_globalVal1) = (*_globalVal1) + 1;
    return (score_t)matchDoc.getDocId();
}
void FakeDefaultScorer::endRequest() {
}

void FakeDefaultScorer::destroy() {
    delete this;
}
///////////////////////////////////
HA3_LOG_SETUP(rank, FakeScorerModuleFactory);
FakeScorerModuleFactory::FakeScorerModuleFactory() { 
}

FakeScorerModuleFactory::~FakeScorerModuleFactory() { 
}

bool FakeScorerModuleFactory::init(const KeyValueMap &factoryParameters) {
    return true;
}

Scorer* FakeScorerModuleFactory::createScorer(const char *scorerName,
        const KeyValueMap &scorerParameters,
        suez::ResourceReader* resourceReader,
        suez::turing::CavaPluginManager *cavaPluginManager)
{
    Scorer* scorer = NULL;
    if (0 == strcmp(scorerName, "FakeScorer")) {
        scorer = new FakeScorer(scorerName);
    } else if (0 == strcmp(scorerName, "FakeDefaultScorer")) {
        scorer = new FakeDefaultScorer(scorerName);
    } else if (0 == strcmp(scorerName, "StringAttributeScorer")) {
        scorer = new StringAttributeScorer(scorerName);
    }
    return scorer;
}

extern "C" 
ModuleFactory* createFactory() {
    return new FakeScorerModuleFactory;
}

extern "C" 
void destroyFactory(ModuleFactory *factory) {
    delete factory;
}


END_HA3_NAMESPACE(rank);

