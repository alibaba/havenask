#ifndef ISEARCH_FAKEFULLCACHESCORER_H
#define ISEARCH_FAKEFULLCACHESCORER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/Scorer.h>
#include <suez/turing/expression/plugin/ScorerModuleFactory.h>

BEGIN_HA3_NAMESPACE(search);

class FakeFullCacheScorer : public rank::Scorer
{
public:
    FakeFullCacheScorer(const std::string &name);
    ~FakeFullCacheScorer();
public:
    rank::Scorer* clone() { return new FakeFullCacheScorer(*this); }
    bool beginRequest(suez::turing::ScoringProvider *provider) { return true; }
    void endRequest() { }
    score_t score(matchdoc::MatchDoc &matchDoc);
    void destroy() {delete this;}
    
    void setSleepTime(int32_t sleepTime) {_sleepTime = sleepTime;}
private:
    int32_t _sleepTime; // ms
private:
    HA3_LOG_DECLARE();
};

class FakeFullCacheScorerModuleFactory: public suez::turing::ScorerModuleFactory {
public:
    FakeFullCacheScorerModuleFactory() {}
    ~FakeFullCacheScorerModuleFactory() {}
public:
    bool init(const KeyValueMap &parameters);
    suez::turing::Scorer* createScorer(const char *scorerName,
                               const KeyValueMap &scorerParameters,
                               suez::ResourceReader *resourceReader,
                               suez::turing::CavaPluginManager *cavaPluginManager);
    void destroy() { };
};

HA3_TYPEDEF_PTR(FakeFullCacheScorer);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_FAKEFULLCACHESCORER_H
