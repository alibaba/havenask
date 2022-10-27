#ifndef ISEARCH_FAKESCORERMODULEFACTORY_H
#define ISEARCH_FAKESCORERMODULEFACTORY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <build_service/plugin/ModuleFactory.h>
#include <suez/turing/expression/plugin/ScorerModuleFactory.h>
#include <string>
#include <ha3/rank/Scorer.h>
#include <matchdoc/MatchDocAllocator.h>
#include <ha3/common/Request.h>
#include <ha3/rank/MatchData.h>
#include <ha3/config/ResourceReader.h>

BEGIN_HA3_NAMESPACE(rank);

class FakeScorer : public Scorer {
public:
    FakeScorer(const std::string &name = "FakeScorer") : Scorer(name) {}
    Scorer* clone();
    virtual ~FakeScorer();
public:
    virtual bool beginRequest(suez::turing::ScoringProvider *provider);
    virtual score_t score(matchdoc::MatchDoc &matchDoc);
    virtual void endRequest();
    virtual void destroy();
private:
    HA3_LOG_DECLARE();
};

class FakeDefaultScorer : public Scorer {
public:
    FakeDefaultScorer(const std::string &name = "FakeDefaultScorer");
    Scorer* clone();
    virtual ~FakeDefaultScorer();
public:

    virtual bool beginRequest(suez::turing::ScoringProvider *provider);
    virtual score_t score(matchdoc::MatchDoc &matchDoc);
    virtual void endRequest();
    virtual void destroy();
private:
    const matchdoc::Reference<double>* _ref0;
    const matchdoc::Reference<int32_t>* _ref1;
    const matchdoc::Reference<MatchData>* _ref2;
    int32_t *_globalVal1;
    int32_t _scoretime;
private:
    HA3_LOG_DECLARE();
};


class FakeScorerModuleFactory : public suez::turing::ScorerModuleFactory
{
public:
    FakeScorerModuleFactory();
    ~FakeScorerModuleFactory();
public:
    virtual bool init(const KeyValueMap &parameters);
    virtual void dummy() {}
    virtual Scorer* createScorer(const char *scorerName,
                                 const KeyValueMap &scorerParameters,
                                 config::ResourceReader *resourceReader,
                                 suez::turing::CavaPluginManager *cavaPluginManager);
    virtual void destroy() { };
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_FAKESCORERMODULEFACTORY_H
