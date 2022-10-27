#ifndef ISEARCH_FAKEMODULEFACTORY_H
#define ISEARCH_FAKEMODULEFACTORY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/plugin/ScorerModuleFactory.h>
#include <ha3/rank/Scorer.h>

BEGIN_HA3_NAMESPACE(search);

class FakeModuleFactory : public suez::turing::ScorerModuleFactory
{
public:
    FakeModuleFactory();
    ~FakeModuleFactory();
public:
    bool init(const KeyValueMap &parameters);
    void destroy();
    rank::Scorer* createScorer(const char *scorerName, 
                               const KeyValueMap &scorerParameters,
                               suez::ResourceReader *resourceReader,
                               suez::turing::CavaPluginManager *cavaPluginManager);
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_FAKEMODULEFACTORY_H
