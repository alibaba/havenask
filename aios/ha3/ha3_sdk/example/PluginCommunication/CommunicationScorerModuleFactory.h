#ifndef ISEARCH_COMMUNICATIONSCORERMODULEFACTORY_H
#define ISEARCH_COMMUNICATIONSCORERMODULEFACTORY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/ScorerModuleFactory.h>
#include <string>
#include <ha3/rank/Scorer.h>

BEGIN_HA3_NAMESPACE(rank);

class CommunicationScorerModuleFactory : public ScorerModuleFactory
{
public:
    CommunicationScorerModuleFactory();
    virtual ~CommunicationScorerModuleFactory();
public:
    /* override */ bool init(const KeyValueMap &parameters);
    /* override */ Scorer* createScorer(const char *scorerName,
            const KeyValueMap &scorerParameters,
            config::ResourceReader * resourceReader,
            suez::turing::CavaPluginManager *cavaPluginManager);
    /* override */ void destroy() { };
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_COMMUNICATIONSCORERMODULEFACTORY_H
