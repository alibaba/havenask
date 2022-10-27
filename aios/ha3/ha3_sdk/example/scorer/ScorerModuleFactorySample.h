#ifndef ISEARCH_SCORERMODULEFACTORYSAMPLE_H
#define ISEARCH_SCORERMODULEFACTORYSAMPLE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/ScorerModuleFactory.h>
#include <string>
#include <ha3/rank/Scorer.h>

BEGIN_HA3_NAMESPACE(rank);

class ScorerModuleFactorySample : public ScorerModuleFactory
{
public:
    ScorerModuleFactorySample();
    virtual ~ScorerModuleFactorySample();
public:
    virtual bool init(const KeyValueMap &parameters);
    virtual Scorer* createScorer(const char *scorerName,
                                 const KeyValueMap &scorerParameters,
                                 config::ResourceReader * resourceReader,
                                 suez::turing::CavaPluginManager *cavaPluginManager);
    virtual void destroy() { };
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_SCORERMODULEFACTORYSAMPLE_H
