#ifndef ISEARCH_SCORERSAMPLE_H
#define ISEARCH_SCORERSAMPLE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/plugin/Scorer.h>
#include <ha3/rank/ScoringProvider.h>
#include <matchdoc/MatchDoc.h>
#include <string>
#include <ha3/common/Tracer.h>

BEGIN_HA3_NAMESPACE(rank);

class RecordInfoScorer : public suez::turing::Scorer
{
public:
    RecordInfoScorer(const std::string &name = DEFAULT_DEBUG_SCORER) : Scorer(name){
    }
    virtual ~RecordInfoScorer() {}
public:
    suez::turing::Scorer* clone() override;
    bool beginRequest(suez::turing::ScoringProvider *provider) override;
    score_t score(matchdoc::MatchDoc &matchDoc) override;
    void endRequest() override;
    void destroy() override;
protected:
    TRACE_DECLARE();
private:
    rank::MetaInfo _metaInfo;
//    TermVector _termVector;
    const matchdoc::Reference<MatchData> *_matchDataRef;

    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(rank);

#endif
