#ifndef ISEARCH_DEFAULTSCORER_H
#define ISEARCH_DEFAULTSCORER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/GlobalMatchData.h>
#include <ha3/rank/ScoringProvider.h>
#include <matchdoc/MatchDoc.h>
#include <string>
#include <ha3/common/Tracer.h>
#include <suez/turing/expression/plugin/Scorer.h>

BEGIN_HA3_NAMESPACE(rank);

class DefaultScorer : public suez::turing::Scorer
{
public:
    DefaultScorer(const std::string &name = "DefaultScorer") : Scorer(name){
        _matchDataRef = NULL;
        _traceRefer = NULL;
    }
    DefaultScorer(const DefaultScorer &another)
        : Scorer(another.getScorerName()) {}
    ~DefaultScorer() {}
public:
    suez::turing::Scorer* clone() override;
    bool beginRequest(suez::turing::ScoringProvider *provider) override;
    score_t score(matchdoc::MatchDoc &matchDoc) override;
    void endRequest() override {}
    void destroy() override;
protected:
    TRACE_DECLARE();
private:
    const matchdoc::Reference<MatchData> *_matchDataRef;
    const rank::GlobalMatchData *_globalMatchData;
    rank::MetaInfo _metaInfo;
    std::vector<int> _rankFactors;
    std::vector<int64_t> _rankValues;
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_DEFAULTSCORER_H
