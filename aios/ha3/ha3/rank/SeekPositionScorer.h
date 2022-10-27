#ifndef ISEARCH_SEEKPOSITIONSCORER_H
#define ISEARCH_SEEKPOSITIONSCORER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/plugin/Scorer.h>
#include <ha3/rank/ScoringProvider.h>
#include <ha3/rank/TermMatchData.h>
#include <matchdoc/MatchDoc.h>
#include <string>

BEGIN_HA3_NAMESPACE(rank);

class SeekPositionScorer : public suez::turing::Scorer
{
public:
    SeekPositionScorer(const std::string &name = "SeekPositionScorer")
        : Scorer(name) 
    {}

    SeekPositionScorer(const SeekPositionScorer &another)
        : Scorer(another.getScorerName())
    {}

    ~SeekPositionScorer() {}

public:
    suez::turing::Scorer* clone() override;
    bool beginRequest(suez::turing::ScoringProvider *provider) override;
    score_t score(matchdoc::MatchDoc &matchDoc) override;
    void endRequest() override;
    void destroy() override;

private:
    bool seekPosition(const rank::TermMatchData &tmd);

private:
    const matchdoc::Reference<MatchData> *_matchDataRef;
    rank::MetaInfo _metaInfo;
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SeekPositionScorer);

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_SEEKPOSITIONSCORER_H
