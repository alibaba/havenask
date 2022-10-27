#ifndef ISEARCH_TRIVIALSCORER_H
#define ISEARCH_TRIVIALSCORER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Request.h>
#include <matchdoc/MatchDoc.h>
#include <matchdoc/Reference.h>
#include <ha3/rank/ScoringProvider.h>
#include <ha3/rank/Scorer.h>

BEGIN_HA3_NAMESPACE(rank);

struct TrivialScorerTrigger {
    score_t baseScore;
    int32_t scoreStep;
    std::set<uint32_t> delMatchDocs;
    bool bReverseMatchDocs;
    TrivialScorerTrigger() {
        baseScore = 0;
        scoreStep = 0;
        bReverseMatchDocs = false;
    }
};
  
class TrivialScorer : public Scorer
{
public:
    TrivialScorer(const std::string &name = "TrivialScorer", 
                  const std::string &attributeName = "uid",
                  score_t baseScore = 1);
    TrivialScorer(const TrivialScorer &another); 
    ~TrivialScorer();

public:
    bool beginRequest(suez::turing::ScoringProvider *provider);
    score_t score(matchdoc::MatchDoc &matchDoc);
    void endRequest();
    void destroy();
    Scorer* clone();
    void batchScore(ScorerParam &scorerParam);
public:
    void setScorerTrigger(TrivialScorerTrigger scorerTrigger) {
        _scorerTrigger = scorerTrigger;
    }
private:
    std::string _attrName;
    score_t _baseScore;
    const matchdoc::Reference<MatchData> *_matchDataRef;
    const matchdoc::Reference<int32_t> *_attrRef;
private:
    TrivialScorerTrigger _scorerTrigger;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_TRIVIALSCORER_H
