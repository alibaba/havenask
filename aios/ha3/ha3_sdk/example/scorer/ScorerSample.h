#ifndef ISEARCH_SCORERSAMPLE_H
#define ISEARCH_SCORERSAMPLE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/Scorer.h>
#include <ha3/rank/ScoringProvider.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <string>

BEGIN_HA3_NAMESPACE(rank);

#define SCORER_CONF "scorer_conf"

class ScorerSample : public Scorer
{
public:
    ScorerSample(const KeyValueMap &scorerParameters, const std::string &name = "ScorerSample") 
        : Scorer(name)
    {
        _matchDataRef = NULL;
        _priceRef = NULL;
        _salesCountRef = NULL;
        _isBatchScoreRef = NULL;
        KeyValueMap::const_iterator it = scorerParameters.find(SCORER_CONF);
        if (it != scorerParameters.end()) {
            _scorerConf = it->second;
        }
    }
    ScorerSample(const ScorerSample &other);
    Scorer* clone() override;
    virtual ~ScorerSample() {}
public:
    bool beginRequest(suez::turing::ScoringProvider *provider) override;
    score_t score(matchdoc::MatchDoc &matchDoc) override;
    void batchScore(ScorerParam &scorerParam) override;
    void endRequest() override;
    void destroy() override;
public:
    bool isBatchScore() const {
        return *_isBatchScoreRef;
    }
private:
    const matchdoc::Reference<MatchData> *_matchDataRef;
    const matchdoc::Reference<int32_t> *_priceRef;
    const matchdoc::Reference<int32_t> *_salesCountRef;
    bool *_isBatchScoreRef;
    std::string _scorerConf;
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_SCORERSAMPLE_H
