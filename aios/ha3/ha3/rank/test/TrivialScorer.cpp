#include <ha3/rank/test/TrivialScorer.h>
#include <assert.h>

USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);

using namespace std;
BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, TrivialScorer);

TrivialScorer::TrivialScorer(const string &name,
                             const string &attrName,
                             score_t baseScore)
    : suez::turing::Scorer(name), _attrName(attrName), _baseScore(baseScore)
{
    _matchDataRef = NULL;
    _attrRef = NULL;
}

TrivialScorer::TrivialScorer(const TrivialScorer &another)
    : Scorer(another.getScorerName()),
      _attrName(another._attrName),
      _baseScore(another._baseScore)
{
    _matchDataRef = NULL;
    _attrRef = NULL;
}


Scorer* TrivialScorer::clone() {
    return new TrivialScorer(*this);
}

TrivialScorer::~TrivialScorer() {
}

bool TrivialScorer::beginRequest(suez::turing::ScoringProvider *provider_){
    auto provider = dynamic_cast<ScoringProvider *>(provider_);
    _matchDataRef = provider->requireMatchData();
    _attrRef = provider->requireAttribute<int32_t>(_attrName);
    return (_matchDataRef != NULL && _attrRef != NULL);
}

score_t TrivialScorer::score(matchdoc::MatchDoc &matchDoc) {
    int32_t value = 0;
    value = _attrRef->get(matchDoc);
    score_t score = _baseScore + value;
    HA3_LOG(TRACE2, "scorer: %s, base score: %.1f, attribute: %d, score: %.1f",
        getScorerName().c_str(), _baseScore, value, score);
    return score;
}

void TrivialScorer::batchScore(ScorerParam &scorerParam) {
    score_t score = _scorerTrigger.baseScore;
    for (size_t i = 0; i < scorerParam.scoreDocCount; ++i) {
        matchdoc::MatchDoc &matchDoc = scorerParam.matchDocs[i];
        scorerParam.reference->set(matchDoc, score);
        score += _scorerTrigger.scoreStep;

        if (_scorerTrigger.delMatchDocs.find(matchDoc.getDocId()) !=
            _scorerTrigger.delMatchDocs.end())
        {
            matchDoc.setDeleted();
        }
    }
    if (_scorerTrigger.bReverseMatchDocs) {
        std::reverse(scorerParam.matchDocs,
                     scorerParam.matchDocs + scorerParam.scoreDocCount);
    }
}

void TrivialScorer::endRequest() {
}


void TrivialScorer::destroy() {
    delete this;
}
END_HA3_NAMESPACE(rank);
