#include <math.h>
#include <ha3/rank/MatchData.h>
#include <ha3/rank/SeekPositionScorer.h>

using namespace std;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, SeekPositionScorer);

Scorer* SeekPositionScorer::clone() {
    return new SeekPositionScorer(*this);
}

bool SeekPositionScorer::beginRequest(suez::turing::ScoringProvider *turingProvider) {
    auto provider = dynamic_cast<rank::ScoringProvider*>(turingProvider);
    if (!provider) {
        return false;
    }
    _matchDataRef = provider->requireMatchData();
    provider->getQueryTermMetaInfo(&_metaInfo);
    return (_matchDataRef != NULL);
}

score_t SeekPositionScorer::score(matchdoc::MatchDoc &matchDoc) {
    const MatchData &data = _matchDataRef->getReference(matchDoc);
    score_t score = 0.0;

    for (uint32_t i = 0; i < data.getNumTerms(); i++) {
        const TermMatchData &tmd = data.getTermMatchData(i);
        if (tmd.isMatched()) {
            if (unlikely(!seekPosition(tmd))) {
		return -1.0;
      	    }
            int tf = tmd.getTermFreq();
            score_t singleTermScore = (score_t)tf / _metaInfo[i].getDocFreq();
            score += singleTermScore;
        }
    }

    return score;
}

void SeekPositionScorer::endRequest() {
}

void SeekPositionScorer::destroy() {
    delete this;
}

bool SeekPositionScorer::seekPosition(const TermMatchData &tmd) {
    index::InDocPositionIteratorPtr inDocPositionIter = tmd.getInDocPositionIterator();
    if (inDocPositionIter != NULL) {
        pos_t tmppos = INVALID_POSITION;
        return inDocPositionIter->SeekPositionWithErrorCode(0, tmppos) == 
            IE_NAMESPACE(common)::ErrorCode::OK;
    }
    return true;
}

END_HA3_NAMESPACE(rank);

