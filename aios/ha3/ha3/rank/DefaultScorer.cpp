#include <math.h>
#include <ha3/common/NumberQuery.h>
#include <ha3/common/RankQuery.h>
#include <ha3/rank/DefaultScorer.h>
#include <ha3/rank/GlobalMatchData.h>
#include <ha3/rank/MatchData.h>

using namespace std;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, DefaultScorer);

Scorer* DefaultScorer::clone() {
    return new DefaultScorer(*this);
}

bool DefaultScorer::beginRequest(suez::turing::ScoringProvider *turingProvider){
    auto provider = dynamic_cast<rank::ScoringProvider*>(turingProvider);
    if (!provider) {
        return false;
    }
    _matchDataRef = NULL;
    _traceRefer = NULL;
    ConfigClause *configClause = provider->getRequest()->getConfigClause();
    if (configClause->getKVPairValue("ds_score_type") == "docid") {
        return true;
    }
    Query *root = provider->getRequest()->getQueryClause()->getRootQuery();
    const Query::QueryVector *child = root->getChildQuery();
    if (root->getQueryName()=="RankQuery") {
        for ( size_t i=0; i<child->size(); i++ ) {
            if ((*child)[i]->getQueryName()=="NumberQuery")
            {
                NumberTerm term = ((NumberQuery*)(*child)[i].get())->getTerm();
                _rankFactors.push_back(((RankQuery*)root)->getRankBoost(i));
                _rankValues.push_back(term.getLeftNumber());
            }
        }
    }
    TRACE_SETUP(provider);

    _globalMatchData = provider->getGlobalMatchData();
    if (!_globalMatchData) {
        HA3_LOG(INFO, "require global match data failed, use docid scorer mode.");
        return true;
    }
    _matchDataRef = provider->requireMatchData();
    if (!_matchDataRef) {
        HA3_LOG(INFO, "require match data failed, use docid scorer mode.");
        return true;
    }
    provider->getQueryTermMetaInfo(&_metaInfo);
    return true;
}

score_t DefaultScorer::score(matchdoc::MatchDoc &matchDoc) {
    if (!_matchDataRef) {
        return matchDoc.getDocId();
    }
    const MatchData &data = _matchDataRef->getReference(matchDoc);
    score_t score = 0.0;
    //int32_t docCount = _globalMatchData->getDocCount();
    for (uint32_t i = 0; i < data.getNumTerms(); i++) {
        const TermMatchData &tmd = data.getTermMatchData(i);
        if (tmd.isMatched()) {
            int tf = tmd.getTermFreq();
            score_t singleTermScore  = (score_t)tf / _metaInfo[i].getDocFreq();
            score += singleTermScore;
            RANK_TRACE(TRACE1, matchDoc, "tf=%d", tf);
        }
    }
    HA3_LOG(TRACE3, "*******rank score: docid[%d], score[%f]", matchDoc.getDocId(), score);
    return score;
}

void DefaultScorer::destroy() {
    delete this;
}

END_HA3_NAMESPACE(rank);
