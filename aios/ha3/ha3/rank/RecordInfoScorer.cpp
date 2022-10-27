#include <ha3/common/QueryTermVisitor.h>
#include <ha3/rank/RecordInfoScorer.h>
#include <ha3/rank/MatchData.h>
#include <ha3/rank/MetaInfo.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(search);
BEGIN_HA3_NAMESPACE(rank);

HA3_LOG_SETUP(rank, RecordInfoScorer);
Scorer* RecordInfoScorer::clone() {
    return new RecordInfoScorer(*this);
}

bool RecordInfoScorer::beginRequest(suez::turing::ScoringProvider *turingProvider)
{
    auto provider = dynamic_cast<rank::ScoringProvider*>(turingProvider);
    provider->getQueryTermMetaInfo(&_metaInfo); 
    TRACE_SETUP(provider);
    _matchDataRef = provider->requireMatchData();
    return _matchDataRef != NULL && _metaInfo.size() > 0;
}

score_t RecordInfoScorer::score(matchdoc::MatchDoc &matchDoc) {
    const MatchData &matchData = _matchDataRef->getReference(matchDoc);
    string matchSep="|||";
    char sep = ':';
    string resStr;
    string indexName, word;
    for (uint32_t i = 0; i < matchData.getNumTerms(); i++) {
        const TermMatchData &tmd = matchData.getTermMatchData(i);
        indexName = _metaInfo[i].getIndexName();
        word = _metaInfo[i].getTermText().c_str();
        resStr += matchSep + indexName + ":" + word + sep;
        if(tmd.isMatched()){
            resStr += "1";
        }else{
            resStr += "0";
        }
    }

    RANK_TRACE(INFO, matchDoc, "###||###%s###||### ", resStr.c_str());
    return 1.0;
}

void RecordInfoScorer::endRequest() {
}

void RecordInfoScorer::destroy() {
    delete this;
}

END_HA3_NAMESPACE(rank);
