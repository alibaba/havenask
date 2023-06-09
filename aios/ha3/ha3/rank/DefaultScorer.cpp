/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/provider/ScoringProvider.h"

#include "ha3/common/ConfigClause.h"
#include "ha3/common/NumberQuery.h"
#include "ha3/common/NumberTerm.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryClause.h"
#include "ha3/common/RankQuery.h"
#include "ha3/common/Request.h"
#include "ha3/common/Term.h"
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/rank/DefaultScorer.h"
#include "ha3/search/MatchData.h"
#include "ha3/search/MetaInfo.h"
#include "ha3/rank/ScoringProvider.h"
#include "ha3/search/TermMatchData.h"
#include "ha3/search/TermMetaInfo.h"
#include "ha3/search/SearchPluginResource.h"
#include "autil/Log.h"

namespace suez {
namespace turing {
class Scorer;
}  // namespace turing
}  // namespace suez

using namespace std;
using namespace suez::turing;

using namespace isearch::common;
using namespace isearch::config;
using namespace isearch::search;

namespace isearch {
namespace rank {
AUTIL_LOG_SETUP(ha3, DefaultScorer);

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
        AUTIL_LOG(INFO, "require global match data failed, use docid scorer mode.");
        return true;
    }
    _matchDataRef = provider->requireMatchData();
    if (!_matchDataRef) {
        AUTIL_LOG(INFO, "require match data failed, use docid scorer mode.");
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
    AUTIL_LOG(TRACE3, "*******rank score: docid[%d], score[%f]", matchDoc.getDocId(), score);
    return score;
}

void DefaultScorer::destroy() {
    delete this;
}

} // namespace rank
} // namespace isearch
