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
#include "ha3/rank/RecordInfoScorer.h"

#include <stdint.h>
#include <cstddef>
#include <string>

#include "matchdoc/Reference.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/provider/ScoringProvider.h"

#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/search/MatchData.h"
#include "ha3/search/MetaInfo.h"
#include "ha3/rank/ScoringProvider.h"
#include "ha3/search/TermMatchData.h"
#include "ha3/search/TermMetaInfo.h"
#include "ha3/search/SearchPluginResource.h"
#include "autil/Log.h"

namespace matchdoc {
class MatchDoc;
}  // namespace matchdoc

using namespace std;
using namespace suez::turing;
using namespace isearch::util;
using namespace isearch::search;
namespace isearch {
namespace rank {

AUTIL_LOG_SETUP(ha3, RecordInfoScorer);
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

} // namespace rank
} // namespace isearch
