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
#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "suez/turing/expression/plugin/Scorer.h"

#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/search/MetaInfo.h"
#include "ha3/search/MatchData.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace rank {
class GlobalMatchData;
}  // namespace rank
}  // namespace isearch
namespace matchdoc {
class MatchDoc;
template <typename T> class Reference;
}  // namespace matchdoc
namespace suez {
namespace turing {
class ScoringProvider;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace rank {

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
    AUTIL_LOG_DECLARE();
};

} // namespace rank
} // namespace isearch
