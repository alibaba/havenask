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

#include <stdint.h>
#include <string>

#include "suez/turing/expression/plugin/Scorer.h"

#include "ha3/isearch.h"
#include "ha3/rank/DefaultScorer.h"
#include "autil/Log.h" // IWYU pragma: keep

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

class TestScorer : public DefaultScorer
{
public:
    TestScorer();
    ~TestScorer();
private:
    TestScorer& operator=(const TestScorer &);
public:
    Scorer* clone() {return new TestScorer(*this);}
    bool beginRequest(suez::turing::ScoringProvider *provider);
    score_t score(matchdoc::MatchDoc &matchDoc);
private:
    std::string _ip;
    matchdoc::Reference<int32_t> *_int32Ref;
    int32_t _count;;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace rank
} // namespace isearch

