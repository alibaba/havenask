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
#include "ha3/rank/TestScorer.h"

#include <unistd.h>
#include <cstddef>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "ha3/isearch.h"
#include "ha3/rank/DefaultScorer.h"
#include "ha3/util/NetFunction.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/provider/ScoringProvider.h"

namespace matchdoc {
class MatchDoc;
}  // namespace matchdoc

using namespace std;
using namespace autil;

namespace isearch {
namespace rank {
AUTIL_LOG_SETUP(ha3, TestScorer);

TestScorer::TestScorer()
  : DefaultScorer("TestScorer")
{
    _int32Ref = NULL;
    _count = 0;
    if (!util::NetFunction::getPrimaryIP(_ip)) {
        throw 0;
    }
}

TestScorer::~TestScorer() {
}

bool TestScorer::beginRequest(suez::turing::ScoringProvider *provider) {
    string ips = provider->getKVPairValue("searcher");
    if (util::NetFunction::containIP(ips, _ip) || ips == "all") {
        string beginRequestSleepTime = provider->getKVPairValue("sleepInScorerBeginRequest");
        if (!beginRequestSleepTime.empty()) {
            usleep(1000ll * StringUtil::fromString<int>(beginRequestSleepTime));
        }
        string declareVariableName = provider->getKVPairValue("declareVariable");
        if (!declareVariableName.empty()) {
            _int32Ref = provider->declareVariable<int32_t>(declareVariableName, true);
        }
    }

    return DefaultScorer::beginRequest(provider);
}

score_t TestScorer::score(matchdoc::MatchDoc &matchDoc) {
    if (_int32Ref) {
        _int32Ref->set(matchDoc, _count++);
    }
    return DefaultScorer::score(matchDoc);
}

} // namespace rank
} // namespace isearch
