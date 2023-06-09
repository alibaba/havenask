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
#include "ha3/cava/ScorerProvider.h"

#include <iosfwd>

#include "suez/turing/expression/provider/MatchInfoReader.h"
#include "ha3/rank/ScoringProvider.h"

class CavaCtx;
namespace ha3 {
class IRefManager;
class KVMapApi;
class TraceApi;
}  // namespace ha3

using namespace std;

namespace ha3 {

    KVMapApi *ScorerProvider::getKVMapApi(CavaCtx *ctx) {
        return getKVMapApiImpl(ctx);
    }
    TraceApi *ScorerProvider::getTraceApi(CavaCtx *ctx) {
        return getTraceApiImpl(ctx);
    }
    IRefManager *ScorerProvider::getRefManager(CavaCtx *ctx) {
        return getRefManagerImpl(ctx);
    }
    MatchInfoReader *ScorerProvider::getMatchInfoReader(CavaCtx *ctx) {
        auto scoringProvider = getScoringProviderImpl();
        if (!_matchInfoReader.isInited()) {
            const auto *simpleMatchDataRef = scoringProvider->requireSimpleMatchData();
            if (nullptr == simpleMatchDataRef) {
                return nullptr;
            }
            scoringProvider->getQueryTermMetaInfo(&_metaInfo);
            _matchInfoReader.init(&_metaInfo, simpleMatchDataRef);
        }

        return &_matchInfoReader;
    }

    bool ScorerProvider::requireMatchValues(CavaCtx *ctx)
    {
        if (!_matchInfoReader.isInited()) {
            if(nullptr == getMatchInfoReader(ctx)) {
                return false;
            }
        }
        const auto *matchValuesRef = getScoringProviderImpl()->requireMatchValues();
        if (nullptr == matchValuesRef) {
            return false;
        }
        _matchInfoReader.setMatchValuesRef(matchValuesRef);
        return true;
    }
}
