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

#include "suez/turing/expression/cava/impl/ScorerProvider.h"

#include "cava/codegen/CavaJitModule.h"

#include "suez/turing/expression/cava/impl/MatchInfoReader.h"
#include "ha3/isearch.h"
#include "ha3/search/MetaInfo.h"
#include "ha3/rank/ScoringProvider.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace ha3 {

class ScorerProvider : public suez::turing::ScorerProvider
{
public:
    ScorerProvider(isearch::rank::ScoringProvider *provider,
                   cava::CavaJitModulesWrapper *cavaJitModulesWrapper)
        : suez::turing::ScorerProvider(
                static_cast<suez::turing::ScoringProvider *>(provider),
                cavaJitModulesWrapper)
    {
    }
    ~ScorerProvider() {
    }
private:
    ScorerProvider(const ScorerProvider &);
    ScorerProvider& operator=(const ScorerProvider &);
public:
    isearch::rank::ScoringProvider *getScoringProviderImpl() {
        return static_cast<isearch::rank::ScoringProvider *>(_scoringProvider);
    }
    MatchInfoReader *getMatchInfoReader(CavaCtx *ctx);
    KVMapApi *getKVMapApi(CavaCtx *ctx);
    TraceApi *getTraceApi(CavaCtx *ctx);
    IRefManager *getRefManager(CavaCtx *ctx);
    bool requireMatchValues(CavaCtx *ctx);
private:
    MatchInfoReader _matchInfoReader;
    isearch::rank::MetaInfo _metaInfo;
};

typedef std::shared_ptr<ScorerProvider> ScorerProviderPtr;

}
