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

#include <map>
#include <string>
#include <functional>

#include "ha3/cava/ScorerProvider.h"
#include "ha3/cava/scorer/CompoundScorerDef.h"

namespace plugins {

class CompoundScorer
{
public:
    CompoundScorer() {}
    virtual ~CompoundScorer() {}
public:
    static CompoundScorer *create(CavaCtx *ctx);
    bool init(CavaCtx *ctx, ha3::ScorerProvider *provider);
    bool init(CavaCtx *ctx, ha3::ScorerProvider *provider, cava::lang::CString *sortFields);
    score_t process(CavaCtx *ctx, ha3::MatchDoc *matchDoc);
private:
    std::vector<isearch::compound_scorer::WorkerType> _workers;
public:
    // for test
    size_t getWorkerCount() { return _workers.size(); }
private:
    AUTIL_LOG_DECLARE();
};

}

