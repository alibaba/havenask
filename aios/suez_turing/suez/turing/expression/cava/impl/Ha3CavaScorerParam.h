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

#include "matchdoc/Reference.h"
#include "suez/turing/expression/cava/impl/FieldRef.h"
#include "suez/turing/expression/common.h"

class CavaCtx;
namespace matchdoc {
class MatchDoc;
} // namespace matchdoc

namespace ha3 {
class MatchDoc;

class Ha3CavaScorerParam {
public:
    Ha3CavaScorerParam(matchdoc::Reference<suez::turing::score_t> *reference, int count, matchdoc::MatchDoc *matchDocs)
        : _fieldRef(reference, false), _scoreDocCount(count), _matchDocs(matchDocs) {}
    ~Ha3CavaScorerParam();

private:
    Ha3CavaScorerParam(const Ha3CavaScorerParam &);
    Ha3CavaScorerParam &operator=(const Ha3CavaScorerParam &);

public:
    ha3::DoubleRef *getScoreFieldRef(CavaCtx *ctx);
    int getScoreDocCount(CavaCtx *ctx);
    ha3::MatchDoc *getMatchDoc(CavaCtx *ctx, int i);

private:
    ha3::DoubleRef _fieldRef;
    int _scoreDocCount;
    matchdoc::MatchDoc *_matchDocs;
};

SUEZ_TYPEDEF_PTR(Ha3CavaScorerParam);

} // namespace ha3
