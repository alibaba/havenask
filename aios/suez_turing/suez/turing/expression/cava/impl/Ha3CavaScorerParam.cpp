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
#include "suez/turing/expression/cava/impl/Ha3CavaScorerParam.h"

#include <iosfwd>

#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/cava/impl/MatchDoc.h"

class CavaCtx;

using namespace std;
namespace ha3 {

Ha3CavaScorerParam::~Ha3CavaScorerParam() {}

ha3::DoubleRef *Ha3CavaScorerParam::getScoreFieldRef(CavaCtx *ctx) { return &_fieldRef; }

int Ha3CavaScorerParam::getScoreDocCount(CavaCtx *ctx) { return _scoreDocCount; }

ha3::MatchDoc *Ha3CavaScorerParam::getMatchDoc(CavaCtx *ctx, int i) { return (ha3::MatchDoc *)&_matchDocs[i]; }

} // namespace ha3
