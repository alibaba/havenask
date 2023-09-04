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

#include "cava/common/Common.h"

class CavaCtx;
namespace cava {
namespace lang {
class CString;
} // namespace lang
} // namespace cava
namespace suez {
namespace turing {
class MatchValues;
class MetaInfo;
class SimpleMatchData;
} // namespace turing
} // namespace suez
namespace matchdoc {
template <typename T>
class Reference;
} // namespace matchdoc

namespace ha3 {
class MatchDoc;
class MatchValue;
class MatchValues;
class SimpleMatchData;

class MatchInfoReader {
public:
    MatchInfoReader();
    ~MatchInfoReader();

private:
    MatchInfoReader(const MatchInfoReader &);
    MatchInfoReader &operator=(const MatchInfoReader &);

public:
    bool isInited() { return _inited; }
    void init(suez::turing::MetaInfo *metaInfo,
              const matchdoc::Reference<suez::turing::SimpleMatchData> *simpleMatchDataRef);

    void setMatchValuesRef(const matchdoc::Reference<suez::turing::MatchValues> *matchValuesRef);
    int32_t getDocFreq(CavaCtx *ctx, uint idx);
    int32_t getTotalTermFreq(CavaCtx *ctx, uint idx);
    uint32_t getPayload(CavaCtx *ctx, uint idx);
    int32_t getTermBoost(CavaCtx *ctx, uint idx);
    cava::lang::CString *getIndexName(CavaCtx *ctx, uint idx);
    cava::lang::CString *getTermText(CavaCtx *ctx, uint idx);

    uint getQuerySize(CavaCtx *ctx);
    cava::lang::CString *getQueryLabel(CavaCtx *ctx, uint idx);
    int32_t getMatchDataLevel(CavaCtx *ctx, uint idx);
    ha3::SimpleMatchData *getSimpleMatchData(CavaCtx *ctx, MatchDoc *doc);
    ha3::MatchValues *getMatchValues(CavaCtx *ctx, MatchDoc *doc);
    bool isMatch(CavaCtx *ctx, ha3::SimpleMatchData *cavaSimpleMatchData, uint idx);
    ha3::MatchValue *getTermMatchValue(CavaCtx *ctx, ha3::MatchValues *cavaMatchValues, uint idx);
    suez::turing::MetaInfo *getMetaInfo() { return _metaInfo; }

private:
    bool _inited;
    suez::turing::MetaInfo *_metaInfo;
    const matchdoc::Reference<suez::turing::SimpleMatchData> *_simpleMatchDataRef;
    const matchdoc::Reference<suez::turing::MatchValues> *_matchValuesRef;
};

} // namespace ha3
