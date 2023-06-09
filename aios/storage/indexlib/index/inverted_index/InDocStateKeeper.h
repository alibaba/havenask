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
#include <memory>

#include "indexlib/index/inverted_index/format/InMemPositionListDecoder.h"
#include "indexlib/index/inverted_index/format/NormalInDocPositionIterator.h"
#include "indexlib/index/inverted_index/format/NormalInDocState.h"
#include "indexlib/index/inverted_index/format/PositionListSegmentDecoder.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/inverted_index/format/skiplist/PairValueSkipListReader.h"

namespace indexlib::index {

class InDocStateKeeper
{
public:
    InDocStateKeeper(NormalInDocState* state, autil::mem_pool::Pool* sessionPool);

    ~InDocStateKeeper();

public:
    void MoveToDoc(ttf_t currentTTF);

    void MoveToSegment(util::ByteSlice* posList, tf_t totalTF, uint32_t posListBegin, uint8_t compressMode,
                       const PositionListFormatOption& option);

    void MoveToSegment(util::ByteSliceList* posList, tf_t totalTF, uint32_t posListBegin, uint8_t compressMode,
                       const PositionListFormatOption& option);

    // for in memory segment
    void MoveToSegment(InMemPositionListDecoder* posDecoder);

private:
    typedef std::vector<PositionListSegmentDecoder*> PosDecoderVec;

private:
    NormalInDocState* _state;
    PosDecoderVec _posDecoders;
    autil::mem_pool::Pool* _sessionPool;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
