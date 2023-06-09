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

#include "indexlib/index/inverted_index/format/BufferedByteSliceReader.h"
#include "indexlib/index/inverted_index/format/PositionListSegmentDecoder.h"
#include "indexlib/index/inverted_index/format/skiplist/SkipListReader.h"

namespace indexlib::index {

class BufferedByteSlice;

class InMemPositionListDecoder : public PositionListSegmentDecoder
{
public:
    InMemPositionListDecoder(const PositionListFormatOption& option, autil::mem_pool::Pool* sessionPool);
    ~InMemPositionListDecoder();

    void Init(ttf_t totalTF, PairValueSkipListReader* skipListReader, BufferedByteSlice* posListBuffer);

    bool SkipTo(ttf_t currentTTF, NormalInDocState* state) override;

    bool LocateRecord(const NormalInDocState* state, uint32_t& termFrequence) override;

    uint32_t DecodeRecord(pos_t* posBuffer, uint32_t posBufferLen, pospayload_t* payloadBuffer,
                          uint32_t payloadBufferLen) override;

private:
    BufferedByteSlice* _posListBuffer;
    BufferedByteSliceReader _posListReader;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
