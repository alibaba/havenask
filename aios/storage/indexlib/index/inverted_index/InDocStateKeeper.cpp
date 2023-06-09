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

#include "indexlib/index/inverted_index/InDocStateKeeper.h"

#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InDocStateKeeper);

InDocStateKeeper::InDocStateKeeper(NormalInDocState* state, autil::mem_pool::Pool* sessionPool)
    : _state(state)
    , _sessionPool(sessionPool)
{
}

InDocStateKeeper::~InDocStateKeeper()
{
    for (size_t i = 0; i < _posDecoders.size(); ++i) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _posDecoders[i]);
    }
    _posDecoders.clear();
}

void InDocStateKeeper::MoveToDoc(ttf_t currentTTF)
{
    assert(!_posDecoders.empty());
    PositionListSegmentDecoder* decoder = _posDecoders.back();
    assert(decoder);
    decoder->SkipTo(currentTTF, _state);
}

void InDocStateKeeper::MoveToSegment(util::ByteSlice* posList, tf_t totalTF, uint32_t posListBegin,
                                     uint8_t compressMode, const PositionListFormatOption& option)
{
    PositionListSegmentDecoder* posSegDecoder =
        IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, PositionListSegmentDecoder, option, _sessionPool);
    // push back in posSegDocoder decoder vec before Init to make sure it would be deallocate when Init throw exception
    _posDecoders.push_back(posSegDecoder);
    posSegDecoder->Init(posList, totalTF, posListBegin, compressMode, _state);
    _state->SetPositionListSegmentDecoder(posSegDecoder);
}

void InDocStateKeeper::MoveToSegment(util::ByteSliceList* posList, tf_t totalTF, uint32_t posListBegin,
                                     uint8_t compressMode, const PositionListFormatOption& option)
{
    PositionListSegmentDecoder* posSegDecoder =
        IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, PositionListSegmentDecoder, option, _sessionPool);
    // push back in posSegDocoder decoder vec before Init to make sure it would be deallocate when Init throw exception
    _posDecoders.push_back(posSegDecoder);
    posSegDecoder->Init(posList, totalTF, posListBegin, compressMode, _state);
    _state->SetPositionListSegmentDecoder(posSegDecoder);
}

void InDocStateKeeper::MoveToSegment(InMemPositionListDecoder* posDecoder)
{
    assert(posDecoder);
    _posDecoders.push_back(posDecoder);
    _state->SetPositionListSegmentDecoder(posDecoder);
}
} // namespace indexlib::index
