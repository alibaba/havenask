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
#include "indexlib/index/inverted_index/format/InMemPositionListDecoder.h"

#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InMemPositionListDecoder);

InMemPositionListDecoder::InMemPositionListDecoder(const PositionListFormatOption& option,
                                                   autil::mem_pool::Pool* sessionPool)
    : PositionListSegmentDecoder(option, sessionPool)
    , _posListBuffer(nullptr)
{
}

InMemPositionListDecoder::~InMemPositionListDecoder() { IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _posListBuffer); }

void InMemPositionListDecoder::Init(ttf_t totalTF, PairValueSkipListReader* skipListReader,
                                    BufferedByteSlice* posListBuffer)
{
    assert(posListBuffer);

    _totalTf = totalTF;
    _posSkipListReader = skipListReader;
    _posListBuffer = posListBuffer;
    _posListReader.Open(posListBuffer);
    _decodedPosCount = 0;
}

bool InMemPositionListDecoder::SkipTo(ttf_t currentTTF, NormalInDocState* state)
{
    if (_totalTf <= currentTTF) {
        return false;
    }

    if (!_posSkipListReader) {
        assert(_totalTf <= MAX_POS_PER_RECORD);
        state->SetRecordOffset(0);
        state->SetOffsetInRecord(currentTTF);
        return true;
    }

    if ((int32_t)_decodedPosCount > currentTTF) {
        // skipTo only used with mState in in_doc_state_keeper
        // in same record, mState no need reset RecordOffset
        state->SetOffsetInRecord(currentTTF - _preRecordTTF);
        return true;
    }

    uint32_t offset, len;
    auto [status, ret] = _posSkipListReader->SkipTo(currentTTF + 1, _decodedPosCount, offset, len);
    THROW_IF_STATUS_ERROR(status);
    if (ret) {
        _preRecordTTF = _posSkipListReader->GetPrevKey();
    } else {
        offset = _posSkipListReader->GetLastValueInBuffer();
        _preRecordTTF = _posSkipListReader->GetCurrentKey();
        _decodedPosCount = _totalTf;
    }

    state->SetRecordOffset(offset);
    state->SetOffsetInRecord(currentTTF - _preRecordTTF);
    return true;
}

bool InMemPositionListDecoder::LocateRecord(const NormalInDocState* state, uint32_t& termFrequence)
{
    assert(!_option.HasTfBitmap());
    _recordOffset = state->GetRecordOffset();
    _offsetInRecord = state->GetOffsetInRecord();
    termFrequence = state->_termFreq;

    if (!_needReopen && _lastDecodeOffset == _recordOffset) {
        // no need to relocate
        return false;
    }

    if (_needReopen || _lastDecodeOffset > _recordOffset) {
        _posListReader.Open(_posListBuffer);
        _lastDecodeOffset = 0;
    }
    _posListReader.Seek(_recordOffset);
    _needReopen = false;
    return true;
}

uint32_t InMemPositionListDecoder::DecodeRecord(pos_t* posBuffer, uint32_t posBufferLen, pospayload_t* payloadBuffer,
                                                uint32_t payloadBufferLen)
{
    _lastDecodeOffset = _posListReader.Tell();

    size_t posCount = 0;
    bool ret = _posListReader.Decode(posBuffer, posBufferLen, posCount);

    if (_option.HasPositionPayload()) {
        size_t payloadCount = 0;
        ret &= _posListReader.Decode(payloadBuffer, payloadBufferLen, payloadCount);
        ret &= (payloadCount == posCount);
    }

    if (!ret) {
        INDEXLIB_THROW(util::IndexCollapsedException, "Pos payload list collapsed.");
    }

    return posCount;
}
} // namespace indexlib::index
