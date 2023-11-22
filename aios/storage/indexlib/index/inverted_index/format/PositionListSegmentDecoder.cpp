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
#include "indexlib/index/inverted_index/format/PositionListSegmentDecoder.h"

#include "indexlib/index/common/numeric_compress/EncoderProvider.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/util/PoolUtil.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib::index {
namespace {
using util::ByteSlice;
using util::ByteSliceList;
} // namespace
AUTIL_LOG_SETUP(indexlib.index, PositionListSegmentDecoder);

PositionListSegmentDecoder::PositionListSegmentDecoder(const PositionListFormatOption& option,
                                                       autil::mem_pool::Pool* sessionPool)
    : _posSkipListReader(nullptr)
    , _sessionPool(sessionPool)
    , _posEncoder(nullptr)
    , _posPayloadEncoder(nullptr)
    , _bitmapReader(nullptr)
    , _posBitmapBlockBuffer(nullptr)
    , _posBitmapBlockCount(0)
    , _totalTf(0)
    , _decodedPosCount(0)
    , _recordOffset(0)
    , _preRecordTTF(0)
    , _offsetInRecord(0)
    , _posListBegin(0)
    , _lastDecodeOffset(0)
    , _option(option)
    , _ownPosBitmapBlockBuffer(false)
    , _needReopen(true)
    , _posSingleSlice(nullptr)
{
}

PositionListSegmentDecoder::~PositionListSegmentDecoder()
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _posSkipListReader);
    IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _bitmapReader);
    if (_ownPosBitmapBlockBuffer) {
        IE_POOL_COMPATIBLE_DELETE_VECTOR(_sessionPool, _posBitmapBlockBuffer, _posBitmapBlockCount);
    }
}

void PositionListSegmentDecoder::InitPositionBitmapReader(const ByteSliceList* posList, uint32_t& posListCursor,
                                                          NormalInDocState* state)
{
    uint32_t bitmapListBegin = posListCursor;
    _bitmapReader = IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, PositionBitmapReader);
    _bitmapReader->Init(posList, bitmapListBegin);
    posListCursor += _bitmapReader->GetBitmapListSize();
}

void PositionListSegmentDecoder::InitPositionBitmapReader(ByteSlice* posList, uint32_t& posListCursor,
                                                          NormalInDocState* state)
{
    uint32_t bitmapListBegin = posListCursor;
    _bitmapReader = IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, PositionBitmapReader);
    _bitmapReader->Init(posList, bitmapListBegin);
    posListCursor += _bitmapReader->GetBitmapListSize();
}

void PositionListSegmentDecoder::InitPositionBitmapBlockBuffer(file_system::ByteSliceReader& posListReader,
                                                               tf_t totalTF, uint32_t posSkipListStart,
                                                               uint32_t posSkipListLen, NormalInDocState* state)
{
    _posBitmapBlockCount = posSkipListLen / sizeof(uint32_t);
    _posBitmapBlockBuffer = IE_POOL_COMPATIBLE_NEW_VECTOR(_sessionPool, uint32_t, _posBitmapBlockCount);

    void* tmpPtr = _posBitmapBlockBuffer;
    posListReader.ReadMayCopy(tmpPtr, posSkipListLen).GetOrThrow();
    if (tmpPtr == _posBitmapBlockBuffer) {
        _ownPosBitmapBlockBuffer = true;
    } else {
        _ownPosBitmapBlockBuffer = false;
        IE_POOL_COMPATIBLE_DELETE_VECTOR(_sessionPool, _posBitmapBlockBuffer, _posBitmapBlockCount);
        _posBitmapBlockBuffer = (uint32_t*)tmpPtr;
    }
}

void PositionListSegmentDecoder::InitPositionSkipList(const ByteSliceList* posList, tf_t totalTF,
                                                      uint32_t posSkipListStart, uint32_t posSkipListLen,
                                                      NormalInDocState* state)
{
    uint32_t posSkipListEnd = posSkipListStart + posSkipListLen;
    if (ShortListOptimizeUtil::IsShortPosList(totalTF)) {
        _decodedPosCount = totalTF;
        state->SetRecordOffset(posSkipListEnd);
    } else {
        _posSkipListReader = IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, PairValueSkipListReader);
        _posSkipListReader->Load(posList, posSkipListStart, posSkipListEnd, (totalTF - 1) / MAX_POS_PER_RECORD + 1);
        _decodedPosCount = 0;
    }
}

void PositionListSegmentDecoder::InitPositionSkipList(ByteSlice* posList, tf_t totalTF, uint32_t posSkipListStart,
                                                      uint32_t posSkipListLen, NormalInDocState* state)
{
    uint32_t posSkipListEnd = posSkipListStart + posSkipListLen;
    if (ShortListOptimizeUtil::IsShortPosList(totalTF)) {
        _decodedPosCount = totalTF;
        state->SetRecordOffset(posSkipListEnd);
    } else {
        _posSkipListReader = IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, PairValueSkipListReader);
        _posSkipListReader->Load(posList, posSkipListStart, posSkipListEnd, (totalTF - 1) / MAX_POS_PER_RECORD + 1);
        _decodedPosCount = 0;
    }
}

void PositionListSegmentDecoder::Init(ByteSlice* posList, tf_t totalTF, uint32_t posListBegin, uint8_t compressMode,
                                      NormalInDocState* state)
{
    assert(posList);
    _posSingleSlice = posList;

    _totalTf = totalTF;
    EncoderProvider::EncoderParam param(compressMode, false, true);
    _posEncoder = EncoderProvider::GetInstance()->GetPosListEncoder(param);
    _posPayloadEncoder = EncoderProvider::GetInstance()->GetPosPayloadEncoder(param);

    if (_option.HasTfBitmap()) {
        InitPositionBitmapReader(posList, posListBegin, state);
    }

    if (!_option.HasPositionList()) {
        return;
    }

    _posListReader.Open(posList).GetOrThrow();
    _posListReader.Seek(posListBegin).GetOrThrow();
    uint32_t posSkipListLen = _posListReader.ReadVUInt32().GetOrThrow();
    // read pos list length
    _posListReader.ReadVUInt32().GetOrThrow();

    uint32_t posSkipListStart = _posListReader.Tell();
    _posListBegin = posSkipListStart + posSkipListLen;

    if (_option.HasTfBitmap()) {
        InitPositionBitmapBlockBuffer(_posListReader, totalTF, posSkipListStart, posSkipListLen, state);
    } else {
        InitPositionSkipList(posList, totalTF, posSkipListStart, posSkipListLen, state);
    }

    _posListReader.Seek(_posListBegin).GetOrThrow();
    state->SetCompressMode(ShortListOptimizeUtil::GetPosCompressMode(compressMode));
}

void PositionListSegmentDecoder::Init(const ByteSliceList* posList, tf_t totalTF, uint32_t posListBegin,
                                      uint8_t compressMode, NormalInDocState* state)
{
    _totalTf = totalTF;
    EncoderProvider::EncoderParam param(compressMode, false, true);
    _posEncoder = EncoderProvider::GetInstance()->GetPosListEncoder(param);
    _posPayloadEncoder = EncoderProvider::GetInstance()->GetPosPayloadEncoder(param);

    if (_option.HasTfBitmap()) {
        InitPositionBitmapReader(posList, posListBegin, state);
    }

    if (!_option.HasPositionList()) {
        return;
    }

    _posListReader.Open(const_cast<ByteSliceList*>(posList)).GetOrThrow();
    _posListReader.Seek(posListBegin).GetOrThrow();
    uint32_t posSkipListLen = _posListReader.ReadVUInt32().GetOrThrow();
    // read pos list length
    _posListReader.ReadVUInt32().GetOrThrow();

    uint32_t posSkipListStart = _posListReader.Tell();
    _posListBegin = posSkipListStart + posSkipListLen;

    if (_option.HasTfBitmap()) {
        InitPositionBitmapBlockBuffer(_posListReader, totalTF, posSkipListStart, posSkipListLen, state);
    } else {
        InitPositionSkipList(posList, totalTF, posSkipListStart, posSkipListLen, state);
    }

    _posListReader.Seek(_posListBegin).GetOrThrow();
    state->SetCompressMode(ShortListOptimizeUtil::GetPosCompressMode(compressMode));
}

bool PositionListSegmentDecoder::SkipTo(ttf_t currentTTF, NormalInDocState* state)
{
    if (!_option.HasPositionList() || _option.HasTfBitmap()) {
        return true;
    }

    if ((int32_t)_decodedPosCount > currentTTF) {
        state->SetOffsetInRecord(currentTTF - _preRecordTTF);
        return true;
    }

    uint32_t offset, len;
    if (_posSkipListReader == nullptr) {
        return false;
    }
    auto [status, ret] = _posSkipListReader->SkipTo(currentTTF + 1, _decodedPosCount, offset, len);
    THROW_IF_STATUS_ERROR(status);
    if (!ret) {
        return false;
    }
    state->SetRecordOffset(offset + _posListBegin);
    _preRecordTTF = _posSkipListReader->GetPrevKey();
    state->SetOffsetInRecord(currentTTF - _preRecordTTF);
    return true;
}

bool PositionListSegmentDecoder::LocateRecord(const NormalInDocState* state, uint32_t& termFrequence)
{
    uint32_t recordOffset;
    int32_t offsetInRecord;

    if (_option.HasTfBitmap()) {
        CalculateRecordOffsetByPosBitmap(state, termFrequence, recordOffset, offsetInRecord);
    } else {
        recordOffset = state->GetRecordOffset();
        offsetInRecord = state->GetOffsetInRecord();
        termFrequence = state->_termFreq;
    }
    _recordOffset = recordOffset;
    _offsetInRecord = offsetInRecord;

    ByteSliceList* positionList = _posListReader.GetByteSliceList();
    if (!_needReopen && _lastDecodeOffset == _recordOffset) {
        // no need to relocate
        return false;
    }

    if (_needReopen || _lastDecodeOffset > _recordOffset) {
        if (_posSingleSlice) {
            _posListReader.Open(_posSingleSlice).GetOrThrow();
        } else {
            _posListReader.Open(positionList).GetOrThrow();
        }
        _lastDecodeOffset = 0;
    }
    _posListReader.Seek(_recordOffset).GetOrThrow();
    _needReopen = false;
    return true;
}

void PositionListSegmentDecoder::CalculateRecordOffsetByPosBitmap(const NormalInDocState* state,
                                                                  uint32_t& termFrequence, uint32_t& recordOffset,
                                                                  int32_t& offsetInRecord)
{
    assert(_bitmapReader);
    PosCountInfo info = _bitmapReader->GetPosCountInfo(state->GetSeekedDocCount());
    termFrequence = info.currentDocPosCount;
    uint32_t preAggPosCount = info.preDocAggPosCount;
    recordOffset = _recordOffset;
    uint32_t decodeBlockPosCount = (_decodedPosCount & MAX_POS_PER_RECORD_MASK);
    if (likely(decodeBlockPosCount == 0)) {
        decodeBlockPosCount = MAX_POS_PER_RECORD;
    }

    if (preAggPosCount >= _decodedPosCount || preAggPosCount < _decodedPosCount - decodeBlockPosCount) {
        uint32_t blockIndex = preAggPosCount >> MAX_POS_PER_RECORD_BIT_NUM;
        uint32_t offset = blockIndex == 0 ? 0 : _posBitmapBlockBuffer[blockIndex - 1];
        _decodedPosCount =
            blockIndex == _posBitmapBlockCount ? _totalTf : (blockIndex + 1) << MAX_POS_PER_RECORD_BIT_NUM;
        decodeBlockPosCount = (_decodedPosCount & MAX_POS_PER_RECORD_MASK);
        if (likely(decodeBlockPosCount == 0)) {
            decodeBlockPosCount = MAX_POS_PER_RECORD;
        }
        recordOffset = _posListBegin + offset;
    }
    offsetInRecord = preAggPosCount - (_decodedPosCount - decodeBlockPosCount);
}

uint32_t PositionListSegmentDecoder::DecodeRecord(pos_t* posBuffer, uint32_t posBufferLen, pospayload_t* payloadBuffer,
                                                  uint32_t payloadBufferLen)
{
    _lastDecodeOffset = _posListReader.Tell();

    auto [status, posCount] = _posEncoder->Decode(posBuffer, posBufferLen, _posListReader);
    THROW_IF_STATUS_ERROR(status);

    if (_option.HasPositionPayload()) {
        auto [status, payloadCount] = _posPayloadEncoder->Decode(payloadBuffer, payloadBufferLen, _posListReader);
        THROW_IF_STATUS_ERROR(status);
        if (payloadCount != posCount) {
            INDEXLIB_THROW(util::IndexCollapsedException, "Pos payload list collapsed.");
        }
    }
    return posCount;
}
} // namespace indexlib::index
