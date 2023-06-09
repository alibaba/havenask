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

#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/ByteSliceReader.h"
#include "indexlib/index/common/numeric_compress/EncoderProvider.h"
#include "indexlib/index/inverted_index/format/NormalInDocState.h"
#include "indexlib/index/inverted_index/format/PositionBitmapReader.h"
#include "indexlib/index/inverted_index/format/PositionListFormatOption.h"
#include "indexlib/index/inverted_index/format/skiplist/PairValueSkipListReader.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

namespace indexlib::index {

class PositionListSegmentDecoder
{
public:
    PositionListSegmentDecoder(const PositionListFormatOption& option, autil::mem_pool::Pool* sessionPool);
    virtual ~PositionListSegmentDecoder();

    void Init(util::ByteSlice* posList, tf_t totalTF, uint32_t posListBegin, uint8_t compressMode,
              NormalInDocState* state);

    void Init(const util::ByteSliceList* posList, tf_t totalTF, uint32_t posListBegin, uint8_t compressMode,
              NormalInDocState* state);

    virtual bool SkipTo(ttf_t currentTTF, NormalInDocState* state);

    void SetNeedReopen(bool needReopen) { _needReopen = needReopen; }

    // TODO: separate TfBitmap CalculateRecordOffset from LocateRecord
    virtual bool LocateRecord(const NormalInDocState* state, uint32_t& termFrequence);

    virtual uint32_t DecodeRecord(pos_t* posBuffer, uint32_t posBufferLen, pospayload_t* payloadBuffer,
                                  uint32_t payloadBufferLen);

    uint32_t GetRecordOffset() const { return _recordOffset; }

    int32_t GetOffsetInRecord() const { return _offsetInRecord; }

    PositionBitmapReader* GetPositionBitmapReader() const { return _bitmapReader; }

private:
    void InitPositionBitmapReader(util::ByteSlice* posList, uint32_t& posListCursor, NormalInDocState* state);

    void InitPositionBitmapReader(const util::ByteSliceList* posList, uint32_t& posListCursor, NormalInDocState* state);

    void InitPositionBitmapBlockBuffer(file_system::ByteSliceReader& posListReader, tf_t totalTF,
                                       uint32_t posSkipListStart, uint32_t posSkipListLen, NormalInDocState* state);

    void InitPositionSkipList(const util::ByteSliceList* posList, tf_t totalTF, uint32_t posSkipListStart,
                              uint32_t posSkipListLen, NormalInDocState* state);

    void InitPositionSkipList(util::ByteSlice* posList, tf_t totalTF, uint32_t posSkipListStart,
                              uint32_t posSkipListLen, NormalInDocState* state);

    void CalculateRecordOffsetByPosBitmap(const NormalInDocState* state, uint32_t& termFrequence,
                                          uint32_t& recordOffset, int32_t& offsetInRecord);

protected:
    PairValueSkipListReader* _posSkipListReader;
    autil::mem_pool::Pool* _sessionPool;
    const Int32Encoder* _posEncoder;
    const Int8Encoder* _posPayloadEncoder;

    PositionBitmapReader* _bitmapReader;
    uint32_t* _posBitmapBlockBuffer;
    uint32_t _posBitmapBlockCount;
    uint32_t _totalTf;

    uint32_t _decodedPosCount;
    uint32_t _recordOffset;
    uint32_t _preRecordTTF;
    int32_t _offsetInRecord;

    uint32_t _posListBegin;
    uint32_t _lastDecodeOffset;
    PositionListFormatOption _option;
    bool _ownPosBitmapBlockBuffer;
    bool _needReopen;

    util::ByteSlice* _posSingleSlice;

private:
    file_system::ByteSliceReader _posListReader;
    friend class PositionListSegmentDecoderTest;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
