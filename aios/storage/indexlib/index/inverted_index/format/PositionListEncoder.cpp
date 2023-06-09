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
#include "indexlib/index/inverted_index/format/PositionListEncoder.h"

#include "indexlib/index/common/numeric_compress/EncoderProvider.h"
#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/skiplist/InMemPairValueSkipListReader.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, PositionListEncoder);

void PositionListEncoder::AddPosition(pos_t pos, pospayload_t posPayload)
{
    _posListBuffer.PushBack(0, pos - _lastPosInCurDoc);

    if (_posListFormatOption.HasPositionPayload()) {
        _posListBuffer.PushBack(1, posPayload);
    }

    _posListBuffer.EndPushBack();

    _lastPosInCurDoc = pos;
    ++_totalPosCount;

    if (_posListBuffer.NeedFlush(MAX_POS_PER_RECORD)) {
        FlushPositionBuffer(PFOR_DELTA_COMPRESS_MODE);
    }
}

void PositionListEncoder::EndDocument() { _lastPosInCurDoc = 0; }

void PositionListEncoder::Flush()
{
    uint8_t compressMode = ShortListOptimizeUtil::GetPosListCompressMode(_totalPosCount);
    FlushPositionBuffer(compressMode);
    if (_posSkipListWriter) {
        _posSkipListWriter->FinishFlush();
    }
}

void PositionListEncoder::Dump(const std::shared_ptr<file_system::FileWriter>& file)
{
    Flush();
    uint32_t posListSize = _posListBuffer.EstimateDumpSize();
    uint32_t posSkipListSize = 0;
    if (_posSkipListWriter) {
        posSkipListSize = _posSkipListWriter->EstimateDumpSize();
    }

    file->WriteVUInt32(posSkipListSize).GetOrThrow();
    file->WriteVUInt32(posListSize).GetOrThrow();
    if (_posSkipListWriter) {
        _posSkipListWriter->Dump(file);
    }
    _posListBuffer.Dump(file);
    AUTIL_LOG(TRACE1, "pos skip list length: [%u], pos list length: [%u", posSkipListSize, posListSize);
}

uint32_t PositionListEncoder::GetDumpLength() const
{
    uint32_t posSkipListSize = 0;
    if (_posSkipListWriter) {
        posSkipListSize = _posSkipListWriter->EstimateDumpSize();
    }

    uint32_t posListSize = _posListBuffer.EstimateDumpSize();
    return VByteCompressor::GetVInt32Length(posSkipListSize) + VByteCompressor::GetVInt32Length(posListSize) +
           posSkipListSize + posListSize;
}

void PositionListEncoder::CreatePosSkipListWriter()
{
    assert(_posSkipListWriter == nullptr);
    autil::mem_pool::RecyclePool* bufferPool =
        dynamic_cast<autil::mem_pool::RecyclePool*>(_posListBuffer.GetBufferPool());
    assert(bufferPool);

    void* buffer = _byteSlicePool->allocate(sizeof(BufferedSkipListWriter));
    BufferedSkipListWriter* posSkipListWriter = new (buffer) BufferedSkipListWriter(_byteSlicePool, bufferPool);
    posSkipListWriter->Init(_posListFormat->GetPositionSkipListFormat());

    // skip list writer should be atomic created;
    _posSkipListWriter = posSkipListWriter;
}

void PositionListEncoder::AddPosSkipListItem(uint32_t totalPosCount, uint32_t compressedPosSize, bool needFlush)
{
    if (_posListFormatOption.HasTfBitmap()) {
        if (needFlush) {
            _posSkipListWriter->AddItem(compressedPosSize);
        }
    } else {
        _posSkipListWriter->AddItem(totalPosCount, compressedPosSize);
    }
}

void PositionListEncoder::FlushPositionBuffer(uint8_t compressMode)
{
    // TODO: uncompress need this
    bool needFlush = _posListBuffer.NeedFlush(MAX_POS_PER_RECORD);

    uint32_t flushSize = _posListBuffer.Flush(compressMode);
    if (flushSize > 0) {
        if (compressMode != SHORT_LIST_COMPRESS_MODE) {
            if (_posSkipListWriter == nullptr) {
                CreatePosSkipListWriter();
            }
            AddPosSkipListItem(_totalPosCount, flushSize, needFlush);
        }
    }
}

InMemPositionListDecoder* PositionListEncoder::GetInMemPositionListDecoder(autil::mem_pool::Pool* sessionPool) const
{
    // doclist -> ttf -> pos skiplist -> poslist
    ttf_t ttf = _totalPosCount;

    // TODO: delete buffer in pool
    InMemPairValueSkipListReader* inMemSkipListReader = nullptr;
    if (_posSkipListWriter) {
        // not support tf bitmap in realtime segment
        assert(!_posListFormatOption.HasTfBitmap());
        inMemSkipListReader =
            IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, InMemPairValueSkipListReader, sessionPool, false);
        inMemSkipListReader->Load(_posSkipListWriter);
    }
    BufferedByteSlice* posListBuffer =
        IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, BufferedByteSlice, sessionPool, sessionPool);
    _posListBuffer.SnapShot(posListBuffer);

    InMemPositionListDecoder* decoder =
        IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, InMemPositionListDecoder, _posListFormatOption, sessionPool);
    decoder->Init(ttf, inMemSkipListReader, posListBuffer);

    return decoder;
}

} // namespace indexlib::index
