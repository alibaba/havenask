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

#include "autil/MultiValueType.h"
#include "indexlib/index/kkv/common/ChunkReader.h"
namespace indexlibv2::index {

class KKVBuiltValueReader
{
public:
    KKVBuiltValueReader(int32_t fixedValueLen, bool isOnline, autil::mem_pool::Pool* sessionPool,
                        const indexlib::file_system::ReadOption& readOption)
        : _chunkReader(readOption, /*chunkOffsetAlignBit*/ 3u, isOnline, sessionPool)
        , _chunkData(ChunkData::Invalid())
        , _chunkOffset(ChunkItemOffset::INVALID_CHUNK_OFFSET)
        , _fixedValueLen(fixedValueLen)
    {
    }
    ~KKVBuiltValueReader() {}

public:
    void Init(indexlib::file_system::FileReader* fileReader, bool isCompressFile)
    {
        _chunkReader.Init(fileReader, isCompressFile);
    }
    autil::StringView Read(ValueOffset valueOffset)
    {
        if (NeedSwitchChunk(valueOffset.chunkOffset)) {
            if (!future_lite::interface::syncAwait(SwitchChunk(valueOffset.chunkOffset))) {
                return autil::StringView::empty_instance();
            }
        }
        return DoRead(valueOffset.inChunkOffset);
    }

public:
    // for coroutine search
    bool NeedSwitchChunk(uint64_t chunkOffset) { return _chunkOffset != chunkOffset; }
    autil::StringView ReadInChunk(uint64_t inChunkOffset) { return DoRead(inChunkOffset); }
    FL_LAZY(bool) SwitchChunk(uint64_t chunkOffset)
    {
        auto chunkData = FL_COAWAIT _chunkReader.Read(chunkOffset);
        if (!chunkData.IsValid()) {
            FL_CORETURN false;
        }
        _chunkOffset = chunkOffset;
        _chunkData = chunkData;
        FL_CORETURN true;
    }

private:
    autil::StringView DoRead(uint64_t inChunkOffset) const
    {
        assert(inChunkOffset < _chunkData.length);
        const char* valueAddr = _chunkData.data + inChunkOffset;
        if (_fixedValueLen > 0) {
            return autil::StringView(valueAddr, _fixedValueLen);
        }
        autil::MultiChar mc;
        mc.init(valueAddr);
        return autil::StringView(mc.data(), mc.size());
    }

private:
    ChunkReader _chunkReader;
    ChunkData _chunkData;
    uint64_t _chunkOffset;
    int32_t _fixedValueLen;
};
} // namespace indexlibv2::index
