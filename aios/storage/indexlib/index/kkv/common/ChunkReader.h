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
#include "autil/mem_pool/Pool.h"
#include "future_lite/CoroInterface.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/kkv/common/ChunkDefine.h"
#include "indexlib/util/PooledUniquePtr.h"

namespace indexlibv2::index {

class ChunkReader
{
public:
    ChunkReader(const indexlib::file_system::ReadOption& readOption, uint32_t chunkOffsetAlignBit, bool isOnline,
                autil::mem_pool::Pool* sessionPool)
        : _readOption(readOption)
        , _chunkOffsetAlignBit(chunkOffsetAlignBit)
        , _isOnline(isOnline)
        , _sessionPool(sessionPool)
    {
    }
    ~ChunkReader() {}

public:
    void Init(indexlib::file_system::FileReader* fileReader, bool isCompressFile);
    FL_LAZY(Status) Prefetch(uint64_t size, uint64_t chunkOffset);
    FL_LAZY(ChunkData) Read(uint64_t chunkOffset);
    uint64_t CalcNextChunkOffset(uint64_t chunkOffset, uint32_t chunkDataLength) const;
    size_t GetReadCount() const { return _readCount; }

private:
    FL_LAZY(ChunkData) ReadAsync(uint64_t chunkOffset);

private:
    const char* _fileBaseAddress = nullptr;
    indexlib::file_system::FileReader* _fileReader = nullptr;
    indexlib::util::PooledUniquePtr<indexlib::file_system::CompressFileReader> _compressFileReader;
    indexlib::file_system::ReadOption _readOption;
    uint32_t _chunkOffsetAlignBit = 0;
    bool _isOnline = false;
    autil::mem_pool::Pool* _sessionPool = nullptr;
    size_t _readCount = 0ul;

    AUTIL_LOG_DECLARE();
};

inline void ChunkReader::Init(indexlib::file_system::FileReader* fileReader, bool isCompressFile)
{
    if (_isOnline && isCompressFile) {
        auto compressFileReader = static_cast<indexlib::file_system::CompressFileReader*>(fileReader);
        _compressFileReader.reset(compressFileReader->CreateSessionReader(_sessionPool), _sessionPool);
        _fileReader = _compressFileReader.get();
    } else {
        _fileReader = fileReader;
    }
    _fileBaseAddress = reinterpret_cast<const char*>(_fileReader->GetBaseAddress());
}

inline FL_LAZY(Status) ChunkReader::Prefetch(uint64_t size, uint64_t offset)
{
    auto ec = FL_COAWAIT _fileReader->PrefetchAsyncCoro(size, offset, _readOption);
    FL_CORETURN ec.Status();
}

inline FL_LAZY(ChunkData) ChunkReader::Read(uint64_t chunkOffset)
{
    ++_readCount;
    if (_fileBaseAddress == nullptr) {
        FL_CORETURN FL_COAWAIT ReadAsync(chunkOffset);
    }

    const char* chunkAddress = _fileBaseAddress + chunkOffset;
    const char* data = chunkAddress + sizeof(ChunkMeta);
    auto meta = reinterpret_cast<const ChunkMeta*>(chunkAddress);
    FL_CORETURN ChunkData::Of(data, meta->length);
}

inline uint64_t ChunkReader::CalcNextChunkOffset(uint64_t chunkOffset, uint32_t chunkDataLength) const
{
    uint64_t ret = chunkOffset + chunkDataLength + sizeof(ChunkMeta);
    if (_chunkOffsetAlignBit == 0u) {
        return ret;
    }

    uint64_t maxPaddingSize = (1u << _chunkOffsetAlignBit) - 1;
    ret += maxPaddingSize;
    return (ret >> _chunkOffsetAlignBit) << _chunkOffsetAlignBit;
}

} // namespace indexlibv2::index
