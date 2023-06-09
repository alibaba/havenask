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

#include <lz4.h>
#include <memory>

#include "indexlib/util/buffer_compressor/BufferCompressor.h"
#include "indexlib/util/buffer_compressor/Lz4CompressHintData.h"

namespace indexlib { namespace util {

class Lz4Compressor : public BufferCompressor
{
public:
    Lz4Compressor(autil::mem_pool::Pool* pool, size_t maxDataSize,
                  const util::KeyValueMap& param = util::KeyValueMap()) noexcept
        : BufferCompressor(pool, LZ4_compressBound(maxDataSize), param)
    {
        _compressorName = COMPRESSOR_NAME;
        _libVersionStr = COMPRESSOR_LIB_VERSION;
    }

    Lz4Compressor(const util::KeyValueMap& param = util::KeyValueMap()) noexcept : BufferCompressor(param)
    {
        _compressorName = COMPRESSOR_NAME;
        _libVersionStr = COMPRESSOR_LIB_VERSION;
    }

    ~Lz4Compressor() noexcept
    {
        if (_stream) {
            LZ4_freeStream(_stream);
            _stream = nullptr;
        }
    }

public:
    bool Compress(const autil::StringView& hintData = autil::StringView::empty_instance()) noexcept override;
    bool DecompressToOutBuffer(const char* src, uint32_t srcLen, CompressHintData* hintData,
                               size_t maxUnCompressLen = 0) noexcept override final;

    CompressHintDataTrainer* CreateTrainer(size_t maxBlockSize, size_t trainBlockCount, size_t hintDataCapacity,
                                           float sampleRatio = 1.0f) const noexcept override final;

    CompressHintData* CreateHintDataObject() const noexcept override;

public:
    static const std::string COMPRESSOR_NAME;
    static const std::string COMPRESSOR_LIB_VERSION;

private:
    LZ4_stream_t* _stream = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<Lz4Compressor> Lz4CompressorPtr;

/////////////////////////////////////////////////////////////////////////////
inline bool Lz4Compressor::Compress(const autil::StringView& hintData) noexcept
{
    _bufferOut->reset();
    if (_bufferIn->getDataLen() == 0) {
        return false;
    }

    size_t maxBuffSize = LZ4_compressBound(_bufferIn->getDataLen());
    _bufferOut->reserve(maxBuffSize);
    int compressLen = 0;
    if (hintData.empty()) {
        compressLen =
            LZ4_compress_default(_bufferIn->getBuffer(), _bufferOut->getPtr(), _bufferIn->getDataLen(), maxBuffSize);
    } else {
        if (_stream == nullptr) {
            _stream = LZ4_createStream();
        } else {
            LZ4_resetStream_fast(_stream);
        }
        LZ4_loadDict(_stream, hintData.data(), hintData.size());
        compressLen = LZ4_compress_fast_continue(_stream, _bufferIn->getBuffer(), _bufferOut->getPtr(),
                                                 _bufferIn->getDataLen(), maxBuffSize, 1);
    }
    _bufferOut->movePtr(compressLen);
    return true;
}

inline bool Lz4Compressor::DecompressToOutBuffer(const char* src, uint32_t srcLen, CompressHintData* hintData,
                                                 size_t maxUnCompressLen) noexcept
{
    if (maxUnCompressLen == 0) {
        maxUnCompressLen = std::min((size_t)srcLen << 8, 1UL << 26); // min(srcLen * 256, 64M)
    }
    _bufferOut->reset();
    _bufferOut->reserve(maxUnCompressLen);

    int len = 0;
    if (hintData) {
        Lz4CompressHintData* lz4CompressHintData = static_cast<Lz4CompressHintData*>(hintData);
        const autil::StringView& dictData = lz4CompressHintData->GetData();
        len = LZ4_decompress_safe_usingDict(src, _bufferOut->getPtr(), srcLen, _bufferOut->getTotalSize(),
                                            dictData.data(), dictData.size());
    } else {
        len = LZ4_decompress_safe(src, _bufferOut->getPtr(), srcLen, _bufferOut->getTotalSize());
    }
    if (len < 0) {
        return false;
    }
    _bufferOut->movePtr(len);
    return true;
}
}} // namespace indexlib::util
