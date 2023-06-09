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
#include <zstd.h>

#include "indexlib/util/buffer_compressor/BufferCompressor.h"
#include "indexlib/util/buffer_compressor/ZstdCompressHintData.h"

namespace indexlib { namespace util {

class ZstdCompressor : public BufferCompressor
{
public:
    ZstdCompressor(autil::mem_pool::Pool* pool, size_t maxDataSize,
                   const util::KeyValueMap& param = util::KeyValueMap()) noexcept
        : BufferCompressor(pool, ZSTD_compressBound(maxDataSize), param)
        , _compressLevel(DEFAULT_COMPRESS_LEVEL)
        , _dstream(NULL)
        , _cCtx(NULL)
        , _dCtx(NULL)
        , _normalCompressTimes(0)
        , _useCompress2(false)
        , _enableGreedyCompress(false)
    {
        _compressorName = COMPRESSOR_NAME;
        _libVersionStr = COMPRESSOR_LIB_VERSION;
        GetCompressLevel(param, _compressLevel);
        if (_compressLevel > ZSTD_maxCLevel()) {
            _compressLevel = DEFAULT_COMPRESS_LEVEL;
        }
        InitCompressMode(param);
    }

    ZstdCompressor(const util::KeyValueMap& param = util::KeyValueMap()) noexcept
        : BufferCompressor(param)
        , _compressLevel(DEFAULT_COMPRESS_LEVEL)
        , _dstream(NULL)
        , _cCtx(NULL)
        , _dCtx(NULL)
        , _normalCompressTimes(0)
        , _useCompress2(false)
        , _enableGreedyCompress(false)
    {
        _compressorName = COMPRESSOR_NAME;
        _libVersionStr = COMPRESSOR_LIB_VERSION;
        GetCompressLevel(param, _compressLevel);
        if (_compressLevel > ZSTD_maxCLevel()) {
            _compressLevel = DEFAULT_COMPRESS_LEVEL;
        }
        InitCompressMode(param);
    }

    ~ZstdCompressor() noexcept
    {
        if (_dstream) {
            ZSTD_freeDStream(_dstream);
            _dstream = NULL;
        }
        if (_cCtx) {
            ZSTD_freeCCtx(_cCtx);
            _cCtx = NULL;
        }
        if (_dCtx) {
            ZSTD_freeDCtx(_dCtx);
            _dCtx = NULL;
        }
    }

public:
    bool Compress(const autil::StringView& hitData = autil::StringView::empty_instance()) noexcept override;

    bool DecompressToOutBuffer(const char* src, uint32_t srcLen, CompressHintData* hintData,
                               size_t maxUnCompressLen = 0) noexcept override final;

    CompressHintDataTrainer* CreateTrainer(size_t maxBlockSize, size_t trainBlockCount, size_t hintDataCapacity,
                                           float sampleRatio = 1.0f) const noexcept override final;

    CompressHintData* CreateHintDataObject() const noexcept override;

private:
    bool DecompressStream(const char* src, uint32_t srcLen) noexcept;
    size_t GreedyCompress(size_t outBufferCapacity) noexcept;
    size_t NormalCompress(size_t outBufferCapacity) noexcept;
    bool NeedGreedyCompress() const noexcept;
    void InitCompressMode(const util::KeyValueMap& param) noexcept;

public:
    static const std::string COMPRESSOR_NAME;
    static const std::string COMPRESSOR_LIB_VERSION;
    int _compressLevel;

private:
    static const int DEFAULT_COMPRESS_LEVEL = 1;
    static const int MAX_GREEDY_MODE_NORMAL_COMPRESS_TIMES = 32;
    ZSTD_DStream* _dstream;
    ZSTD_CCtx* _cCtx;
    ZSTD_DCtx* _dCtx;
    uint32_t _normalCompressTimes;
    bool _useCompress2;
    bool _enableGreedyCompress;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ZstdCompressor> ZstdCompressorPtr;
/////////////////////////////////////////////////////////////////////////////
inline bool ZstdCompressor::Compress(const autil::StringView& hintData) noexcept
{
    _bufferOut->reset();
    if (_bufferIn->getDataLen() == 0) {
        return false;
    }

    if (!_cCtx) {
        _cCtx = ZSTD_createCCtx();
        if (!_cCtx) {
            AUTIL_LOG(ERROR, "ZSTD_createCCtx() error");
            return false;
        }
    }

    size_t outBufferCapacity = ZSTD_compressBound(_bufferIn->getDataLen());
    _bufferOut->reserve(outBufferCapacity);
    size_t compressLen;
    if (hintData.empty()) {
        if (NeedGreedyCompress()) {
            compressLen = GreedyCompress(outBufferCapacity);
        } else {
            compressLen = NormalCompress(outBufferCapacity);
        }
    } else {
        compressLen =
            ZSTD_compress_usingDict(_cCtx, _bufferOut->getPtr(), outBufferCapacity, _bufferIn->getBuffer(),
                                    _bufferIn->getDataLen(), hintData.data(), hintData.size(), _compressLevel);
    }
    if (ZSTD_isError(compressLen)) {
        return false;
    }
    _bufferOut->movePtr(compressLen);
    return true;
}

inline size_t ZstdCompressor::GreedyCompress(size_t outBufferCapacity) noexcept
{
    _normalCompressTimes = 0;
    size_t compress1Len = ZSTD_compressCCtx(_cCtx, _bufferOut->getPtr(), outBufferCapacity, _bufferIn->getBuffer(),
                                            _bufferIn->getDataLen(), _compressLevel);
    size_t compress2Len =
        ZSTD_compress2(_cCtx, _bufferOut->getPtr(), outBufferCapacity, _bufferIn->getBuffer(), _bufferIn->getDataLen());
    if (compress2Len < compress1Len) {
        _useCompress2 = true;
        return compress2Len;
    }
    _useCompress2 = false;
    return ZSTD_compressCCtx(_cCtx, _bufferOut->getPtr(), outBufferCapacity, _bufferIn->getBuffer(),
                             _bufferIn->getDataLen(), _compressLevel);
}

inline size_t ZstdCompressor::NormalCompress(size_t outBufferCapacity) noexcept
{
    ++_normalCompressTimes;
    if (_useCompress2) {
        return ZSTD_compress2(_cCtx, _bufferOut->getPtr(), outBufferCapacity, _bufferIn->getBuffer(),
                              _bufferIn->getDataLen());
    }
    return ZSTD_compressCCtx(_cCtx, _bufferOut->getPtr(), outBufferCapacity, _bufferIn->getBuffer(),
                             _bufferIn->getDataLen(), _compressLevel);
}

inline bool ZstdCompressor::NeedGreedyCompress() const noexcept
{
    return _enableGreedyCompress && _normalCompressTimes >= MAX_GREEDY_MODE_NORMAL_COMPRESS_TIMES;
}

inline bool ZstdCompressor::DecompressStream(const char* src, uint32_t srcLen) noexcept
{
    // Guarantee to successfully flush at least one complete compressed block in all circumstances.
    if (_bufferOut->getDataLen() == 0) {
        _bufferOut->reserve(ZSTD_DStreamOutSize());
    }
    _bufferOut->reset();

    if (!_dstream) {
        _dstream = ZSTD_createDStream();
        if (!_dstream) {
            AUTIL_LOG(ERROR, "ZSTD_createDStream() error");
            return false;
        }
    }

    const size_t initResult = ZSTD_initDStream(_dstream);
    if (ZSTD_isError(initResult)) {
        AUTIL_LOG(ERROR, "ZSTD_initDStream() error, %s", ZSTD_getErrorName(initResult));
        return false;
    }
    size_t toRead = initResult;
    ZSTD_inBuffer input = {src, srcLen, 0};
    while (input.pos < input.size) {
        uint32_t remain = _bufferOut->remaining();
        ZSTD_outBuffer output = {_bufferOut->getPtr(), remain, 0};
        // toRead : size of next compressed block
        toRead = ZSTD_decompressStream(_dstream, &output, &input);
        if (ZSTD_isError(toRead)) {
            AUTIL_LOG(ERROR, "ZSTD_decompressStream() error, %s", ZSTD_getErrorName(toRead));
            return false;
        }
        _bufferOut->movePtr(output.pos);
        if (output.pos == 0) {
            _bufferOut->reserve((_bufferOut->getTotalSize() << 1) - _bufferOut->getDataLen());
        }
    }
    return true;
}

inline bool ZstdCompressor::DecompressToOutBuffer(const char* src, uint32_t srcLen, CompressHintData* hintData,
                                                  size_t maxUnCompressLen) noexcept
{
    if (maxUnCompressLen == 0) {
        if (hintData != nullptr) {
            AUTIL_LOG(ERROR, "not support decompress stream with hint data.");
            return false;
        }
        return DecompressStream(src, srcLen);
    }

    _bufferOut->reserve(maxUnCompressLen);
    _bufferOut->reset();
    if (!_dCtx) {
        _dCtx = ZSTD_createDCtx();
        if (!_dCtx) {
            AUTIL_LOG(ERROR, "ZSTD_createDCtx() error");
            return false;
        }
    }

    size_t len;
    if (hintData) {
        ZstdCompressHintData* zstdHintData = static_cast<ZstdCompressHintData*>(hintData);
        ZSTD_DDict* ddict = zstdHintData->GetDDict();
        assert(ddict);
        len = ZSTD_decompress_usingDDict(_dCtx, _bufferOut->getPtr(), maxUnCompressLen, src, srcLen, ddict);
    } else {
        len = ZSTD_decompressDCtx(_dCtx, _bufferOut->getPtr(), maxUnCompressLen, src, srcLen);
    }
    if (ZSTD_isError(len)) {
        return false;
    }
    _bufferOut->movePtr(len);
    return true;
}

inline void ZstdCompressor::InitCompressMode(const util::KeyValueMap& param) noexcept
{
    _enableGreedyCompress = false;
    _useCompress2 = false;
    if (param.empty()) {
        return;
    }

    auto iter = param.find("compress_mode");
    if (iter == param.end()) {
        return;
    }

    if (iter->second == "compress2") {
        _enableGreedyCompress = false;
        _useCompress2 = true;
        return;
    }

    if (iter->second == "greedy") {
        _enableGreedyCompress = true;
        _normalCompressTimes = MAX_GREEDY_MODE_NORMAL_COMPRESS_TIMES;
        return;
    }
}

}} // namespace indexlib::util
