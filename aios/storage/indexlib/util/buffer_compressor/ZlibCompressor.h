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
#include <zlib.h>

#include "indexlib/util/buffer_compressor/BufferCompressor.h"

namespace indexlib { namespace util {

class ZlibCompressor : public BufferCompressor
{
public:
    ZlibCompressor(autil::mem_pool::Pool* pool, size_t maxDataSize,
                   const util::KeyValueMap& param = util::KeyValueMap(), int defaultLevel = Z_BEST_SPEED) noexcept
        : BufferCompressor(pool, compressBound(maxDataSize), param)
        , _compressLevel(defaultLevel)
    {
        _compressorName = COMPRESSOR_NAME;
        _libVersionStr = COMPRESSOR_LIB_VERSION;
        GetCompressLevel(param, _compressLevel);
        if (_compressLevel < Z_DEFAULT_COMPRESSION || _compressLevel > Z_BEST_COMPRESSION) {
            _compressLevel = defaultLevel;
        }
    }

    ZlibCompressor(const util::KeyValueMap& param = util::KeyValueMap(), int defaultLevel = Z_BEST_SPEED) noexcept
        : BufferCompressor(param)
        , _compressLevel(defaultLevel)
    {
        _compressorName = COMPRESSOR_NAME;
        _libVersionStr = COMPRESSOR_LIB_VERSION;
        GetCompressLevel(param, _compressLevel);
        if (_compressLevel < Z_DEFAULT_COMPRESSION || _compressLevel > Z_BEST_COMPRESSION) {
            _compressLevel = defaultLevel;
        }
    }

    ~ZlibCompressor() noexcept {}

public:
    bool Compress(const autil::StringView& hintData = autil::StringView::empty_instance()) noexcept override;
    bool DecompressToOutBuffer(const char* src, uint32_t srcLen, CompressHintData* hintData,
                               size_t maxUncompressLen = 0) noexcept override final;

private:
    static const uint32_t DEFAULT_BUFFER_SIZE = 64 * 1024; // 64k

private:
    bool DecompressStream(const char* src, uint32_t srcLen) noexcept;

public:
    static const std::string COMPRESSOR_NAME;
    static const std::string COMPRESSOR_LIB_VERSION;

protected:
    int _compressLevel;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ZlibCompressor> ZlibCompressorPtr;
/////////////////////////////////////////////////////////////////////
inline bool ZlibCompressor::Compress(const autil::StringView& hintData) noexcept
{
    assert(hintData.empty());
    _bufferOut->reset();
    if (_bufferIn->getDataLen() == 0) {
        return false;
    }

    uLong compressLen = compressBound(_bufferIn->getDataLen());
    _bufferOut->reserve((uint32_t)compressLen);

    if (compress2((Bytef*)_bufferOut->getPtr(), &compressLen, (const Bytef*)_bufferIn->getBuffer(),
                  (uLong)_bufferIn->getDataLen(), _compressLevel) != Z_OK) {
        return false;
    }
    _bufferOut->movePtr(compressLen);
    return true;
}

inline bool ZlibCompressor::DecompressStream(const char* src, uint32_t srcLen) noexcept
{
    int ret;
    unsigned have;
    z_stream strm;

    if (_bufferOut->getDataLen() == 0) {
        _bufferOut->reserve(DEFAULT_BUFFER_SIZE);
    }
    _bufferOut->reset();

    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    ret = inflateInit(&strm);
    if (ret != Z_OK) {
        return false;
    }

    strm.avail_in = srcLen;
    strm.next_in = (Bytef*)src;

    do {
        uint32_t remain = _bufferOut->remaining();
        strm.avail_out = remain;
        strm.next_out = (Bytef*)_bufferOut->getPtr();

        ret = inflate(&strm, Z_NO_FLUSH);
        switch (ret) {
        case Z_STREAM_ERROR:
        case Z_NEED_DICT:
        case Z_DATA_ERROR:
        case Z_MEM_ERROR:
            (void)inflateEnd(&strm);
            return false;
        }
        have = remain - strm.avail_out;
        _bufferOut->movePtr(have);
        if (have == remain) {
            _bufferOut->reserve((_bufferOut->getTotalSize() << 1) - _bufferOut->getDataLen());
        }
    } while (strm.avail_out == 0);

    (void)inflateEnd(&strm);
    return true;
}

inline bool ZlibCompressor::DecompressToOutBuffer(const char* src, uint32_t srcLen, CompressHintData* hintData,
                                                  size_t maxUnCompressLen) noexcept
{
    if (maxUnCompressLen == 0) {
        return DecompressStream(src, srcLen);
    }
    _bufferOut->reserve(maxUnCompressLen);
    _bufferOut->reset();
    uLong len = std::max((size_t)_bufferOut->getTotalSize(), maxUnCompressLen);

    if (uncompress((Bytef*)_bufferOut->getPtr(), &len, (const Bytef*)src, (uLong)srcLen) != Z_OK) {
        return false;
    }
    _bufferOut->movePtr(len);
    return true;
}
}} // namespace indexlib::util
