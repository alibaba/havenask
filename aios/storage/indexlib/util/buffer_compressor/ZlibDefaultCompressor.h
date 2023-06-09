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

#include "indexlib/util/buffer_compressor/ZlibCompressor.h"

namespace indexlib { namespace util {

class ZlibDefaultCompressor : public ZlibCompressor
{
public:
    ZlibDefaultCompressor(autil::mem_pool::Pool* pool, size_t maxDataSize,
                          const util::KeyValueMap& param = util::KeyValueMap()) noexcept
        : ZlibCompressor(pool, compressBound(maxDataSize), param, Z_DEFAULT_COMPRESSION)
    {
        _compressorName = COMPRESSOR_NAME;
    }

    ZlibDefaultCompressor(const util::KeyValueMap& param = util::KeyValueMap()) noexcept
        : ZlibCompressor(param, Z_DEFAULT_COMPRESSION)
    {
        _compressorName = COMPRESSOR_NAME;
    }

    ~ZlibDefaultCompressor() noexcept {}

public:
    static const int NO_COMPRESSION = Z_NO_COMPRESSION;
    static const int BEST_SPEED = Z_BEST_SPEED;
    static const int BEST_COMPRESSION = Z_BEST_COMPRESSION;
    static const int DEFAULT_COMPRESSION = Z_DEFAULT_COMPRESSION;

private:
    static const uint32_t DEFAULT_BUFFER_SIZE = 64 * 1024; // 64k

public:
    bool Compress(const autil::StringView& hintData = autil::StringView::empty_instance()) noexcept override;

public:
    static const std::string COMPRESSOR_NAME;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ZlibDefaultCompressor> ZlibDefaultCompressorPtr;
/////////////////////////////////////////////////////////////////////
inline bool ZlibDefaultCompressor::Compress(const autil::StringView& hintData) noexcept
{
    assert(hintData.empty());
    if (_bufferIn->getDataLen() == 0) {
        return false;
    }
    if (_bufferOut->getDataLen() == 0) {
        _bufferOut->reserve(DEFAULT_BUFFER_SIZE);
    }

    int ret, flush;
    uint32_t have;
    z_stream strm;

    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    ret = deflateInit(&strm, _compressLevel);
    if (ret != Z_OK) {
        return false;
    }

    strm.avail_in = _bufferIn->getDataLen();
    strm.next_in = (Bytef*)_bufferIn->getBuffer();
    flush = Z_FINISH;

    do {
        uint32_t remain = _bufferOut->remaining();
        strm.avail_out = remain;
        strm.next_out = (Bytef*)_bufferOut->getPtr();
        ret = deflate(&strm, flush);
        if (ret == Z_STREAM_ERROR) {
            return false;
        }

        have = remain - strm.avail_out;
        _bufferOut->movePtr(have);
        if (strm.avail_out == 0) {
            _bufferOut->reserve((_bufferOut->getTotalSize() << 1) - _bufferOut->getDataLen());
        }
    } while (strm.avail_out == 0);

    assert(strm.avail_in == 0);
    (void)deflateEnd(&strm);

    return true;
}
}} // namespace indexlib::util
