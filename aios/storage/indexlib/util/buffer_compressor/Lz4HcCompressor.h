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

#include <lz4hc.h>
#include <memory>

#include "indexlib/util/buffer_compressor/Lz4Compressor.h"

namespace indexlib { namespace util {

class Lz4HcCompressor : public Lz4Compressor
{
public:
    Lz4HcCompressor(autil::mem_pool::Pool* pool, size_t maxDataSize,
                    const util::KeyValueMap& param = util::KeyValueMap()) noexcept
        : Lz4Compressor(pool, maxDataSize, param)
        , _compressLevel(LZ4HC_CLEVEL_DEFAULT)
    {
        _compressorName = COMPRESSOR_NAME;
        GetCompressLevel(param, _compressLevel);
        if (_compressLevel < LZ4HC_CLEVEL_MIN || _compressLevel > LZ4HC_CLEVEL_MAX) {
            _compressLevel = LZ4HC_CLEVEL_DEFAULT;
        }
    }

    Lz4HcCompressor(const util::KeyValueMap& param = util::KeyValueMap()) noexcept
        : Lz4Compressor(param)
        , _compressLevel(LZ4HC_CLEVEL_DEFAULT)
    {
        _compressorName = COMPRESSOR_NAME;
        GetCompressLevel(param, _compressLevel);
        if (_compressLevel < LZ4HC_CLEVEL_MIN || _compressLevel > LZ4HC_CLEVEL_MAX) {
            _compressLevel = LZ4HC_CLEVEL_DEFAULT;
        }
    }

    ~Lz4HcCompressor() noexcept {}

public:
    bool Compress(const autil::StringView& hintData = autil::StringView::empty_instance()) noexcept override;

public:
    static const std::string COMPRESSOR_NAME;
    int _compressLevel;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<Lz4HcCompressor> Lz4HcCompressorPtr;

/////////////////////////////////////////////////////////////////////////////
inline bool Lz4HcCompressor::Compress(const autil::StringView& hintData) noexcept
{
    assert(hintData.empty());
    _bufferOut->reset();
    if (_bufferIn->getDataLen() == 0) {
        return false;
    }

    size_t maxBuffSize = LZ4_compressBound(_bufferIn->getDataLen());
    _bufferOut->reserve(maxBuffSize);
    int compressLen = LZ4_compress_HC(_bufferIn->getBuffer(), _bufferOut->getPtr(), _bufferIn->getDataLen(),
                                      maxBuffSize, _compressLevel);
    _bufferOut->movePtr(compressLen);
    return true;
}
}} // namespace indexlib::util
