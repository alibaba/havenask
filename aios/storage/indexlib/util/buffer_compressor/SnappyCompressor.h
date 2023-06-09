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

#include "indexlib/util/buffer_compressor/BufferCompressor.h"
#include "snappy.h"

namespace indexlib { namespace util {

class SnappyCompressor : public BufferCompressor
{
public:
    SnappyCompressor(autil::mem_pool::Pool* pool, size_t maxDataSize,
                     const util::KeyValueMap& param = util::KeyValueMap()) noexcept
        : BufferCompressor(pool, snappy::MaxCompressedLength(maxDataSize), param)
    {
        _compressorName = COMPRESSOR_NAME;
        _libVersionStr = COMPRESSOR_LIB_VERSION;
    }

    SnappyCompressor(const util::KeyValueMap& param = util::KeyValueMap()) noexcept : BufferCompressor(param)
    {
        _compressorName = COMPRESSOR_NAME;
        _libVersionStr = COMPRESSOR_LIB_VERSION;
    }

    ~SnappyCompressor() noexcept {}

public:
    bool Compress(const autil::StringView& hintData = autil::StringView::empty_instance()) noexcept override;
    bool DecompressToOutBuffer(const char* src, uint32_t srcLen, CompressHintData* hintData,
                               size_t maxUncompressLen = 0) noexcept override final;

public:
    static const std::string COMPRESSOR_NAME;
    static const std::string COMPRESSOR_LIB_VERSION;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SnappyCompressor> SnappyCompressorPtr;
/////////////////////////////////////////////////////////////////////
inline bool SnappyCompressor::Compress(const autil::StringView& hintData) noexcept
{
    assert(hintData.empty());
    _bufferOut->reset();
    if (_bufferIn->getDataLen() == 0) {
        return false;
    }
    _bufferOut->reserve(snappy::MaxCompressedLength(_bufferIn->getDataLen()));
    size_t compressLen = 0;
    snappy::RawCompress(_bufferIn->getBuffer(), _bufferIn->getDataLen(), _bufferOut->getPtr(), &compressLen);
    _bufferOut->movePtr(compressLen);
    return true;
}

inline bool SnappyCompressor::DecompressToOutBuffer(const char* src, uint32_t srcLen, CompressHintData* hintData,
                                                    size_t maxUnCompressLen) noexcept
{
    size_t len = 0;
    if (!snappy::GetUncompressedLength(src, srcLen, &len)) {
        assert(false);
        return false;
    }

    if (maxUnCompressLen > 0) {
        _bufferOut->reserve(maxUnCompressLen);
    } else {
        _bufferOut->reserve(len);
    }
    _bufferOut->reset();
    if (!snappy::RawUncompress(src, srcLen, _bufferOut->getPtr())) {
        assert(false);
        return false;
    }
    _bufferOut->movePtr(len);
    return true;
}
}} // namespace indexlib::util
