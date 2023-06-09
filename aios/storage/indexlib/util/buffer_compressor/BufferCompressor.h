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

#include "autil/DynamicBuf.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/util/PoolUtil.h"
#include "indexlib/util/buffer_compressor/CompressHintData.h"
#include "indexlib/util/buffer_compressor/CompressHintDataTrainer.h"

namespace indexlib { namespace util {

class BufferCompressor
{
public:
    BufferCompressor(autil::mem_pool::Pool* pool, size_t maxDataSize, const util::KeyValueMap& param) noexcept
        : _bufferIn(NULL)
        , _bufferOut(NULL)
        , _pool(pool)
        , _poolBufferIn(NULL)
        , _poolBufferOut(NULL)
        , _poolBufferSize(maxDataSize)
    {
        _poolBufferIn = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, char, maxDataSize);
        _poolBufferOut = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, char, maxDataSize);
        _bufferIn = IE_POOL_COMPATIBLE_NEW_CLASS(_pool, autil::DynamicBuf, _poolBufferIn, maxDataSize);
        _bufferOut = IE_POOL_COMPATIBLE_NEW_CLASS(_pool, autil::DynamicBuf, _poolBufferOut, maxDataSize);
    }

    BufferCompressor(const util::KeyValueMap& param) noexcept
        : _bufferIn(NULL)
        , _bufferOut(NULL)
        , _pool(NULL)
        , _poolBufferIn(NULL)
        , _poolBufferOut(NULL)
        , _poolBufferSize(0)
    {
        _bufferIn = new autil::DynamicBuf;
        _bufferOut = new autil::DynamicBuf;
    }

    virtual ~BufferCompressor() noexcept
    {
        IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _bufferIn);
        IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _bufferOut);
        if (_poolBufferIn) {
            IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, _poolBufferIn, _poolBufferSize);
        }
        if (_poolBufferOut) {
            IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, _poolBufferOut, _poolBufferSize);
        }
    }

public:
    void SetBufferInLen(const uint32_t bufferInLen) noexcept
    {
        _bufferIn->reset();
        _bufferIn->reserve(bufferInLen);
    }

    void SetBufferOutLen(const uint32_t bufferOutLen) noexcept
    {
        _bufferOut->reset();
        _bufferOut->reserve(bufferOutLen);
    }

public:
    const std::string& GetCompressorName() const noexcept { return _compressorName; }
    const std::string& GetLibVersionString() const noexcept { return _libVersionStr; }
    void AddDataToBufferIn(const char* source, const uint32_t len) noexcept;
    void AddDataToBufferIn(const std::string& source) noexcept;

    autil::DynamicBuf& GetInBuffer() noexcept { return *_bufferIn; }

    autil::DynamicBuf& GetOutBuffer() noexcept { return *_bufferOut; }

    const char* GetBufferOut() const noexcept;
    const char* GetBufferIn() const noexcept;

    uint32_t GetBufferOutLen() const noexcept;
    uint32_t GetBufferInLen() const noexcept;
    void Reset() noexcept;

    bool Decompress(const autil::StringView& hintDataStr, size_t maxUncompressLen = 0) noexcept
    {
        CompressHintDataPtr hintObj(CreateHintData(hintDataStr, true));
        if (!hintObj) {
            return false;
        }
        return Decompress(hintObj.get(), maxUncompressLen);
    }

    bool Decompress(CompressHintData* hintData = nullptr, size_t maxUncompressLen = 0) noexcept
    {
        return DecompressToOutBuffer(_bufferIn->getBuffer(), _bufferIn->getDataLen(), hintData, maxUncompressLen);
    }

    virtual bool Compress(const autil::StringView& hintData = autil::StringView::empty_instance()) noexcept = 0;

    virtual bool DecompressToOutBuffer(const char* src, uint32_t srcLen, CompressHintData* hintData = nullptr,
                                       size_t maxUncompressLen = 0) noexcept = 0;

    virtual CompressHintDataTrainer* CreateTrainer(size_t maxBlockSize, size_t trainBlockCount, size_t hintDataCapacity,
                                                   float sampleRatio = 1.0f) const noexcept
    {
        return nullptr;
    }

    CompressHintData* CreateHintData(const autil::StringView& hintData, bool needCopy) noexcept;

protected:
    virtual CompressHintData* CreateHintDataObject() const noexcept { return nullptr; }
    bool GetCompressLevel(const util::KeyValueMap& param, int32_t& level) const noexcept;

protected:
    autil::DynamicBuf* _bufferIn;
    autil::DynamicBuf* _bufferOut;
    autil::mem_pool::Pool* _pool;
    std::string _compressorName;
    std::string _libVersionStr;

    char* _poolBufferIn;
    char* _poolBufferOut;
    size_t _poolBufferSize;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<BufferCompressor> BufferCompressorPtr;

/////////////////////////////////////////////////////////////////////
inline void BufferCompressor::AddDataToBufferIn(const char* source, const uint32_t len) noexcept
{
    _bufferIn->add(source, len);
}

inline void BufferCompressor::AddDataToBufferIn(const std::string& source) noexcept
{
    _bufferIn->add(source.c_str(), source.size());
}

inline const char* BufferCompressor::GetBufferOut() const noexcept { return _bufferOut->getBuffer(); }

inline uint32_t BufferCompressor::GetBufferOutLen() const noexcept { return _bufferOut->getDataLen(); }

inline uint32_t BufferCompressor::GetBufferInLen() const noexcept { return _bufferIn->getDataLen(); }

inline const char* BufferCompressor::GetBufferIn() const noexcept { return _bufferIn->getBuffer(); }

inline void BufferCompressor::Reset() noexcept
{
    _bufferIn->reset();
    _bufferOut->reset();
}

inline bool BufferCompressor::GetCompressLevel(const util::KeyValueMap& param, int& level) const noexcept
{
    if (param.empty()) {
        return false;
    }
    auto iter = param.find("compress_level");
    if (iter == param.end()) {
        return false;
    }
    return autil::StringUtil::fromString(iter->second, level);
}

}} // namespace indexlib::util
