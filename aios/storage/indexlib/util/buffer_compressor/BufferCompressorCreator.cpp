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
#include "indexlib/util/buffer_compressor/BufferCompressorCreator.h"

#include "indexlib/util/buffer_compressor/Lz4Compressor.h"
#include "indexlib/util/buffer_compressor/Lz4HcCompressor.h"
#include "indexlib/util/buffer_compressor/SnappyCompressor.h"
#include "indexlib/util/buffer_compressor/ZlibCompressor.h"
#include "indexlib/util/buffer_compressor/ZlibDefaultCompressor.h"
#include "indexlib/util/buffer_compressor/ZstdCompressor.h"

using namespace std;
using namespace autil::mem_pool;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, BufferCompressorCreator);

BufferCompressorCreator::BufferCompressorCreator() {}

BufferCompressorCreator::~BufferCompressorCreator() {}

BufferCompressor* BufferCompressorCreator::CreateBufferCompressor(const string& compressorName,
                                                                  const util::KeyValueMap& param) noexcept
{
    if (compressorName == SnappyCompressor::COMPRESSOR_NAME) {
        return new SnappyCompressor(param);
    } else if (compressorName == Lz4Compressor::COMPRESSOR_NAME) {
        return new Lz4Compressor(param);
    } else if (compressorName == Lz4HcCompressor::COMPRESSOR_NAME) {
        return new Lz4HcCompressor(param);
    } else if (compressorName == ZstdCompressor::COMPRESSOR_NAME) {
        return new ZstdCompressor(param);
    } else if (compressorName == ZlibCompressor::COMPRESSOR_NAME) {
        return new ZlibCompressor(param);
    } else if (compressorName == ZlibDefaultCompressor::COMPRESSOR_NAME) {
        return new ZlibDefaultCompressor(param);
    } else {
        AUTIL_LOG(ERROR, "unknown compressor name [%s]", compressorName.c_str());
        return NULL;
    }
    return NULL;
}

BufferCompressor* BufferCompressorCreator::CreateBufferCompressor(Pool* pool, const string& compressorName,
                                                                  size_t maxBufferSize,
                                                                  const util::KeyValueMap& param) noexcept
{
    assert(pool);
    BufferCompressor* compressor = NULL;
    if (compressorName == SnappyCompressor::COMPRESSOR_NAME) {
        compressor = IE_POOL_COMPATIBLE_NEW_CLASS(pool, SnappyCompressor, pool, maxBufferSize, param);
    } else if (compressorName == Lz4Compressor::COMPRESSOR_NAME) {
        compressor = IE_POOL_COMPATIBLE_NEW_CLASS(pool, Lz4Compressor, pool, maxBufferSize, param);
    } else if (compressorName == Lz4HcCompressor::COMPRESSOR_NAME) {
        compressor = IE_POOL_COMPATIBLE_NEW_CLASS(pool, Lz4HcCompressor, pool, maxBufferSize, param);
    } else if (compressorName == ZstdCompressor::COMPRESSOR_NAME) {
        compressor = IE_POOL_COMPATIBLE_NEW_CLASS(pool, ZstdCompressor, pool, maxBufferSize, param);
    } else if (compressorName == ZlibCompressor::COMPRESSOR_NAME) {
        compressor = IE_POOL_COMPATIBLE_NEW_CLASS(pool, ZlibCompressor, pool, maxBufferSize, param);
    } else if (compressorName == ZlibDefaultCompressor::COMPRESSOR_NAME) {
        compressor = IE_POOL_COMPATIBLE_NEW_CLASS(pool, ZlibDefaultCompressor, pool, maxBufferSize, param);
    } else {
        AUTIL_LOG(ERROR, "unknown compressor name [%s]", compressorName.c_str());
        return NULL;
    }
    return compressor;
}

bool BufferCompressorCreator::IsValidCompressorName(const string& compressorName) noexcept
{
    set<string> validCompressorNames {SnappyCompressor::COMPRESSOR_NAME, Lz4Compressor::COMPRESSOR_NAME,
                                      Lz4HcCompressor::COMPRESSOR_NAME,  ZstdCompressor::COMPRESSOR_NAME,
                                      ZlibCompressor::COMPRESSOR_NAME,   ZlibDefaultCompressor::COMPRESSOR_NAME};
    return validCompressorNames.find(compressorName) != validCompressorNames.end();
}
}} // namespace indexlib::util
