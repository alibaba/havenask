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
#include "autil/CompressionUtil.h"

#include "autil/ZlibCompressor.h"
#include "zlib.h"
#include "lz4.h"
#include "snappy.h"
#include "autil/mem_pool/PoolBase.h"

namespace autil {

CompressType convertCompressType(const std::string &str) {
    if (str == "no_compress") {
        return CompressType::NO_COMPRESS;
    } else if (str == "z_speed_compress") {
        return CompressType::Z_SPEED_COMPRESS;
    } else if (str == "z_default_compress") {
        return CompressType::Z_DEFAULT_COMPRESS;
    } else if (str == "z_best_compress") {
        return CompressType::Z_BEST_COMPRESS;
    } else if (str == "snappy") {
        return CompressType::SNAPPY;
    } else if (str == "lz4") {
        return CompressType::LZ4;
    }
    return CompressType::INVALID_COMPRESS_TYPE;
}

const char* CompressType_Name(CompressType type) {
    switch (type) {
    case CompressType::Z_SPEED_COMPRESS:
        return "z_speed_compress";
    case CompressType::Z_DEFAULT_COMPRESS:
        return "z_default_compress";
    case CompressType::SNAPPY:
        return "snappy";
    case CompressType::LZ4:
        return "lz4";
    case CompressType::Z_BEST_COMPRESS:
        return "z_best_compress";
    default:
        return "no_compress";
    }
}

static int convertZlibCompressLevel(CompressType type) {
    switch (type) {
    case CompressType::Z_SPEED_COMPRESS:
        return Z_BEST_SPEED;
    case CompressType::Z_BEST_COMPRESS:
        return Z_BEST_COMPRESSION;
    default:
        return Z_DEFAULT_COMPRESSION;
    }
}

bool CompressionUtil::compressInternal(const char *input,
                                       size_t len,
                                       CompressType type,
                                       std::string &compressedResult,
                                       autil::mem_pool::Pool *pool)
{
    switch (type) {
    case CompressType::Z_SPEED_COMPRESS:
    case CompressType::Z_DEFAULT_COMPRESS:
    case CompressType::Z_BEST_COMPRESS: {
        int compressType = convertZlibCompressLevel(type);
        autil::ZlibCompressor compressor(compressType);
        compressor.addDataToBufferIn(input, len);
        if (!compressor.compress()) {
            return false;
        }
        compressedResult.assign(compressor.getBufferOut(),
                                compressor.getBufferOutLen());
        return true;
    }
    case CompressType::SNAPPY:
        return snappy::Compress(input, len, &compressedResult);
    case CompressType::LZ4: {
        int maxDestSize = LZ4_compressBound(len);
        if (0 == maxDestSize) {
            return false;
        }
        // header compress length
        char *dest = POOL_COMPATIBLE_NEW_VECTOR(pool, char,
                maxDestSize + sizeof(int));
        int compSize = LZ4_compress_fast(input, dest + sizeof(int), len, maxDestSize, 1);
        if (0 == compSize) {
            POOL_COMPATIBLE_DELETE_VECTOR(pool, dest);
            return false;
        }
        *(int *)dest = len;
        compressedResult.assign(dest, compSize + sizeof(int));
        POOL_COMPATIBLE_DELETE_VECTOR(pool, dest);
        return true;
    }
    case CompressType::NO_COMPRESS: {
        compressedResult.assign(input, len);
        return true;
    }
    default:
        return false;
    }
}

bool CompressionUtil::decompressInternal(const char *input,
                                         size_t len,
                                         CompressType type,
                                         std::string &decompressedResult,
                                         autil::mem_pool::Pool *pool)
{
    switch (type) {
    case CompressType::SNAPPY:
        return snappy::Uncompress(input, len, &decompressedResult);
    case CompressType::Z_SPEED_COMPRESS:
    case CompressType::Z_DEFAULT_COMPRESS:
    case CompressType::Z_BEST_COMPRESS: {
        int compressType = convertZlibCompressLevel(type);
        autil::ZlibCompressor compressor(compressType);
        compressor.addDataToBufferIn(input, len);
        if (!compressor.decompress()) {
            return false;
        }
        decompressedResult.assign(compressor.getBufferOut(), compressor.getBufferOutLen());
        return true;
    }
    case CompressType::LZ4: {
        if (len < 4) {
            return false;
        }
        int originalSize = *(int*)input;
        char *dest = POOL_COMPATIBLE_NEW_VECTOR(pool, char,
                originalSize + 1);
        int uncompRet = LZ4_decompress_safe_partial(input + sizeof(int), dest, len - sizeof(int),
                originalSize, originalSize + 1);
        if (uncompRet < 0 || uncompRet != originalSize) {
            POOL_COMPATIBLE_DELETE_VECTOR(pool, dest);
            return false;
        }
        decompressedResult.assign(dest, originalSize);
        POOL_COMPATIBLE_DELETE_VECTOR(pool, dest);
        return true;
    }
    case CompressType::NO_COMPRESS: {
        decompressedResult.assign(input, len);
        return true;
    }
    default:
        return false;
    }
}


}
