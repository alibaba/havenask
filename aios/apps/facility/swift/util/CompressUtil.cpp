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
#include "swift/util/CompressUtil.h"

#include <assert.h>
#include <zlib.h>

#include "autil/ZlibCompressor.h"
#include "snappy.h"

namespace swift {
namespace util {
AUTIL_LOG_SETUP(swift, CompressUtil);

CompressUtil::CompressUtil() {}

CompressUtil::~CompressUtil() {}

bool CompressUtil::compress(const std::string &result, SwiftCompressType type, std::string &compressedResult) {
    assert(type != SWIFT_INVALID_COMPRESS_TYPE);
    switch (type) {
    case SWIFT_SNAPPY:
        return snappy::Compress(result.c_str(), result.length(), &compressedResult);
    case SWIFT_Z_SPEED_COMPRESS:
    case SWIFT_Z_DEFAULT_COMPRESS:
    case SWIFT_Z_BEST_COMPRESS: {
        int compressType = Z_BEST_COMPRESSION;
        if (type == SWIFT_Z_SPEED_COMPRESS) {
            compressType = Z_BEST_SPEED;
        } else if (type == SWIFT_Z_DEFAULT_COMPRESS) {
            compressType = Z_DEFAULT_COMPRESSION;
        }
        autil::ZlibCompressor compressor(compressType);
        compressor.addDataToBufferIn(result);
        if (false == compressor.compress()) {
            AUTIL_LOG(WARN, "ZlibCompressor compress failed!");
            return false;
        }
        compressedResult.assign(compressor.getBufferOut(), compressor.getBufferOutLen());
        return true;
    }
    case SWIFT_LZ4: {
        AUTIL_LOG(WARN, "SWIFT_LZ4 not support");
        return false;
    }
    case SWIFT_NO_COMPRESS:
        return false;
    default:
        return false;
    }
}

bool CompressUtil::decompress(const std::string &result, SwiftCompressType type, std::string &decompressedResult) {
    assert(type != SWIFT_INVALID_COMPRESS_TYPE);
    switch (type) {
    case SWIFT_SNAPPY:
        return snappy::Uncompress(result.c_str(), result.length(), &decompressedResult);
    case SWIFT_Z_SPEED_COMPRESS:
    case SWIFT_Z_DEFAULT_COMPRESS:
    case SWIFT_Z_BEST_COMPRESS: {
        int compressType = Z_BEST_COMPRESSION;
        if (type == SWIFT_Z_SPEED_COMPRESS) {
            compressType = Z_BEST_SPEED;
        } else if (type == SWIFT_Z_DEFAULT_COMPRESS) {
            compressType = Z_DEFAULT_COMPRESSION;
        }
        autil::ZlibCompressor compressor(compressType);
        compressor.addDataToBufferIn(result);
        if (false == compressor.decompress()) {
            AUTIL_LOG(WARN, "ZlibCompressor decompress failed!");
            return false;
        }
        decompressedResult.assign(compressor.getBufferOut(), compressor.getBufferOutLen());
        return true;
    }
    case SWIFT_LZ4: {
        AUTIL_LOG(WARN, "SWIFT_LZ4 not support");
        return false;
    }
    case SWIFT_NO_COMPRESS:
        return false;
    default:
        return false;
    }
}

} // namespace util
} // namespace swift
