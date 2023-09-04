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

#include <stddef.h>
#include <string>

#include "autil/mem_pool/Pool.h"

namespace autil {

enum class CompressType {
    NO_COMPRESS = 0,
    Z_SPEED_COMPRESS = 1,
    Z_DEFAULT_COMPRESS = 2,
    SNAPPY = 3,
    LZ4 = 4,
    Z_BEST_COMPRESS = 5,
    MAX,
    INVALID_COMPRESS_TYPE = -1,
};

extern CompressType convertCompressType(const std::string &str);
extern const char *CompressType_Name(CompressType type);

class CompressionUtil {
public:
    template <typename StringType>
    static bool compress(const StringType &input,
                         CompressType type,
                         std::string &compressedResult,
                         autil::mem_pool::Pool *pool = nullptr) {
        return compressInternal(input.data(), input.size(), type, compressedResult, pool);
    }

    template <typename StringType>
    static bool decompress(const StringType &input,
                           CompressType type,
                           std::string &decompressedResult,
                           autil::mem_pool::Pool *pool = nullptr) {
        return decompressInternal(input.data(), input.size(), type, decompressedResult, pool);
    }

private:
    static bool compressInternal(const char *data,
                                 size_t len,
                                 CompressType type,
                                 std::string &compressedResult,
                                 autil::mem_pool::Pool *pool = nullptr);
    static bool decompressInternal(const char *data,
                                   size_t len,
                                   CompressType type,
                                   std::string &decompressedResult,
                                   autil::mem_pool::Pool *pool = nullptr);
};

} // namespace autil
