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

#include <string>

#include "autil/Log.h"

namespace swift {
namespace util {

enum SwiftCompressType {
    SWIFT_INVALID_COMPRESS_TYPE = 0,
    SWIFT_NO_COMPRESS,
    SWIFT_Z_SPEED_COMPRESS,
    SWIFT_Z_DEFAULT_COMPRESS,
    SWIFT_Z_BEST_COMPRESS,
    SWIFT_SNAPPY,
    SWIFT_LZ4,
};

class CompressUtil {
public:
    CompressUtil();
    ~CompressUtil();

private:
    CompressUtil(const CompressUtil &);
    CompressUtil &operator=(const CompressUtil &);

public:
    static bool compress(const std::string &result, SwiftCompressType type, std::string &compressedResult);

    static bool decompress(const std::string &result, SwiftCompressType type, std::string &decompressedResult);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace util
} // namespace swift
