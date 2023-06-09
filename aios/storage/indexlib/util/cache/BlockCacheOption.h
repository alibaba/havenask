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

#include "autil/legacy/jsonizable.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlib { namespace util {
struct BlockCacheOption {
    size_t memorySize = 1 * 1024 * 1024;         // 1MB
    size_t diskSize = 16UL * 1024 * 1024 * 1024; // 16GB
    size_t blockSize = 4 * 1024;                 // 4k
    size_t ioBatchSize = 4;
    std::string cacheType = "lru";
    util::KeyValueMap cacheParams;

    static BlockCacheOption LRU(size_t memorySize, size_t blockSize, size_t ioBatchSize)
    {
        BlockCacheOption option;
        option.memorySize = memorySize;
        option.blockSize = blockSize;
        option.ioBatchSize = ioBatchSize;
        option.cacheType = "lru";
        return option;
    }

    // all size are B
    static BlockCacheOption DADI(size_t memorySize, size_t diskSize, size_t blockSize, size_t ioBatchSize)
    {
        BlockCacheOption option;
        option.memorySize = memorySize;
        option.diskSize = diskSize;
        option.blockSize = blockSize;
        option.ioBatchSize = ioBatchSize;
        option.cacheType = "dadi";
        return option;
    }

    bool operator==(const BlockCacheOption& other) const
    {
        return memorySize == other.memorySize && blockSize == other.blockSize && ioBatchSize == other.ioBatchSize &&
               cacheType == other.cacheType && cacheParams == other.cacheParams;
    }

    std::string DebugString() const
    {
        std::stringstream ss;
        ss << "memory size: " << memorySize << " block size: " << blockSize << " io batch size: " << ioBatchSize
           << " cache type : " << cacheType << " cacheParams: " << autil::legacy::ToJsonString(cacheParams, true);
        return ss.str();
    }
};
}} // namespace indexlib::util
