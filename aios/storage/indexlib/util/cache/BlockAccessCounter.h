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

#include <iostream>

#include "indexlib/util/cache/HistogramCounter.h"

namespace indexlib { namespace util {

struct BlockAccessCounter {
    int64_t blockCacheHitCount = 0;
    int64_t blockCacheMissCount = 0;
    int64_t blockCacheReadLatency = 0;
    int64_t blockCacheIOCount = 0;
    int64_t blockCacheIODataSize = 0;
    std::unique_ptr<HistogramCounter> histCounter = nullptr;

    BlockAccessCounter(size_t histogramBucketSize = 0) noexcept
        : blockCacheHitCount(0)
        , blockCacheMissCount(0)
        , blockCacheReadLatency(0)
        , blockCacheIOCount(0)
        , blockCacheIODataSize(0)
    {
        if (histogramBucketSize) {
            histCounter = std::make_unique<HistogramCounter>(histogramBucketSize);
        }
    }

    BlockAccessCounter(const BlockAccessCounter& other) noexcept
        : blockCacheHitCount(other.blockCacheHitCount)
        , blockCacheMissCount(other.blockCacheMissCount)
        , blockCacheReadLatency(other.blockCacheReadLatency)
        , blockCacheIOCount(other.blockCacheIOCount)
        , blockCacheIODataSize(other.blockCacheIODataSize)
    {
        if (other.histCounter) {
            histCounter = std::make_unique<HistogramCounter>(*(other.histCounter));
        }
    }

    BlockAccessCounter(BlockAccessCounter&& other) noexcept
        : blockCacheHitCount(other.blockCacheHitCount)
        , blockCacheMissCount(other.blockCacheMissCount)
        , blockCacheReadLatency(other.blockCacheReadLatency)
        , blockCacheIOCount(other.blockCacheIOCount)
        , blockCacheIODataSize(other.blockCacheIODataSize)
    {
        if (other.histCounter) {
            histCounter.reset(other.histCounter.release());
        }
    }

    BlockAccessCounter& operator=(const BlockAccessCounter& other) noexcept
    {
        blockCacheHitCount = other.blockCacheHitCount;
        blockCacheMissCount = other.blockCacheMissCount;
        blockCacheReadLatency = other.blockCacheReadLatency;
        blockCacheIOCount = other.blockCacheIOCount;
        blockCacheIODataSize = other.blockCacheIODataSize;
        if (other.histCounter) {
            histCounter.reset(new HistogramCounter(*other.histCounter));
        } else {
            histCounter.reset();
        }
        return *this;
    }

    ~BlockAccessCounter() noexcept {}

    void Reset() noexcept
    {
        blockCacheHitCount = 0;
        blockCacheMissCount = 0;
        blockCacheReadLatency = 0;
        blockCacheIOCount = 0;
        blockCacheIODataSize = 0;
        if (histCounter) {
            histCounter->Reset();
        }
    }

    BlockAccessCounter& operator+=(const BlockAccessCounter& other) noexcept
    {
        blockCacheHitCount += other.blockCacheHitCount;
        blockCacheMissCount += other.blockCacheMissCount;
        blockCacheReadLatency += other.blockCacheReadLatency;
        blockCacheIOCount += other.blockCacheIOCount;
        blockCacheIODataSize += other.blockCacheIODataSize;
        if (histCounter && other.histCounter) {
            *histCounter += *other.histCounter;
        }
        return *this;
    }

    friend std::ostream& operator<<(std::ostream& os, const BlockAccessCounter& counter)
    {
        return os << "blockCacheHitCnt:" << counter.blockCacheHitCount
                  << ", blockCacheMissCnt:" << counter.blockCacheMissCount
                  << ", blockCacheReadLatency:" << counter.blockCacheReadLatency
                  << ", blockCacheIOCnt:" << counter.blockCacheIOCount
                  << ", blockCacheIODataSize:" << counter.blockCacheIODataSize;
    }
};
}} // namespace indexlib::util
