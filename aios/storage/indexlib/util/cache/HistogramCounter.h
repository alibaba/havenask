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

#include <atomic>
#include <memory>

namespace indexlib { namespace util {

class HistogramBucket
{
public:
    HistogramBucket() {}
    ~HistogramBucket() {}

    HistogramBucket(const HistogramBucket&) = default;
    HistogramBucket(HistogramBucket&&) = default;
    HistogramBucket& operator=(const HistogramBucket&) = default;
    HistogramBucket& operator=(HistogramBucket&&) = default;

public:
    void Report(int64_t IOSize, int64_t latency)
    {
        readCount += 1;
        readSize += IOSize;
        ioLatency += latency;
    }
    void Reset()
    {
        readSize = 0;
        readCount = 0;
        ioLatency = 0;
    }
    HistogramBucket& operator+=(const HistogramBucket& other)
    {
        readSize += other.readSize;
        readCount += other.readCount;
        ioLatency += other.ioLatency;
        return *this;
    }

public:
    int64_t readSize = 0;
    int64_t readCount = 0;
    int64_t ioLatency = 0;
};

class HistogramCounter
{
public:
    HistogramCounter(size_t initBucketSize) { _buckets.resize(initBucketSize); }
    ~HistogramCounter() {}

public:
    HistogramCounter(const HistogramCounter& other) : _buckets(other._buckets) {}

    HistogramCounter& operator=(const HistogramCounter& other)
    {
        _buckets = other._buckets;
        return *this;
    }

    HistogramCounter(HistogramCounter&& other) : _buckets(std::move(other._buckets)) {}

    HistogramCounter& operator=(HistogramCounter&& other)
    {
        _buckets = std::move(other._buckets);
        return *this;
    }

public:
    size_t GetBucketCount() const { return _buckets.size(); }
    const HistogramBucket& GetBucket(size_t bucketIdx) const { return _buckets[bucketIdx]; }
    void Report(size_t blockCount, int64_t readSize, int64_t latency)
    {
        size_t bucketIdx = GetBucketIdx(blockCount);
        if (bucketIdx >= _buckets.size()) {
            _buckets.resize(bucketIdx + 1);
        }
        _buckets[bucketIdx].Report(readSize, latency);
    }

    void Reset()
    {
        for (auto& bucket : _buckets) {
            bucket.Reset();
        }
    }

    HistogramCounter& operator+=(const HistogramCounter& other)
    {
        size_t mergeBucketSize = std::max(_buckets.size(), other._buckets.size());
        if (mergeBucketSize > _buckets.size()) {
            _buckets.resize(mergeBucketSize);
        }
        for (size_t i = 0; i < other._buckets.size(); i++) {
            _buckets[i] += other._buckets[i];
        }
        return *this;
    }

private:
    size_t GetBucketIdx(size_t blockCount)
    {
        size_t idx = 0;
        size_t powersOfTwo = 1;
        // p = 2^idx;
        while (powersOfTwo < blockCount) {
            idx++;
            powersOfTwo = powersOfTwo << 1;
        }
        return idx;
    }

private:
    std::vector<HistogramBucket> _buckets;
};

struct AtomicHistogramBucket {
public:
    AtomicHistogramBucket() {}
    AtomicHistogramBucket(const AtomicHistogramBucket& other)
    {
        readSize.store(other.readSize);
        readCount.store(other.readCount);
        ioLatency.store(other.ioLatency);
    }

    AtomicHistogramBucket(AtomicHistogramBucket&&) = delete;

    void Collect(const HistogramBucket& src)
    {
        readSize.fetch_add(src.readSize);
        readCount.fetch_add(src.readCount);
        ioLatency.fetch_add(src.ioLatency);
    }
    std::string toString()
    {
        std::string ret = "";
        int64_t tmpReadSize = readSize.load(), tmpReadCount = readCount.load(), tmpIOLatency = ioLatency.load();
        ret.append("readCount=" + std::to_string(readCount) + ";");
        ret.append("ReadSize=" + std::to_string(readSize) + ";");
        ret.append("IOLatency=" + std::to_string(ioLatency) + ";");
        if (tmpReadCount != 0 && tmpIOLatency != 0) {
            ret.append("avgReadSize=" + std::to_string(tmpReadSize / tmpReadCount) + ";");
            ret.append("avgIOLatency=" + std::to_string(tmpIOLatency / tmpReadCount) + ";");
            ret.append("averageBandwidth(KB/s)=" + std::to_string((float)tmpReadSize * 1000000 / tmpIOLatency / 1024) +
                       ";");
        }
        return ret;
    }
    std::atomic<int64_t> readSize {0};
    std::atomic<int64_t> readCount {0};
    std::atomic<int64_t> ioLatency {0};
};

struct AtomicHistogramCounter {
public:
    AtomicHistogramCounter(size_t reserveBucketSize) : buckets(reserveBucketSize, AtomicHistogramBucket()) {}

public:
    AtomicHistogramCounter(const AtomicHistogramCounter&) = delete;
    AtomicHistogramCounter& operator=(const AtomicHistogramCounter&) = delete;
    AtomicHistogramCounter(AtomicHistogramCounter&&) = delete;
    AtomicHistogramCounter& operator=(AtomicHistogramCounter&&) = delete;

public:
    void Collect(const HistogramCounter& srcHistogram)
    {
        size_t srcBucketCount = srcHistogram.GetBucketCount();
        size_t endIdx = std::min(buckets.size(), srcBucketCount);
        for (size_t i = 0; i < endIdx; i++) {
            buckets[i].Collect(srcHistogram.GetBucket(i));
        }
    }
    std::string toString()
    {
        std::string ret = "Histogram:\n";
        for (size_t i = 0, start = 0, end = 1; i < buckets.size(); i++) {
            ret.append("bucket(" + std::to_string(start) + "," + std::to_string(end) + "]:" + buckets[i].toString() +
                       "\n");
            start = end;
            end = end << 1;
        }
        ret.append("\n");
        return ret;
    }

public:
    std::vector<AtomicHistogramBucket> buckets;
};

typedef std::shared_ptr<HistogramCounter> HistogramCounterPtr;
}} // namespace indexlib::util
