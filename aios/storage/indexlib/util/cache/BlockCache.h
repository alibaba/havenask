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

#include "autil/Log.h"
#include "autil/cache/cache.h"
#include "future_lite/CoroInterface.h"
#include "indexlib/util/Timer.h"
#include "indexlib/util/cache/Block.h"
#include "indexlib/util/cache/BlockAccessCounter.h"
#include "indexlib/util/cache/BlockCacheOption.h"
#include "indexlib/util/cache/CacheResourceInfo.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/metrics/MetricReporter.h"
#include "indexlib/util/metrics/Monitor.h"
#include "indexlib/util/metrics/TaggedMetricReporterGroup.h"

namespace indexlib { namespace util {

class BlockAllocator;

class BlockCache
{
public:
    static constexpr size_t DEFAULT_SHARED_BITS_NUM = 6;

    struct TaggedMetricReporter {
    public:
        void ReportHit(bool trace = false) noexcept
        {
            if (hitQps) {
                hitQps->IncreaseQps(1, trace);
            }
        }
        void ReportMiss(bool trace = false) noexcept
        {
            if (missQps) {
                missQps->IncreaseQps(1, trace);
            }
        }
        void ReportReadBlockCount(uint64_t count, bool trace = false) noexcept
        {
            if (readCount) {
                readCount->IncreaseQps(count, trace);
            }
        }
        void ReportReadSize(uint64_t size, bool trace = false) noexcept
        {
            if (readSize) {
                readSize->IncreaseQps(size, trace);
            }
        }
        void ReportReadLatency(uint64_t value, bool trace = false) noexcept
        {
            if (readLatency) {
                readLatency->Record(value, trace);
            }
        }
        QpsMetricReporterPtr hitQps;
        QpsMetricReporterPtr missQps;
        QpsMetricReporterPtr readCount;
        QpsMetricReporterPtr readSize;
        InputMetricReporterPtr readLatency;
    };

public:
    BlockCache() noexcept;
    virtual ~BlockCache() noexcept;

public:
    bool Init(const BlockCacheOption& option);

    // if you put different block value with same block id, we don't ensure the block you read later is not stale
    virtual bool Put(Block* block, autil::CacheBase::Handle** handle, autil::CacheBase::Priority priority) noexcept = 0;
    virtual Block* Get(const blockid_t& blockId, autil::CacheBase::Handle** handle) noexcept = 0;
    virtual FL_LAZY(Block*) GetAsync(const blockid_t& blockId, autil::CacheBase::Handle** handle) noexcept
    {
        FL_CORETURN Get(blockId, handle);
    }
    virtual void ReleaseHandle(autil::CacheBase::Handle* handle) noexcept = 0;

    const std::shared_ptr<BlockAllocator>& GetBlockAllocator() const noexcept { return _blockAllocator; }
    virtual CacheResourceInfo GetResourceInfo() const noexcept = 0;
    virtual uint32_t GetBlockCount() const = 0;
    virtual uint32_t GetMaxBlockCount() const = 0;

    size_t GetBlockSize() const noexcept { return _blockSize; }
    uint32_t GetIOBatchSize() const noexcept { return _iOBatchSize; }

    int64_t GetTotalHitCount() { return _blockCacheHitReporter.GetTotalCount(); }
    int64_t GetTotalMissCount() { return _blockCacheMissReporter.GetTotalCount(); }

public:
    virtual void RegisterMetrics(const util::MetricProviderPtr& metricProvider, const std::string& prefix,
                                 const kmonitor::MetricsTags& metricsTags);
    void ReportHit(BlockAccessCounter* counter = nullptr) noexcept
    {
        if (_memorySize == 0) {
            return;
        }
        _hitRatioReporter.Record(100);
        _blockCacheHitReporter.IncreaseQps(1);
        if (counter) {
            counter->blockCacheHitCount++;
        }
    }
    void ReportMiss(BlockAccessCounter* counter = nullptr) noexcept
    {
        if (_memorySize == 0) {
            return;
        }
        _hitRatioReporter.Record(0);
        _blockCacheMissReporter.IncreaseQps(1);
        if (counter) {
            counter->blockCacheMissCount++;
        }
    }
    void ReportReadLatency(int64_t readLatency, BlockAccessCounter* counter = nullptr) noexcept
    {
        _readLatencyReporter.Record(static_cast<double>(readLatency));
        if (counter) {
            counter->blockCacheReadLatency += readLatency;
            counter->blockCacheIOCount++;
        }
    }

    void ReportReadSize(int64_t size, BlockAccessCounter* counter = nullptr) noexcept
    {
        if (counter) {
            counter->blockCacheIODataSize += size;
        }
    }
    void ReportPrefetchCount(uint64_t count) noexcept
    {
        if (_memorySize == 0) {
            return;
        }
        _blockCachePrefetchReporter.IncreaseQps(count);
    }

    bool SupportReportTagMetrics() const noexcept { return _blockCacheHitTagReporter != nullptr; }

    TaggedMetricReporter DeclareTaggedMetricReporter(const std::map<std::string, std::string>& tagMap) noexcept;

    virtual void ReportMetrics()
    {
        _blockCacheHitReporter.Report();
        _blockCacheMissReporter.Report();
        _hitRatioReporter.Report();
        _readLatencyReporter.Report();
        _blockCachePrefetchReporter.Report();
        if (_blockCacheHitTagReporter) {
            _blockCacheHitTagReporter->Report();

            assert(_blockCacheMissTagReporter);
            _blockCacheMissTagReporter->Report();

            assert(_blockCacheReadCountTagReporter);
            _blockCacheReadCountTagReporter->Report();

            assert(_blockCacheReadSizeTagReporter);
            _blockCacheReadSizeTagReporter->Report();

            assert(_blockCacheReadLatencyTagReporter);
            _blockCacheReadLatencyTagReporter->Report();
        }
    }

public:
    virtual uint32_t TEST_GetRefCount(autil::CacheBase::Handle* handle) = 0;
    virtual const char* TEST_GetCacheName() const = 0;

protected:
    virtual bool DoInit(const BlockCacheOption& option) = 0;

    bool ExtractCacheParam(const BlockCacheOption& option, int32_t& shardBitsNum, float& lruHighPriorityRatio) const;

protected:
    size_t _memorySize;
    size_t _blockSize;
    uint32_t _iOBatchSize;

private:
    IE_DECLARE_METRIC(BlockCacheHitRatio);
    IE_DECLARE_METRIC(BlockCacheHitQps);
    IE_DECLARE_METRIC(BlockCacheMissQps);
    IE_DECLARE_METRIC(BlockCachePrefetchQps);
    IE_DECLARE_METRIC(BlockCacheReadLatency);
    IE_DECLARE_METRIC(BlockCacheReadCount);
    IE_DECLARE_METRIC(BlockCacheReadOneBlockQps);
    IE_DECLARE_METRIC(BlockCacheReadMultiBlockQps);

    InputMetricReporter _hitRatioReporter;
    InputMetricReporter _readLatencyReporter;

    QpsMetricReporter _blockCacheHitReporter;
    QpsMetricReporter _blockCacheMissReporter;
    QpsMetricReporter _blockCachePrefetchReporter;

    QpsTaggedMetricReporterGroupPtr _blockCacheHitTagReporter;
    QpsTaggedMetricReporterGroupPtr _blockCacheMissTagReporter;
    QpsTaggedMetricReporterGroupPtr _blockCacheReadCountTagReporter;
    QpsTaggedMetricReporterGroupPtr _blockCacheReadSizeTagReporter;
    InputTaggedMetricReporterGroupPtr _blockCacheReadLatencyTagReporter;

    Timer _timer;
    std::shared_ptr<BlockAllocator> _blockAllocator;

    kmonitor::MetricsTags _metricsTags;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<BlockCache> BlockCachePtr;
}} // namespace indexlib::util
