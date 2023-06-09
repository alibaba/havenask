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
#include <vector>

#include "autil/Log.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/metrics/Monitor.h"
#include "indexlib/util/metrics/TaggedMetricReporterGroup.h"

namespace indexlib { namespace file_system {
class FileSystemMetrics;
class StorageMetrics;
}} // namespace indexlib::file_system

namespace indexlib { namespace file_system {

class FileSystemMetricsReporter
{
public:
    FileSystemMetricsReporter(const util::MetricProviderPtr& metricProvider) noexcept;
    ~FileSystemMetricsReporter() noexcept;

public:
    void DeclareMetrics(const FileSystemMetrics& metrics, FSMetricPreference metricPref) noexcept;

    void ReportMetrics(const FileSystemMetrics& metrics) noexcept;
    void ReportMemoryQuotaUse(size_t memoryQuotaUse) noexcept;

    void ReportCompressTrainingLatency(int64_t latency, const kmonitor::MetricsTags* tags) noexcept;
    void ReportCompressBufferLatency(int64_t latency, const kmonitor::MetricsTags* tags) noexcept;

    bool SupportReportDecompressInfo() const noexcept
    {
        return _decompressBufferQps != nullptr || _decompressBufferLatency != nullptr ||
               _decompressBufferCpuAvgTime != nullptr;
    }

    bool SupportReportFileLength() const noexcept { return _typedFileLenReporter != nullptr; }

    void ExtractCompressTagInfo(const std::string& filePath, const std::string& compressType, size_t compressBlockSize,
                                const std::map<std::string, std::string>& compressParam,
                                std::map<std::string, std::string>& tagMap) const noexcept;

    void ExtractTagInfoFromPath(const std::string& filePath, std::map<std::string, std::string>& tagMap) const noexcept;

    util::InputMetricReporterPtr
    DeclareDecompressBufferLatencyReporter(const std::map<std::string, std::string>& tagMap) noexcept;

    util::QpsMetricReporterPtr
    DeclareDecompressBufferQpsReporter(const std::map<std::string, std::string>& tagMap) noexcept;

    util::StatusMetricReporterPtr
    DeclareFileLengthMetricReporter(const std::map<std::string, std::string>& tagMap) noexcept;

    util::CpuSlotQpsMetricReporterPtr
    DeclareDecompressBufferOneCpuSlotAvgTimeReporter(const std::map<std::string, std::string>& tagMap) noexcept;

    util::StatusMetricReporterPtr
    DeclareFileNodeUseCountMetricReporter(const std::map<std::string, std::string>& tagMap) noexcept;

    void UpdateGlobalTags(const std::map<std::string, std::string>& tags) noexcept
    {
        autil::ScopedWriteLock lock(_tagLock);
        for (auto item : tags) {
            _globalTags[item.first] = item.second;
        }
    }

    void ReportFenceDirCreate(const std::string& dirPath) noexcept;
    void ReportTempDirCreate(const std::string& dirPath) noexcept;
    void ReportBranchCreate(const std::string& branchName) noexcept;

private:
    void DeclareStorageMetrics(FSMetricPreference metricPref) noexcept;
    void DeclareFenceMetric() noexcept;
    void ReportStorageMetrics(const StorageMetrics& inputStorageMetrics,
                              const StorageMetrics& outputStorageMetrics) noexcept;

public:
    // for test
#define TEST_DEFINE_GET_METRIC(metricName)                                                                             \
    const util::MetricPtr& Get##metricName##Metric() const { return m##metricName##Metric; }

    TEST_DEFINE_GET_METRIC(InMemFileLength);
    TEST_DEFINE_GET_METRIC(MmapLockFileLength);
    TEST_DEFINE_GET_METRIC(MmapNonLockFileLength);
    TEST_DEFINE_GET_METRIC(BlockFileLength);
    TEST_DEFINE_GET_METRIC(BufferFileLength);
    TEST_DEFINE_GET_METRIC(SliceFileLength);
    TEST_DEFINE_GET_METRIC(ResourceFileLength);
    TEST_DEFINE_GET_METRIC(ResourceFileCount);

#undef TEST_DEFINE_GET_METRIC

private:
    typedef std::vector<util::MetricPtr> MetricSamplerVec;

private:
    util::MetricProviderPtr _metricProvider;

    IE_DECLARE_METRIC(InMemFileLength);
    IE_DECLARE_METRIC(MmapLockFileLength);
    IE_DECLARE_METRIC(MmapNonLockFileLength);
    IE_DECLARE_METRIC(BlockFileLength);
    IE_DECLARE_METRIC(BufferFileLength);
    IE_DECLARE_METRIC(SliceFileLength);
    IE_DECLARE_METRIC(ResourceFileLength);
    IE_DECLARE_METRIC(ResourceFileCount);

    IE_DECLARE_METRIC(InMemCacheFileCount);

    IE_DECLARE_METRIC(LocalCacheFileCount);

    IE_DECLARE_METRIC(InMemStorageFlushMemoryUse);

    IE_DECLARE_METRIC(MemoryQuotaUse);

    IE_DECLARE_METRIC(CompressBufferQps);
    IE_DECLARE_METRIC(CompressBufferLatency);

    IE_DECLARE_METRIC(CompressTrainingQps);
    IE_DECLARE_METRIC(CompressTrainingLatency);

    IE_DECLARE_METRIC(CreateFenceDirQps);
    IE_DECLARE_METRIC(CreateTempDirQps);
    IE_DECLARE_METRIC(CreateBranchQps);

    util::QpsTaggedMetricReporterGroupPtr _decompressBufferQps;
    util::InputTaggedMetricReporterGroupPtr _decompressBufferLatency;
    util::CpuSlotQpsTaggedMetricReporterGroupPtr _decompressBufferCpuAvgTime;
    util::StatusTaggedMetricReporterGroupPtr _typedFileLenReporter;
    util::StatusTaggedMetricReporterGroupPtr _fileNodeUseCountReporter;

    MetricSamplerVec _cacheHitRatioVec;
    MetricSamplerVec _cacheLast1000AccessHitRatio;

    std::map<std::string, std::string> _globalTags;
    mutable autil::ReadWriteLock _tagLock;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FileSystemMetricsReporter> FileSystemMetricsReporterPtr;

////////////////////////////////////////////////////////////////////////////////////////////////
class ScopedCompressLatencyReporter
{
public:
    ScopedCompressLatencyReporter(FileSystemMetricsReporter* reporter, const kmonitor::MetricsTags* tag,
                                  bool isTraining = false)
        : _reporter(reporter)
        , _tags(tag)
        , _initTs(autil::TimeUtility::currentTime())
        , _isTraining(isTraining)
    {
    }

    ~ScopedCompressLatencyReporter()
    {
        if (_reporter) {
            int64_t interval = autil::TimeUtility::currentTime() - _initTs;
            if (_isTraining) {
                _reporter->ReportCompressTrainingLatency(interval, _tags);
            } else {
                _reporter->ReportCompressBufferLatency(interval, _tags);
            }
        }
    }

private:
    FileSystemMetricsReporter* _reporter;
    const kmonitor::MetricsTags* _tags;
    int64_t _initTs;
    bool _isTraining;
};

}} // namespace indexlib::file_system
