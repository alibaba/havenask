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
#include "indexlib/file_system/FileSystemMetricsReporter.h"

#include <cstddef>
#include <memory>

#include "autil/EnvUtil.h"
#include "autil/StringTokenizer.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemMetrics.h"
#include "indexlib/file_system/StorageMetrics.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/util/RegularExpression.h"
#include "kmonitor/client/MetricType.h"

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, FileSystemMetricsReporter);

FileSystemMetricsReporter::FileSystemMetricsReporter(const util::MetricProviderPtr& metricProvider) noexcept
    : _metricProvider(metricProvider)
{
    if (autil::EnvUtil::getEnv("INDEXLIB_REPORT_HEAVY_COST_METRIC", false) ||
        autil::EnvUtil::getEnv("INDEXLIB_REPORT_LIGHT_COST_METRIC", false)) {
        _decompressBufferQps.reset(new util::QpsTaggedMetricReporterGroup);
        _decompressBufferLatency.reset(new util::InputTaggedMetricReporterGroup);
        uint32_t cpuSlotNum = autil::EnvUtil::getEnv("INDEXLIB_QUOTA_CPU_SLOT_NUM", 0);
        if (cpuSlotNum > 0) {
            _decompressBufferCpuAvgTime.reset(new util::CpuSlotQpsTaggedMetricReporterGroup);
        }
    }

    if (autil::EnvUtil::getEnv("INDEXLIB_REPORT_DETAIL_FILE_INFO", false)) {
        _typedFileLenReporter.reset(new util::StatusTaggedMetricReporterGroup);
        _fileNodeUseCountReporter.reset(new util::StatusTaggedMetricReporterGroup);
    }
}

FileSystemMetricsReporter::~FileSystemMetricsReporter() noexcept {}

void FileSystemMetricsReporter::DeclareMetrics(const FileSystemMetrics& metrics, FSMetricPreference metricPref) noexcept
{
    DeclareStorageMetrics(metricPref);
    DeclareFenceMetric();

    if (_decompressBufferQps) {
        vector<string> limitedTagKeys;
        if (autil::EnvUtil::getEnv("INDEXLIB_REPORT_LIGHT_COST_METRIC", false)) {
            limitedTagKeys = {"identifier", "data_type", "file_name", "compress_type"};
        }
        _decompressBufferQps->Init(_metricProvider, "file_system/DecompressBufferQps", limitedTagKeys);
        assert(_decompressBufferLatency);
        _decompressBufferLatency->Init(_metricProvider, "file_system/DecompressBufferLatency", limitedTagKeys);
        if (_decompressBufferCpuAvgTime) {
            _decompressBufferCpuAvgTime->Init(_metricProvider, "file_system/DecompressBufferAvgCpuTime",
                                              limitedTagKeys);
        }
    }

    if (_typedFileLenReporter) {
        _typedFileLenReporter->Init(_metricProvider, "file_system/TypedFileLength");
    }

    if (_fileNodeUseCountReporter) {
        _fileNodeUseCountReporter->Init(_metricProvider, "file_system/FileNodeUseCount");
    }

    IE_INIT_METRIC_GROUP(_metricProvider, CompressBufferQps, "file_system/CompressBufferQps", kmonitor::QPS, "count");
    IE_INIT_METRIC_GROUP(_metricProvider, CompressBufferLatency, "file_system/CompressBufferLatency", kmonitor::GAUGE,
                         "us");

    IE_INIT_METRIC_GROUP(_metricProvider, CompressTrainingQps, "file_system/CompressTrainingQps", kmonitor::QPS,
                         "count");
    IE_INIT_METRIC_GROUP(_metricProvider, CompressTrainingLatency, "file_system/CompressTrainingLatency",
                         kmonitor::GAUGE, "us");
}

void FileSystemMetricsReporter::DeclareFenceMetric() noexcept
{
    IE_INIT_METRIC_GROUP(_metricProvider, CreateFenceDirQps, "file_system/CreateFenceDirQps", kmonitor::QPS, "count");
    IE_INIT_METRIC_GROUP(_metricProvider, CreateTempDirQps, "file_system/CreateTempDirQps", kmonitor::QPS, "count");
    IE_INIT_METRIC_GROUP(_metricProvider, CreateBranchQps, "file_system/CreateBranchQps", kmonitor::QPS, "count");
}

void FileSystemMetricsReporter::ReportFenceDirCreate(const std::string& dirPath) noexcept
{
    map<string, string> kvMap = {{"fence_dir_path", dirPath}};
    kmonitor::MetricsTagsPtr tags(new kmonitor::MetricsTags(kvMap));
    IE_INCREASE_QPS_WITH_TAGS(CreateFenceDirQps, tags.get());
}

void FileSystemMetricsReporter::ReportTempDirCreate(const std::string& dirPath) noexcept
{
    map<string, string> kvMap = {{"temp_dir_path", dirPath}};
    kmonitor::MetricsTagsPtr tags(new kmonitor::MetricsTags(kvMap));
    IE_INCREASE_QPS_WITH_TAGS(CreateTempDirQps, tags.get());
}

void FileSystemMetricsReporter::ReportBranchCreate(const std::string& branchName) noexcept
{
    map<string, string> kvMap = {{"branch_name", branchName}};
    kmonitor::MetricsTagsPtr tags(new kmonitor::MetricsTags(kvMap));
    IE_INCREASE_QPS_WITH_TAGS(CreateBranchQps, tags.get());
}

void FileSystemMetricsReporter::DeclareStorageMetrics(FSMetricPreference metricPref) noexcept
{
#define INIT_FILE_SYSTEM_METRIC(metric, unit)                                                                          \
    IE_INIT_METRIC_GROUP(_metricProvider, metric, "file_system/" #metric, kmonitor::GAUGE, unit)

    if (metricPref == FSMP_ONLINE) {
        INIT_FILE_SYSTEM_METRIC(MmapLockFileLength, "byte");
        INIT_FILE_SYSTEM_METRIC(MmapNonLockFileLength, "byte");
        INIT_FILE_SYSTEM_METRIC(BlockFileLength, "byte");
        INIT_FILE_SYSTEM_METRIC(SliceFileLength, "byte");
        INIT_FILE_SYSTEM_METRIC(ResourceFileLength, "byte");
        INIT_FILE_SYSTEM_METRIC(ResourceFileCount, "count");
        INIT_FILE_SYSTEM_METRIC(BufferFileLength, "byte");
    }

    INIT_FILE_SYSTEM_METRIC(InMemFileLength, "byte");

    INIT_FILE_SYSTEM_METRIC(InMemStorageFlushMemoryUse, "byte");

    INIT_FILE_SYSTEM_METRIC(MemoryQuotaUse, "byte");

#undef INIT_FILE_SYSTEM_METRIC
}

void FileSystemMetricsReporter::ReportMetrics(const FileSystemMetrics& metrics) noexcept
{
    ReportStorageMetrics(metrics.GetInputStorageMetrics(), metrics.GetOutputStorageMetrics());
    if (_decompressBufferLatency) {
        _decompressBufferLatency->Report();
    }
    if (_decompressBufferQps) {
        _decompressBufferQps->Report();
    }
    if (_decompressBufferCpuAvgTime) {
        _decompressBufferCpuAvgTime->Report();
    }
    if (_typedFileLenReporter) {
        _typedFileLenReporter->Report();
    }
    if (_fileNodeUseCountReporter) {
        _fileNodeUseCountReporter->Report();
    }
}

void FileSystemMetricsReporter::ReportMemoryQuotaUse(size_t memoryQuotaUse) noexcept
{
    IE_REPORT_METRIC(MemoryQuotaUse, memoryQuotaUse);
}

void FileSystemMetricsReporter::ReportStorageMetrics(const StorageMetrics& inputStorageMetrics,
                                                     const StorageMetrics& outputStorageMetrics) noexcept
{
    IE_REPORT_METRIC(InMemFileLength, inputStorageMetrics.GetFileLength(FSMG_MEM, FSFT_MEM) +
                                          inputStorageMetrics.GetFileLength(FSMG_LOCAL, FSFT_MEM) +
                                          outputStorageMetrics.GetFileLength(FSMG_MEM, FSFT_MEM) +
                                          outputStorageMetrics.GetFileLength(FSMG_LOCAL, FSFT_MEM));
    IE_REPORT_METRIC(MmapLockFileLength, inputStorageMetrics.GetMmapLockFileLength(FSMG_MEM) +
                                             inputStorageMetrics.GetMmapLockFileLength(FSMG_LOCAL) +
                                             outputStorageMetrics.GetMmapLockFileLength(FSMG_MEM) +
                                             outputStorageMetrics.GetMmapLockFileLength(FSMG_LOCAL));
    IE_REPORT_METRIC(MmapNonLockFileLength, inputStorageMetrics.GetMmapNonlockFileLength(FSMG_MEM) +
                                                inputStorageMetrics.GetMmapNonlockFileLength(FSMG_LOCAL) +
                                                outputStorageMetrics.GetMmapNonlockFileLength(FSMG_MEM) +
                                                outputStorageMetrics.GetMmapNonlockFileLength(FSMG_LOCAL));
    IE_REPORT_METRIC(BlockFileLength, inputStorageMetrics.GetFileLength(FSMG_MEM, FSFT_BLOCK) +
                                          inputStorageMetrics.GetFileLength(FSMG_LOCAL, FSFT_BLOCK) +
                                          outputStorageMetrics.GetFileLength(FSMG_MEM, FSFT_BLOCK) +
                                          outputStorageMetrics.GetFileLength(FSMG_LOCAL, FSFT_BLOCK));
    IE_REPORT_METRIC(BufferFileLength, inputStorageMetrics.GetFileLength(FSMG_MEM, FSFT_BUFFERED) +
                                           inputStorageMetrics.GetFileLength(FSMG_LOCAL, FSFT_BUFFERED) +
                                           outputStorageMetrics.GetFileLength(FSMG_MEM, FSFT_BUFFERED) +
                                           outputStorageMetrics.GetFileLength(FSMG_LOCAL, FSFT_BUFFERED));
    IE_REPORT_METRIC(SliceFileLength, inputStorageMetrics.GetFileLength(FSMG_MEM, FSFT_SLICE) +
                                          inputStorageMetrics.GetFileLength(FSMG_LOCAL, FSFT_SLICE) +
                                          outputStorageMetrics.GetFileLength(FSMG_MEM, FSFT_SLICE) +
                                          outputStorageMetrics.GetFileLength(FSMG_LOCAL, FSFT_SLICE));
    IE_REPORT_METRIC(ResourceFileLength, inputStorageMetrics.GetFileLength(FSMG_MEM, FSFT_RESOURCE) +
                                             inputStorageMetrics.GetFileLength(FSMG_LOCAL, FSFT_RESOURCE) +
                                             outputStorageMetrics.GetFileLength(FSMG_MEM, FSFT_RESOURCE) +
                                             outputStorageMetrics.GetFileLength(FSMG_LOCAL, FSFT_RESOURCE));
    IE_REPORT_METRIC(ResourceFileCount, inputStorageMetrics.GetFileCount(FSMG_MEM, FSFT_RESOURCE) +
                                            inputStorageMetrics.GetFileCount(FSMG_LOCAL, FSFT_RESOURCE) +
                                            outputStorageMetrics.GetFileCount(FSMG_MEM, FSFT_RESOURCE) +
                                            outputStorageMetrics.GetFileCount(FSMG_LOCAL, FSFT_RESOURCE));

    IE_REPORT_METRIC(InMemStorageFlushMemoryUse, outputStorageMetrics.GetFlushMemoryUse());

    IE_REPORT_METRIC(InMemCacheFileCount, outputStorageMetrics.GetTotalFileCount());

    IE_REPORT_METRIC(LocalCacheFileCount, inputStorageMetrics.GetTotalFileCount());
}

void FileSystemMetricsReporter::ReportCompressTrainingLatency(int64_t latency,
                                                              const kmonitor::MetricsTags* tags) noexcept
{
    IE_INCREASE_QPS_WITH_TAGS(CompressTrainingQps, tags);
    IE_REPORT_METRIC_WITH_TAGS(CompressTrainingLatency, tags, latency);
}

void FileSystemMetricsReporter::ReportCompressBufferLatency(int64_t latency, const kmonitor::MetricsTags* tags) noexcept
{
    IE_INCREASE_QPS_WITH_TAGS(CompressBufferQps, tags);
    IE_REPORT_METRIC_WITH_TAGS(CompressBufferLatency, tags, latency);
}

util::InputMetricReporterPtr
FileSystemMetricsReporter::DeclareDecompressBufferLatencyReporter(const map<string, string>& tagMap) noexcept
{
    if (_decompressBufferLatency) {
        return _decompressBufferLatency->DeclareMetricReporter(tagMap);
    }
    return util::InputMetricReporterPtr();
}

util::QpsMetricReporterPtr
FileSystemMetricsReporter::DeclareDecompressBufferQpsReporter(const map<string, string>& tagMap) noexcept
{
    if (_decompressBufferQps) {
        return _decompressBufferQps->DeclareMetricReporter(tagMap);
    }
    return util::QpsMetricReporterPtr();
}

util::CpuSlotQpsMetricReporterPtr
FileSystemMetricsReporter::DeclareDecompressBufferOneCpuSlotAvgTimeReporter(const map<string, string>& tagMap) noexcept
{
    if (_decompressBufferCpuAvgTime) {
        return _decompressBufferCpuAvgTime->DeclareMetricReporter(tagMap);
    }
    return util::CpuSlotQpsMetricReporterPtr();
}

util::StatusMetricReporterPtr
FileSystemMetricsReporter::DeclareFileLengthMetricReporter(const map<string, string>& tagMap) noexcept
{
    if (_typedFileLenReporter) {
        return _typedFileLenReporter->DeclareMetricReporter(tagMap);
    }
    return util::StatusMetricReporterPtr();
}

util::StatusMetricReporterPtr
FileSystemMetricsReporter::DeclareFileNodeUseCountMetricReporter(const map<string, string>& tagMap) noexcept
{
    if (_fileNodeUseCountReporter) {
        return _fileNodeUseCountReporter->DeclareMetricReporter(tagMap);
    }
    return util::StatusMetricReporterPtr();
}

void FileSystemMetricsReporter::ExtractCompressTagInfo(const string& filePath, const string& compressType,
                                                       size_t compressBlockSize,
                                                       const map<string, string>& compressParam,
                                                       map<string, string>& tagMap) const noexcept
{
    tagMap["compress_type"] = compressType;
    tagMap["compress_block_size"] = autil::StringUtil::toString(compressBlockSize);

    bool needHintData =
        indexlib::util::GetTypeValueFromKeyValueMap(compressParam, COMPRESS_ENABLE_HINT_PARAM_NAME, (bool)false);
    tagMap[COMPRESS_ENABLE_HINT_PARAM_NAME] = needHintData ? "true" : "false";
    auto iter = compressParam.find("compress_level");
    tagMap["compress_level"] = (iter == compressParam.end()) ? "default" : iter->second;
    ExtractTagInfoFromPath(filePath, tagMap);
}

void FileSystemMetricsReporter::ExtractTagInfoFromPath(const string& filePath,
                                                       map<string, string>& tagMap) const noexcept
{
    {
        autil::ScopedReadLock lock(_tagLock);
        for (auto it : _globalTags) {
            tagMap[it.first] = it.second;
        }
    }

    vector<string> pathSplitVec = autil::StringTokenizer::tokenize(autil::StringView(filePath), "/");
    if (pathSplitVec.empty()) {
        return;
    }

    tagMap["file_name"] = *pathSplitVec.rbegin();
    string segmentPattern = "^segment_[0-9]+_level_[0-9]+$";
    util::RegularExpression regex;
    regex.Init(segmentPattern);

    int32_t segmentDirIdx = -1;
    for (int32_t i = 0; i < pathSplitVec.size(); i++) {
        if (regex.Match(pathSplitVec[i])) {
            segmentDirIdx = i;
            size_t end = pathSplitVec[i].find("_level_");
            tagMap["segment_id"] = pathSplitVec[i].substr(8, end - 8);
            break;
        }
    }
    if (segmentDirIdx == -1) {
        tagMap["segment_id"] = "-1";
    }

    // segment_0_level_0/index/phrase/posting
    string dataType;
    string identifierStr;
    for (int32_t i = segmentDirIdx + 1; i < (int32_t)pathSplitVec.size() - 1; i++) {
        if (i == segmentDirIdx + 1) {
            dataType = pathSplitVec[i];
            continue;
        }
        if (!identifierStr.empty()) {
            identifierStr += "-";
        }
        identifierStr += pathSplitVec[i];
    }
    tagMap["data_type"] = dataType.empty() ? "_none_" : dataType;
    tagMap["identifier"] = identifierStr.empty() ? "_none_" : identifierStr;
}

}} // namespace indexlib::file_system
