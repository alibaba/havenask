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

#include "indexlib/file_system/FileSystemMetricsReporter.h"
#include "indexlib/util/metrics/MetricReporter.h"

namespace indexlib { namespace file_system {

class DecompressMetricsReporter
{
public:
    DecompressMetricsReporter() {}
    ~DecompressMetricsReporter() {}

public:
    void Init(FileSystemMetricsReporter* reporter, const std::map<std::string, std::string>& tagMap) noexcept
    {
        if (!reporter) {
            return;
        }
        _latencyReporter = reporter->DeclareDecompressBufferLatencyReporter(tagMap);
        _qpsReporter = reporter->DeclareDecompressBufferQpsReporter(tagMap);
        _avgTimePerCpuSlotReporter = reporter->DeclareDecompressBufferOneCpuSlotAvgTimeReporter(tagMap);
    }

    void Report(uint64_t latency, bool trace = false)
    {
        if (_qpsReporter) {
            _qpsReporter->IncreaseQps(1, trace);
        }
        if (_latencyReporter) {
            _latencyReporter->Record(latency, trace);
        }
        if (_avgTimePerCpuSlotReporter) {
            _avgTimePerCpuSlotReporter->IncreaseQps(latency, trace);
        }
    }

private:
    util::QpsMetricReporterPtr _qpsReporter;
    util::InputMetricReporterPtr _latencyReporter;
    util::CpuSlotQpsMetricReporterPtr _avgTimePerCpuSlotReporter; // (value / 10000) show cpu percent in use

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DecompressMetricsReporter> DecompressMetricsReporterPtr;

class ScopedDecompressMetricReporter
{
public:
    ScopedDecompressMetricReporter(DecompressMetricsReporter& reporter, bool trace = false)
        : _reporter(reporter)
        , _initTs(autil::TimeUtility::currentTime())
        , _trace(trace)
    {
    }

    ~ScopedDecompressMetricReporter()
    {
        int64_t interval = autil::TimeUtility::currentTime() - _initTs;
        _reporter.Report(interval, _trace);
    }

private:
    DecompressMetricsReporter& _reporter;
    int64_t _initTs;
    bool _trace;
};

}} // namespace indexlib::file_system
