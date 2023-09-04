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

#include "fslib/common.h"
#include "fslib/fs/hdfs/PanguMonitor.h"
#include "kmonitor_adapter/Monitor.h"

FSLIB_PLUGIN_BEGIN_NAMESPACE(hdfs);

class FileMonitor {
public:
    FileMonitor(PanguMonitor *panguMonitor, const std::string &fileName);

    FileMonitor(const FileMonitor &other) = delete;
    FileMonitor &operator=(const FileMonitor &other) = delete;

public:
    void reportWriteLatency(uint64_t latency);
    void reportWriteSize(uint64_t size);

    void reportReadLatency(uint64_t latency);
    void reportReadSize(uint64_t size);

private:
    void initTag(kmonitor::MetricsTags &tags, const std::string &fileName);

private:
    PanguMonitor *_panguMonitor = NULL;
    kmonitor_adapter::LightMetricPtr _writeLatencyMetric;
    kmonitor_adapter::LightMetricPtr _writeSizeMetric;
    kmonitor_adapter::LightMetricPtr _writeSpeedMetric;

    kmonitor_adapter::LightMetricPtr _readLatencyMetric;
    kmonitor_adapter::LightMetricPtr _readSizeMetric;
    kmonitor_adapter::LightMetricPtr _readSpeedMetric;
};

FSLIB_PLUGIN_END_NAMESPACE(hdfs);
