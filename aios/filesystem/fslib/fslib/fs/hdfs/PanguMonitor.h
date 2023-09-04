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
#include "kmonitor_adapter/Monitor.h"

FSLIB_PLUGIN_BEGIN_NAMESPACE(hdfs);

class PanguMonitor {
public:
    PanguMonitor(kmonitor_adapter::Monitor *monitor);

public:
    void init(kmonitor_adapter::Monitor *monitor);
    bool valid() const { return _monitor != NULL; }

private:
    friend class FileMonitor;
    kmonitor_adapter::Monitor *_monitor = NULL;
    kmonitor_adapter::MetricPtr _writeLatencyMetric;
    kmonitor_adapter::MetricPtr _writeSizeMetric;
    kmonitor_adapter::MetricPtr _writeSpeedMetric;

    kmonitor_adapter::MetricPtr _readLatencyMetric;
    kmonitor_adapter::MetricPtr _readSizeMetric;
    kmonitor_adapter::MetricPtr _readSpeedMetric;
};

FSLIB_PLUGIN_END_NAMESPACE(hdfs);
