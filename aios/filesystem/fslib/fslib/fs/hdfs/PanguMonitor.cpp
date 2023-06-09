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
#include "autil/StringUtil.h"
#include "fslib/fs/hdfs/PanguMonitor.h"

using namespace std;

FSLIB_PLUGIN_BEGIN_NAMESPACE(hdfs);

PanguMonitor::PanguMonitor(kmonitor_adapter::Monitor* monitor)
    : _monitor(monitor)
{
    init(_monitor);
}

void PanguMonitor::init(kmonitor_adapter::Monitor* monitor)
{
    if (unlikely(!monitor)) {
        return;
    }

    kmonitor_adapter::Monitor::KVVec tags;
    // FileMoitor
    _writeLatencyMetric = monitor->registerLatencyMetric(
            "pangu.write_latency", kmonitor::FATAL, tags);
    _writeSpeedMetric = monitor->registerQPSMetric(
            "pangu.write_speed", kmonitor::FATAL, tags);
    _writeSizeMetric = monitor->registerGaugeMetric(
            "pangu.write_size", kmonitor::NORMAL, tags);

    _readLatencyMetric = monitor->registerLatencyMetric(
            "pangu.read_latency", kmonitor::FATAL, tags);
    _readSpeedMetric = monitor->registerQPSMetric(
            "pangu.read_speed", kmonitor::FATAL, tags);
    _readSizeMetric = monitor->registerGaugeMetric(
            "pangu.read_size", kmonitor::NORMAL, tags);
}

FSLIB_PLUGIN_END_NAMESPACE(hdfs);
