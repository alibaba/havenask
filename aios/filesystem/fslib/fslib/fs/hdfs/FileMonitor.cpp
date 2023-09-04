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
#include "fslib/fs/hdfs/FileMonitor.h"

#include "autil/StringUtil.h"
#include "fslib/util/URLParser.h"

using namespace std;

FSLIB_PLUGIN_BEGIN_NAMESPACE(hdfs);

FileMonitor::FileMonitor(PanguMonitor *panguMonitor, const std::string &fileName) {
    if (panguMonitor && panguMonitor->valid()) {
        _panguMonitor = panguMonitor;
        kmonitor::MetricsTags tags;
        initTag(tags, fileName);
        _writeLatencyMetric = _panguMonitor->_writeLatencyMetric->declareLightMetric(tags);
        _writeSpeedMetric = _panguMonitor->_writeSpeedMetric->declareLightMetric(tags);
        _writeSizeMetric = _panguMonitor->_writeSizeMetric->declareLightMetric(tags);
        _readLatencyMetric = _panguMonitor->_readLatencyMetric->declareLightMetric(tags);
        _readSpeedMetric = _panguMonitor->_readSpeedMetric->declareLightMetric(tags);
        _readSizeMetric = _panguMonitor->_readSizeMetric->declareLightMetric(tags);
    }
}

void FileMonitor::initTag(kmonitor::MetricsTags &tags, const string &path) {
    auto parseResult = util::URLParser::splitPath(path);
    tags.AddTag("dfsType", get<0>(parseResult).to_string());
    string dfsName(get<1>(parseResult).to_string());
    autil::StringUtil::replace(dfsName, ':', '-');
    tags.AddTag("dfsName", dfsName);
    tags.AddTag("link", "alb");
}

void FileMonitor::reportWriteSize(uint64_t size) {
    if (_panguMonitor) {
        _writeSizeMetric->report(size);
        _writeSpeedMetric->report(size);
    }
}

void FileMonitor::reportWriteLatency(uint64_t latency) {
    if (_panguMonitor) {
        _writeLatencyMetric->report(latency);
    }
}

void FileMonitor::reportReadSize(uint64_t size) {
    if (_panguMonitor) {
        _readSizeMetric->report(size);
        _readSpeedMetric->report(size);
    }
}

void FileMonitor::reportReadLatency(uint64_t latency) {
    if (_panguMonitor) {
        _readLatencyMetric->report(latency);
    }
}

FSLIB_PLUGIN_END_NAMESPACE(hdfs);
