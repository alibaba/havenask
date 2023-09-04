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
#ifndef FSLIB_METRICREPORTER_H
#define FSLIB_METRICREPORTER_H

#include "autil/Log.h"
#include "fslib/fslib.h"
#include "fslib/util/Singleton.h"
#include "kmonitor_adapter/Monitor.h"

FSLIB_BEGIN_NAMESPACE(util);
class MetricTagsHandler;
FSLIB_TYPEDEF_SHARED_PTR(MetricTagsHandler);
FSLIB_END_NAMESPACE(util);

FSLIB_BEGIN_NAMESPACE(fs);

class LatencyMetricReporter {
public:
    LatencyMetricReporter(const std::string &filePath, const std::string &opType);
    ~LatencyMetricReporter();

private:
    std::string _filePath;
    std::string _opType;
    int64_t _startTs;
};

class ReaderLatencyMetricReporter {
public:
    ReaderLatencyMetricReporter(const std::string &filePath);
    ~ReaderLatencyMetricReporter();

public:
    void setLength(size_t length) { _length = length; }

private:
    std::string _filePath;
    int64_t _startTs;
    size_t _length;
};

class WriterLatencyMetricReporter {
public:
    WriterLatencyMetricReporter(const std::string &filePath);
    ~WriterLatencyMetricReporter();

public:
    void setLength(size_t length) { _length = length; }

private:
    std::string _filePath;
    int64_t _startTs;
    size_t _length;
};

class MetricReporter : public util::Singleton<MetricReporter> {
public:
    MetricReporter();
    ~MetricReporter();

public:
    void close();

public:
    void reportErrorMetric(const std::string &filePath, const std::string &opType, double value = 1.0);
    void reportQpsMetric(const std::string &filePath, const std::string &opType, double value = 1.0);
    void reportLatencyMetric(const std::string &filePath, const std::string &opType, int64_t latency);

    void reportDNErrorQps(const std::string &filePath, const std::string &opType, double value = 1.0);
    void reportDNReadErrorQps(const std::string &filePath, double value = 1.0);
    void reportDNReadSpeed(const std::string &filePath, double speed);
    void reportDNReadLatency(const std::string &filePath, int64_t latency);
    void reportDNReadAvgLatency(const std::string &filePath, int64_t latency);

    void reportDNWriteErrorQps(const std::string &filePath, double value = 1.0);
    void reportDNWriteSpeed(const std::string &filePath, double speed);
    void reportDNWriteLatency(const std::string &filePath, int64_t latency);
    void reportDNWriteAvgLatency(const std::string &filePath, int64_t latency);

    void reportMetaCachedPathCount(int64_t fileCount);
    void reportMetaCacheImmutablePathCount(int64_t fileCount);
    void reportMetaCacheHitQps(const std::string &filePath, double value = 1.0);

    void reportDataCachedFileCount(int64_t fileCount);
    void reportDataCacheMemUse(int64_t size);
    void reportDataCacheHitQps(const std::string &filePath, double value = 1.0);

public:
    util::MetricTagsHandlerPtr getMetricTagsHandler();
    bool updateTagsHandler(util::MetricTagsHandlerPtr handler);
    bool enable() const;

private:
    void initMetric();

private:
    bool _enable;
    kmonitor_adapter::Monitor *_monitor;
    volatile size_t _handlerId;
    util::MetricTagsHandlerPtr _tagsHandlers[2];
    autil::ThreadMutex _updateLock;

    kmonitor_adapter::MetricPtr _metaQps;
    kmonitor_adapter::MetricPtr _metaErrorQps;
    kmonitor_adapter::MetricPtr _metaLatency;

    kmonitor_adapter::MetricPtr _dataErrorQps;
    kmonitor_adapter::MetricPtr _dataReadErrorQps;
    kmonitor_adapter::MetricPtr _dataReadSpeed; // bytes
    kmonitor_adapter::MetricPtr _dataReadLatency;
    kmonitor_adapter::MetricPtr _dataReadAvgLatency;

    kmonitor_adapter::MetricPtr _dataWriteErrorQps;
    kmonitor_adapter::MetricPtr _dataWriteSpeed; // bytes
    kmonitor_adapter::MetricPtr _dataWriteLatency;
    kmonitor_adapter::MetricPtr _dataWriteAvgLatency;

    kmonitor_adapter::MetricPtr _metaCachedPathCount;
    kmonitor_adapter::MetricPtr _metaCacheImmutablePathCount;
    kmonitor_adapter::MetricPtr _metaCacheHitQps;

    kmonitor_adapter::MetricPtr _dataCachedFileCount;
    kmonitor_adapter::MetricPtr _dataCacheMemUse;
    kmonitor_adapter::MetricPtr _dataCacheHitQps;

    static const size_t DEFAULT_HANDLER = 0;
    static const size_t USER_HANDLER = 1;
};

FSLIB_TYPEDEF_SHARED_PTR(MetricReporter);

FSLIB_END_NAMESPACE(fs);

#endif // FSLIB_METRICREPORTER_H
