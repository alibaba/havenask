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
#include "fslib/fs/MetricReporter.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/MetricTagsHandler.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "kmonitor_adapter/MonitorFactory.h"

using namespace std;
using namespace autil;
FSLIB_USE_NAMESPACE(util);

FSLIB_BEGIN_NAMESPACE(fs);
AUTIL_DECLARE_AND_SETUP_LOGGER(fs, MetricReporter);

MetricReporter::MetricReporter()
    : _handlerId(DEFAULT_HANDLER)
{
    AUTIL_LOG(INFO, "begin init fslib metric.");
    _enable = false;
    const char* env = getenv(FSLIB_ENABLE_REPORT_METRIC);
    if (!env || !autil::StringUtil::fromString(env, _enable) || !_enable) {
        AUTIL_LOG(INFO, "fslib metric is disable.");
        return;
    }

    _tagsHandlers[_handlerId].reset(new MetricTagsHandler);

    const char* serviceName = getenv(FSLIB_KMONITOR_SERVICE_NAME);
    if (serviceName) {
        _monitor = kmonitor_adapter::MonitorFactory::getInstance()->createMonitor(
            string(serviceName));
    } else {
        _monitor = kmonitor_adapter::MonitorFactory::getInstance()->createMonitor(
            FSLIB_METRIC_DEFAULT_SERVICE_NAME);
    }
    
    if (_monitor == NULL) {
        AUTIL_LOG(WARN, "fslib create monitor failed!");
        _enable = false;
        return;
    }
    initMetric();
    AUTIL_LOG(INFO, "init fslib metric success.");
}

MetricReporter::~MetricReporter() {
}

#define FSLIB_REPORT_METRIC(METRIC, filePath, opType, value) {          \
        if (!_enable || METRIC == NULL) {                               \
            return;                                                     \
        }                                                               \
        kmonitor::MetricsTags tags;                                     \
        _tagsHandlers[_handlerId]->getTags(filePath, tags);             \
        tags.AddTag(FSLIB_METRIC_TAGS_OPTYPE, opType);                  \
        METRIC->report(tags, value);                                    \
}

#define FSLIB_REPORT_METRIC2(METRIC, filePath, value) {         \
        if (!_enable || METRIC == NULL) {                       \
            return;                                             \
        }                                                       \
        kmonitor::MetricsTags tags;                             \
        _tagsHandlers[_handlerId]->getTags(filePath, tags);     \
        METRIC->report(tags, value);                            \
}

#define FSLIB_REPORT_CACHE_METRIC(METRIC, value) {      \
        if (!_enable || METRIC == NULL) {               \
             return;                                    \
        }                                               \
        METRIC->report(value);                          \
    }
    

util::MetricTagsHandlerPtr MetricReporter::getMetricTagsHandler()
{
    return _tagsHandlers[_handlerId];
}

bool MetricReporter::enable() const
{
    return _enable;
}

bool MetricReporter::updateTagsHandler(util::MetricTagsHandlerPtr tagsHandler)
{
    autil::ScopedLock lock(_updateLock);
    if (_handlerId == USER_HANDLER) {
        AUTIL_LOG(WARN, "can not set user-defined MetricTagsHandler twice!");
        return false;
    }
    _tagsHandlers[USER_HANDLER] = tagsHandler;
    _handlerId = USER_HANDLER;
    return true;
}

void MetricReporter::initMetric()
{
    assert(_monitor);
    // metric for namenode/master
    _metaQps = _monitor->registerQPSMetric(
        FSLIB_METRIC_DFS_QPS, kmonitor::FATAL);
    _metaErrorQps = _monitor->registerQPSMetric(
        FSLIB_METRIC_DFS_ERROR_QPS, kmonitor::FATAL);
    _metaLatency = _monitor->registerGaugePercentileMetric(
        FSLIB_METRIC_DFS_LATENCY, kmonitor::FATAL);
    
    // metric for datanode/chunk
    _dataErrorQps = _monitor->registerQPSMetric(
        FSLIB_METRIC_DN_ERROR_QPS, kmonitor::FATAL);
    
    _dataReadErrorQps = _monitor->registerQPSMetric(
        FSLIB_METRIC_DN_READ_ERROR_QPS, kmonitor::FATAL);
    _dataReadSpeed = _monitor->registerQPSMetric(
        FSLIB_METRIC_DN_READ_SPEED, kmonitor::FATAL);
    _dataReadLatency = _monitor->registerGaugePercentileMetric(
        FSLIB_METRIC_DN_READ_LATENCY, kmonitor::FATAL);
    _dataReadAvgLatency = _monitor->registerGaugePercentileMetric(
        FSLIB_METRIC_DN_READ_AVG_LATENCY, kmonitor::FATAL);


    _dataWriteErrorQps = _monitor->registerQPSMetric(
        FSLIB_METRIC_DN_WRITE_ERROR_QPS, kmonitor::FATAL);
    _dataWriteSpeed = _monitor->registerQPSMetric(
        FSLIB_METRIC_DN_WRITE_SPEED, kmonitor::FATAL);
    _dataWriteLatency = _monitor->registerGaugePercentileMetric(
        FSLIB_METRIC_DN_WRITE_LATENCY, kmonitor::FATAL);
    _dataWriteAvgLatency = _monitor->registerGaugePercentileMetric(
        FSLIB_METRIC_DN_WRITE_AVG_LATENCY, kmonitor::FATAL);

    _metaCachedPathCount = _monitor->registerGaugeMetric(
            FSLIB_METRIC_META_CACHE_PATH_COUNT, kmonitor::NORMAL);
    _metaCacheImmutablePathCount = _monitor->registerGaugeMetric(
            FSLIB_METRIC_META_CACHE_IMMUTABLE_PATH_COUNT, kmonitor::NORMAL);
    _metaCacheHitQps = _monitor->registerQPSMetric(
            FSLIB_METRIC_META_CACHE_HIT_QPS, kmonitor::NORMAL);

    _dataCachedFileCount = _monitor->registerGaugeMetric(
            FSLIB_METRIC_DATA_CACHE_FILE_COUNT, kmonitor::NORMAL);
    _dataCacheMemUse = _monitor->registerGaugeMetric(
            FSLIB_METRIC_DATA_CACHE_MEM_USE, kmonitor::NORMAL);
    _dataCacheHitQps = _monitor->registerQPSMetric(
            FSLIB_METRIC_DATA_CACHE_HIT_QPS, kmonitor::NORMAL);
}

void MetricReporter::reportQpsMetric(const string& filePath,
                                     const string& opType,
                                     double value)
{
    FSLIB_REPORT_METRIC(_metaQps, filePath, opType, value);
}

void MetricReporter::reportErrorMetric(const string& filePath,
                                       const string& opType,
                                       double value)
{
    FSLIB_REPORT_METRIC(_metaErrorQps, filePath, opType, value);
}

void MetricReporter::reportLatencyMetric(const string& filePath,
                                         const string& opType,
                                         int64_t latency)
{
    FSLIB_REPORT_METRIC(_metaLatency, filePath, opType, latency);
}

void MetricReporter::reportDNErrorQps(const string& filePath,
                                      const string& opType,
                                      double value)
{
    FSLIB_REPORT_METRIC(_dataErrorQps, filePath, opType, value);
}

void MetricReporter::reportDNReadErrorQps(const string& filePath,
                                          double value)
{
    FSLIB_REPORT_METRIC2(_dataReadErrorQps, filePath, value);
}

void MetricReporter::reportDNReadSpeed(const string& filePath,
                                       double speed)
{
    FSLIB_REPORT_METRIC2(_dataReadSpeed, filePath, speed);
}

void MetricReporter::reportDNReadLatency(const string& filePath,
                                         int64_t latency)
{
    FSLIB_REPORT_METRIC2(_dataReadLatency, filePath, latency);
}

void MetricReporter::reportDNReadAvgLatency(const string& filePath,
                                            int64_t latency)
{
    FSLIB_REPORT_METRIC2(_dataReadAvgLatency, filePath, latency);
}

void MetricReporter::reportDNWriteErrorQps(const string& filePath,
                                           double value)
{
    FSLIB_REPORT_METRIC2(_dataWriteErrorQps, filePath, value);
}

void MetricReporter::reportDNWriteSpeed(const string& filePath,
                                        double speed)
{
    FSLIB_REPORT_METRIC2(_dataWriteSpeed, filePath, speed);
}

void MetricReporter::reportDNWriteLatency(const string& filePath,
                                          int64_t latency)
{
    FSLIB_REPORT_METRIC2(_dataWriteLatency, filePath, latency);
}

void MetricReporter::reportDNWriteAvgLatency(const string& filePath,
                                             int64_t latency)
{
    FSLIB_REPORT_METRIC2(_dataWriteAvgLatency, filePath, latency);
}

void MetricReporter::reportMetaCachedPathCount(int64_t fileCount)
{
    FSLIB_REPORT_CACHE_METRIC(_metaCachedPathCount, fileCount);
}
    
void MetricReporter::reportMetaCacheImmutablePathCount(int64_t fileCount)
{
    FSLIB_REPORT_CACHE_METRIC(_metaCacheImmutablePathCount, fileCount);
}

void MetricReporter::reportMetaCacheHitQps(const string& filePath, double value)
{
    FSLIB_REPORT_METRIC2(_metaCacheHitQps, filePath, value);
}
    
void MetricReporter::reportDataCachedFileCount(int64_t fileCount)
{
    FSLIB_REPORT_CACHE_METRIC(_dataCachedFileCount, fileCount);
}
    
void MetricReporter::reportDataCacheMemUse(int64_t size)
{
    FSLIB_REPORT_CACHE_METRIC(_dataCacheMemUse, size);
}

void MetricReporter::reportDataCacheHitQps(const string& filePath, double value)
{
    FSLIB_REPORT_METRIC2(_dataCacheHitQps, filePath, value);
}

LatencyMetricReporter::LatencyMetricReporter(const string& filePath, const string& opType)
    : _filePath(filePath)
    , _opType(opType)
{
    _startTs = TimeUtility::currentTime();
}

LatencyMetricReporter::~LatencyMetricReporter()
{
    FileSystem::reportLatencyMetric(_filePath, _opType, TimeUtility::currentTime() - _startTs);
}

ReaderLatencyMetricReporter::ReaderLatencyMetricReporter(const string& filePath)
    : _filePath(filePath)
{
    _startTs = TimeUtility::currentTime();
}

ReaderLatencyMetricReporter::~ReaderLatencyMetricReporter()
{
    FileSystem::reportDNReadLatency(_filePath, TimeUtility::currentTime() - _startTs);
    if (_length > 0) {
        FileSystem::reportDNReadAvgLatency(_filePath,
                (double)(TimeUtility::currentTime()-_startTs)*1024/_length);
    }
}

WriterLatencyMetricReporter::WriterLatencyMetricReporter(const string& filePath)
    : _filePath(filePath)
{
    _startTs = TimeUtility::currentTime();
}

WriterLatencyMetricReporter::~WriterLatencyMetricReporter()
{
    FileSystem::reportDNWriteLatency(_filePath, TimeUtility::currentTime() - _startTs);
    if (_length > 0) {
        FileSystem::reportDNWriteAvgLatency(_filePath,
                (double)(TimeUtility::currentTime()-_startTs)*1024/_length);
    }
}

void MetricReporter::close()
{
    _metaQps.reset();
    _metaErrorQps.reset();
    _metaLatency.reset();
    
    _dataErrorQps.reset();
    _dataReadErrorQps.reset();
    _dataReadSpeed.reset(); //bytes
    _dataReadLatency.reset();
    _dataReadAvgLatency.reset();
    
    _dataWriteErrorQps.reset();
    _dataWriteSpeed.reset(); //bytes
    _dataWriteLatency.reset();
    _dataWriteAvgLatency.reset();
    
    _metaCachedPathCount.reset();
    _metaCacheImmutablePathCount.reset();
    _metaCacheHitQps.reset();
    
    _dataCachedFileCount.reset();
    _dataCacheMemUse.reset();
    _dataCacheHitQps.reset();
    
}

FSLIB_END_NAMESPACE(fs);
