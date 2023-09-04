/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2020-06-17
 * Author Name: dongdong.shan, wister.zwz
 * Author Email: dongdong.shan@alibaba-inc.com, wister.zwz@alibaba-inc.com
 * */

#include "kmonitor/client/MetricsReporter.h"

#include "kmonitor/client/KMonitorFactory.h"

using namespace std;
using namespace autil;

BEGIN_KMONITOR_NAMESPACE(kmonitor);

AUTIL_LOG_SETUP(kmonitor, MetricsGroupManager);
AUTIL_LOG_SETUP(kmonitor, MetricsReporter);
AUTIL_LOG_SETUP(kmonitor, MetricsGroup);

size_t MetricsReporter::_staticMetricsReporterCacheLimit = 1 << 12;

MetricsGroupManager::MetricsGroupManager(KMonitor *monitor, const std::string &metricsPath)
    : _monitor(monitor), _metricsPath(metricsPath) {}

MutableMetric *
MetricsGroupManager::declareMutableMetrics(const std::string &name, MetricType metricType, MetricLevel level) {
    {
        ScopedReadLock lock(_metricsRwLock);
        auto it = _mutableMetricsCache.find(name);
        if (it != _mutableMetricsCache.end()) {
            return it->second.get();
        }
    }
    {
        ScopedWriteLock lock(_metricsRwLock); // double check
        auto it = _mutableMetricsCache.find(name);
        if (it != _mutableMetricsCache.end()) {
            return it->second.get();
        }
        string sep;
        if (!_metricsPath.empty() && !name.empty()) {
            sep = ".";
        }
        string metricName = _metricsPath + sep + name;
        auto *mutableMetric = _monitor->RegisterMetric(metricName, metricType, level);
        _mutableMetricsCache.insert({name, MutableMetricPtr(mutableMetric)});
        return mutableMetric;
    }
}

void MetricsGroupManager::unregister(const string &name, const MetricsTags *tags) {
    ScopedReadLock lock(_metricsRwLock);
    // should not clear mutable metric cache
    string sep;
    if (!_metricsPath.empty() && !name.empty()) {
        sep = ".";
    }
    string metricName = _metricsPath + sep + name;
    Metric *metric = _monitor->DeclareMetric(metricName, tags);
    _monitor->UndeclareMetric(metric);
}

atomic_uint MetricsReporter::_reporterCounter(0);

MetricsReporter::MetricsReporter(const string &metricPrefix,
                                 const string &metricsPath,
                                 const MetricsTags &tags,
                                 const string &monitorNamePrefix)
    : MetricsReporter(metricsPath, tags, monitorNamePrefix) {
    _monitor->SetServiceName(metricPrefix);
}

MetricsReporter::MetricsReporter(const string &metricsPath, const MetricsTags &tags, const string &monitorNamePrefix)
    : _metricsPath(metricsPath), _tags(tags) {
    _monitorName = monitorNamePrefix + "_" + std::to_string(++_reporterCounter) + "_reporter";
    _monitor.reset(KMonitorFactory::GetKMonitor(_monitorName, true),
                   [](KMonitor *monitor) { KMonitorFactory::ReleaseKMonitor(monitor->Name()); });
    _metricsGroupManager.reset(new MetricsGroupManager(_monitor.get(), _metricsPath));
}

MetricsReporter::MetricsReporter(KMonitorPtr monitor, const string &metricsPath, const MetricsTags &tags)
    : _monitor(monitor)
    , _metricsPath(metricsPath)
    , _metricsGroupManager(new MetricsGroupManager(_monitor.get(), metricsPath))
    , _tags(tags) {}

MetricsReporter::MetricsReporter(KMonitorPtr monitor,
                                 const std::string &metricsPath,
                                 const MetricsTags &tags,
                                 MetricsGroupManagerPtr metricsGroupManager)
    : _monitor(monitor), _metricsPath(metricsPath), _metricsGroupManager(metricsGroupManager), _tags(tags) {}

MetricsReporter::~MetricsReporter() { _metricsReporterCache.clear(); }

void MetricsReporter::report(
    double value, const string &name, MetricType metricType, const MetricsTags *tags, bool needSummary) {
    report(value, name, metricType, tags, MetricLevel::NORMAL);
}

void MetricsReporter::report(
    double value, const std::string &name, MetricType metricType, const MetricsTags *tags, MetricLevel metricLevel) {
    auto metric = _metricsGroupManager->declareMutableMetrics(name, metricType, metricLevel);
    if (tags == nullptr) {
        metric->Report(&_tags, value);
    } else {
        MetricsTags mergeTags = _tags;
        tags->MergeTags(&mergeTags);
        metric->Report(&mergeTags, value);
    }
}

MetricsReportSession::MetricsReportSession(const MetricsReporter *reporter,
                                           const MetricsTags *tags,
                                           MetricLevel metricLevel)
    : _reporter(reporter), _metricLevel(metricLevel) {
    if (tags == nullptr) {
        _tagsPtr = &_reporter->_tags;
    } else {
        _tagsBuf.reset(new MetricsTags(_reporter->_tags));
        tags->MergeTags(_tagsBuf.get());
        _tagsPtr = _tagsBuf.get();
    }
}

MetricsReportSession &
MetricsReportSession::report(double value, const string &name, MetricType metricType, bool needSummary) {
    return report(value, name, metricType);
}

MetricsReportSession &MetricsReportSession::report(double value, const string &name, MetricType metricType) {
    auto metric = _reporter->_metricsGroupManager->declareMutableMetrics(name, metricType, _metricLevel);
    metric->Report(_tagsPtr, value);
    return *this;
}

MetricsReportSession MetricsReporter::getReportSession(const MetricsTags *tags, MetricLevel metricLevel) const {
    return MetricsReportSession(this, tags, metricLevel);
}

MutableMetric *
MetricsReporter::declareMutableMetrics(const std::string &name, MetricType metricType, MetricLevel metricLevel) {
    return _metricsGroupManager->declareMutableMetrics(name, metricType, metricLevel);
}

MetricsReporterPtr MetricsReporter::getSubReporter(const string &subMetricsPath, const MetricsTags &newTags) {
    uint64_t tagsHash = newTags.Hashcode();
    auto key = std::make_pair(subMetricsPath, tagsHash);
    {
        ScopedReadLock lock(_metricsReporterCacheMutex);
        auto it = _metricsReporterCache.find(key);
        if (it != _metricsReporterCache.end()) {
            return it->second;
        }
    }
    {
        ScopedWriteLock lock(_metricsReporterCacheMutex);
        auto it = _metricsReporterCache.find(key);
        if (it != _metricsReporterCache.end()) {
            return it->second;
        }
        if (_metricsReporterCache.size() >= _staticMetricsReporterCacheLimit) {
            AUTIL_LOG(WARN,
                      "subReporter cache limit exceed, kmonitorName=[%s] limit=[%lu], actual=[%lu]",
                      _monitor->Name().c_str(),
                      _metricsReporterCacheLimit,
                      _metricsReporterCache.size());
            _metricsReporterCache.clear();
        }

        MetricsReporterPtr metricsManager = newSubReporter(subMetricsPath, newTags, true);
        _metricsReporterCache.insert(make_pair(key, metricsManager));
        return metricsManager;
    }
}

MetricsReporterPtr
MetricsReporter::newSubReporter(const string &subMetricsPath, const MetricsTags &newTags, bool shareMetricsGroup) {
    MetricsTags mergeTags = _tags;
    newTags.MergeTags(&mergeTags);

    MetricsReporterPtr metricsManager;
    if (subMetricsPath.empty()) {
        if (shareMetricsGroup) {
            // use same MetricsGroupManager
            metricsManager.reset(new MetricsReporter(_monitor, _metricsPath, mergeTags, _metricsGroupManager));
        } else {
            metricsManager.reset(new MetricsReporter(_monitor, _metricsPath, mergeTags));
        }
    } else {
        // create new MetricsGroupManager
        string sep;
        if (!_metricsPath.empty()) {
            sep = ".";
        }
        string metricsPath = _metricsPath + sep + subMetricsPath;
        metricsManager.reset(new MetricsReporter(_monitor, metricsPath, mergeTags));
    }
    return metricsManager;
}

void MetricsReporter::unregister(const string &name, const MetricsTags *tags) {
    if (tags != NULL) {
        MetricsTags mergeTags = _tags;
        tags->MergeTags(&mergeTags);
        _metricsGroupManager->unregister(name, &mergeTags);
    } else {
        _metricsGroupManager->unregister(name, &_tags);
    }
}

END_KMONITOR_NAMESPACE(kmonitor);
