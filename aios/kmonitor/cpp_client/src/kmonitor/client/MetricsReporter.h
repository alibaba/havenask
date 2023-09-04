/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2020-06-17
 * Author Name: dongdong.shan, wister.zwz
 * Author Email: dongdong.shan@alibaba-inc.com, wister.zwz@alibaba-inc.com
 * */

#pragma once
#include <atomic>
#include <map>
#include <unordered_map>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "kmonitor/client/KMonitor.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor/client/core/MutableMetric.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class MetricsGroup;
class MetricsGroupManager;
class MetricsReporter;
typedef std::shared_ptr<MetricsGroup> MetricsGroupPtr;
typedef std::shared_ptr<MetricsGroupManager> MetricsGroupManagerPtr;
typedef std::shared_ptr<MetricsReporter> MetricsReporterPtr;

class MetricsGroup {
public:
    MetricsGroup() {}
    virtual ~MetricsGroup() {}

public:
    virtual bool init(MetricsGroupManager *manager) = 0;

protected:
    AUTIL_LOG_DECLARE();
};

class MetricsGroupManager {
public:
    MetricsGroupManager(KMonitor *monitor, const std::string &metricsPath);

public:
    template <typename T>
    T *getMetricsGroup();
    MutableMetric *declareMutableMetrics(const std::string &name, MetricType metricType, MetricLevel level);
    void unregister(const std::string &name, const MetricsTags *tags);

private:
    KMonitor *_monitor;
    std::string _metricsPath;
    autil::ReadWriteLock _groupRwLock;
    autil::ReadWriteLock _metricsRwLock;
    std::unordered_map<std::string, MetricsGroupPtr> _metricsGroupCache;
    std::unordered_map<std::string, std::shared_ptr<MutableMetric>> _mutableMetricsCache;

private:
    AUTIL_LOG_DECLARE();
};

class MetricsReportSession {
public:
    MetricsReportSession(const MetricsReporter *reporter,
                         const MetricsTags *tags,
                         MetricLevel metricLevel = MetricLevel::NORMAL);

    [[deprecated("arg 'needSummary' is useless")]] MetricsReportSession &
    report(double value, const std::string &name, MetricType metricType, bool needSummary);
    MetricsReportSession &report(double value, const std::string &name, MetricType metricType);

    template <typename MetricsGroupT, typename MetricsValue>
    bool report(MetricsValue *value);

private:
    const MetricsReporter *_reporter;
    const MetricsTags *_tagsPtr;
    const MetricLevel _metricLevel;
    std::shared_ptr<MetricsTags> _tagsBuf;
};

class MetricsReporter {
    friend class MetricsReportSession;

public:
    // own kmonitor with metric prefix
    MetricsReporter(const std::string &metricPrefix, // for KMonitor service_name
                    const std::string &metricsPath,  // for MetricsReporter
                    const MetricsTags &tags,
                    const std::string &monitorNamePrefix = "");
    // own kmonitor with default prefix
    MetricsReporter(const std::string &metricsPath, const MetricsTags &tags, const std::string &monitorNamePrefix = "");
    MetricsReporter(KMonitorPtr monitor, const std::string &metricsPath, const MetricsTags &tags);
    MetricsReporter(KMonitorPtr monitor,
                    const std::string &metricsPath,
                    const MetricsTags &tags,
                    MetricsGroupManagerPtr metricsGroupManager);
    ~MetricsReporter();

private:
    MetricsReporter(const MetricsReporter &);
    MetricsReporter &operator=(const MetricsReporter &);

public:
    typedef std::pair<std::string, uint64_t> MetricsMapKeyType;
    typedef std::unordered_map<std::string, std::shared_ptr<MutableMetric>> UserMetricsMapType;
    typedef std::map<MetricsMapKeyType, MetricsReporterPtr> MetricsReporterMapType;

public:
    [[deprecated("arg 'needSummary' is useless")]] void
    report(double v, const std::string &name, MetricType metricType, const MetricsTags *tags, bool needSummary);
    void report(double v,
                const std::string &name,
                MetricType metricType,
                const MetricsTags *tags,
                MetricLevel metricLevel = MetricLevel::NORMAL);
    MetricsReportSession getReportSession(const MetricsTags *tags, MetricLevel metricLevel = MetricLevel::NORMAL) const;

    template <typename MetricsGroupT, typename MetricsValue>
    bool report(const MetricsTags *tags, MetricsValue *value) const;

    MutableMetric *declareMutableMetrics(const std::string &name, MetricType metricType, MetricLevel metricLevel);

    MetricsReporterPtr getSubReporter(const std::string &subPath, const MetricsTags &newTags);
    MetricsReporterPtr
    newSubReporter(const std::string &subPath, const MetricsTags &newTags, bool shareMetricsGroup = false);
    void unregister(const std::string &name, const MetricsTags *tags = NULL);
    static void setMetricsReporterCacheLimit(size_t limit) { _staticMetricsReporterCacheLimit = limit; }
    template <typename T>
    T *getMetricsGroup() {
        return _metricsGroupManager->getMetricsGroup<T>();
    }
    const MetricsTags &getTags() const { return _tags; }

private:
    KMonitorPtr _monitor;
    std::string _monitorName;
    std::string _metricsPath;
    MetricsGroupManagerPtr _metricsGroupManager;
    autil::ReadWriteLock _metricsReporterCacheMutex;
    MetricsReporterMapType _metricsReporterCache;
    size_t _metricsReporterCacheLimit = 1 << 12; // TODO compatible ha3 plugin, remove it
    MetricsTags _tags;

private:
    static size_t _staticMetricsReporterCacheLimit;
    static std::atomic_uint _reporterCounter;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
T *MetricsGroupManager::getMetricsGroup() {
    static std::string typeName = typeid(T).name();
    {
        autil::ScopedReadLock lock(_groupRwLock);
        auto it = _metricsGroupCache.find(typeName);
        if (it != _metricsGroupCache.end()) {
            // typeName must match MetricsGroup's type, dynamic cast is not needed
            return static_cast<T *>(it->second.get());
        }
    }
    {
        autil::ScopedWriteLock lock(_groupRwLock);
        auto it = _metricsGroupCache.find(typeName);
        if (it != _metricsGroupCache.end()) {
            return static_cast<T *>(it->second.get());
        }
        T *metricsGroup = new T();
        MetricsGroupPtr metricsGroupPtr(metricsGroup);
        if (!metricsGroup->init(this)) {
            AUTIL_LOG(WARN, "metrics group [%s] init failed.", typeName.c_str());
            return nullptr;
        }
        _metricsGroupCache.insert(make_pair(typeName, metricsGroupPtr));
        return metricsGroup;
    }
}

template <typename MetricsGroupT, typename MetricsValue>
bool MetricsReportSession::report(MetricsValue *value) {
    return _reporter->report<MetricsGroupT, MetricsValue>(_tagsPtr, value);
}

template <typename MetricsGroupT, typename MetricsValue>
bool MetricsReporter::report(const MetricsTags *tags, MetricsValue *value) const {
    MetricsGroupT *metricsGroup = _metricsGroupManager->getMetricsGroup<MetricsGroupT>();
    if (metricsGroup == nullptr) {
        return false;
    }
    if (tags == nullptr) {
        metricsGroup->report(&_tags, value);
    } else {
        if (_tags.Size() != 0) {
            MetricsTags mergeTags = _tags;
            tags->MergeTags(&mergeTags);
            metricsGroup->report(&mergeTags, value);
        } else {
            metricsGroup->report(tags, value);
        }
    }
    return true;
}

END_KMONITOR_NAMESPACE(kmonitor);
