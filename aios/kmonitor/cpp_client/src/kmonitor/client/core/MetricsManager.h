/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 10:05
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#ifndef KMONITOR_CLIENT_CORE_METRICSMANAGER_H_
#define KMONITOR_CLIENT_CORE_METRICSMANAGER_H_

#include <map>
#include <set>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/MetricType.h"
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/core/MetricsTags.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

static const uint32_t kShardedNum = 128;

class Metric;
class MetricsData;
class MetricsCollector;
class MetricsMap;
class MutableMetric;
class MetricsCache;
class MetricsTagsManager;
class BuildInMetrics;

class MetricsManager {
public:
    explicit MetricsManager(const std::string &name);
    ~MetricsManager();

public:
    void Snapshot(MetricsCollector *collector, const std::set<MetricLevel> &levels, int64_t curTimeMs);
    void Register(const std::string &metric_name,
                  MetricType type,
                  MetricLevel level,
                  MetricsTagsManager *tags_manager,
                  MetricsCache *metricsCache = NULL);
    MutableMetric *RegisterMetric(const std::string &metric_name,
                                  MetricType type,
                                  MetricLevel level,
                                  MetricsTagsManager *tags_manager,
                                  MetricsCache *metricsCache = NULL);
    Metric *GetMetric(const std::string &metric_name, const MetricsTagsPtr &tags);
    void Unregister(const std::string &metric_name);

    void registerBuildInMetrics(const std::string &name, const MetricsTagsPtr &tags, MetricLevel level = NORMAL);

private:
    MetricsManager(const MetricsManager &);
    MetricsManager &operator=(const MetricsManager &);

private:
    uint32_t GetShardId(const std::string &metric_name) const;

private:
    std::string name_;
    uint32_t sharded_num_;
    MetricsMap *sharded_metrics_;
    autil::ThreadMutex buildin_mutex_;
    BuildInMetrics *buildInMetrics_;

private:
    friend class MetricsManagerTest_TestUnRegister_Test;
    friend class MetricsManagerTest_TestRegister_Test;
    friend class MetricsManagerTest_TestClear_Test;

private:
    AUTIL_LOG_DECLARE();
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_CORE_METRICSMANAGER_H_
