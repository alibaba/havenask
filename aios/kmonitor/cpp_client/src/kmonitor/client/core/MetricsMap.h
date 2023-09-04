/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-07-31 11:20
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#ifndef KMONITOR_CLIENT_CORE_METRICSMAP_H_
#define KMONITOR_CLIENT_CORE_METRICSMAP_H_

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

class Metric;
class MetricsTags;
class MetricsData;
class MetricsCollector;
class MutableMetric;
class MetricsCache;
class MetricsTagsManager;

class MetricsMap {
public:
    MetricsMap();
    ~MetricsMap();

public:
    void Snapshot(MetricsCollector *collector, const std::set<MetricLevel> &levels, int64_t curTimeMs);
    void Register(const std::string &metric_name,
                  MetricType type,
                  MetricLevel level,
                  MetricsTagsManager *tags_manager = NULL,
                  MetricsCache *metricsCache = NULL);
    MutableMetric *RegisterMetric(const std::string &metric_name,
                                  MetricType type,
                                  MetricLevel level,
                                  MetricsTagsManager *tags_manager = NULL,
                                  MetricsCache *metricsCache = NULL);
    Metric *GetMetric(const std::string &metric_name, const MetricsTagsPtr &tags);
    void Unregister(const std::string &metric_name);

private:
    MetricsMap(const MetricsMap &);
    MetricsMap &operator=(const MetricsMap &);

private:
    std::map<std::string, MetricsData *> metric_data_map_;
    MetricLevelManager level_manager_;
    autil::ThreadMutex metric_mutex_;

private:
    friend class MetricsMapTest_TestRegister_Test;
    friend class MetricsMapTest_TestUnRegister_Test;
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_CORE_METRICSMAP_H_
