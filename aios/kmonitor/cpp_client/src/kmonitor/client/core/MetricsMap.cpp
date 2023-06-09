/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-07-31 11:20
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#include <set>
#include <string>
#include <map>
#include "kmonitor/client/core/MetricsData.h"
#include "kmonitor/client/core/MetricsCollector.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "kmonitor/client/core/MetricsMap.h"
#include "kmonitor/client/core/MetricsCache.h"
#include "kmonitor/client/common/Util.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

using std::string;
using std::set;
using std::map;

MetricsMap::MetricsMap() {
}

MetricsMap::~MetricsMap() {
    map<string, MetricsData*>::iterator iter = metric_data_map_.begin();
    for (; iter != metric_data_map_.end(); iter++) {
        delete iter->second;
    }
}

void MetricsMap::Snapshot(MetricsCollector* collector, const set<MetricLevel>& levels, int64_t curTimeMs) {
    autil::ScopedLock lock(metric_mutex_);
    for (auto iter = levels.cbegin(); iter != levels.cend(); ++iter) {
        auto period = (int64_t)MetricLevelManager::GetLevelPeriod(*iter) * 1000;
        curTimeMs = Util::TimeAlign(curTimeMs, period);
        set<string> metrics = level_manager_.GetMetric(*iter);
        for (auto it = metrics.cbegin(); it != metrics.cend(); ++it) {
            map<string, MetricsData*>::iterator it_data = metric_data_map_.find(*it);
            if (it_data != metric_data_map_.end()) {
                it_data->second->Snapshot(collector, curTimeMs);
            }
        }
    }
}

void MetricsMap::Register(const string &metric_name, MetricType type, MetricLevel level,
                          MetricsTagsManager *tags_manager, MetricsCache *metricsCache) {
    autil::ScopedLock lock(metric_mutex_);
    map<string, MetricsData*>::iterator iter = metric_data_map_.find(metric_name);
    if (iter == metric_data_map_.end()) {
        MetricsData *data = new MetricsData(metric_name, type, level, tags_manager, metricsCache);
        metric_data_map_.insert(make_pair(metric_name, data));
    }
    level_manager_.AddMetricLevel(metric_name, level);
}

MutableMetric* MetricsMap::RegisterMetric(const std::string &metric_name, MetricType type,
        MetricLevel level, MetricsTagsManager* tags_manager, MetricsCache* metricsCache)
{
    autil::ScopedLock lock(metric_mutex_);
    MutableMetric *mutable_metric = NULL;
    map<string, MetricsData*>::iterator iter = metric_data_map_.find(metric_name);
    if (iter == metric_data_map_.end()) {
        MetricsData *data = new MetricsData(metric_name, type, level, tags_manager, metricsCache);
        metric_data_map_.insert(make_pair(metric_name, data));
        mutable_metric = new MutableMetric(data, metric_name, tags_manager);
    } else {
        mutable_metric = new MutableMetric(iter->second, metric_name, tags_manager);
    }
    level_manager_.AddMetricLevel(metric_name, level);
    return mutable_metric;
}


void MetricsMap::Unregister(const string &metric_name) {
    autil::ScopedLock lock(metric_mutex_);
    map<string, MetricsData *>::iterator iter = metric_data_map_.find(metric_name);
    if (iter != metric_data_map_.end()) {
        iter->second->Unregister();
        delete iter->second;
        metric_data_map_.erase(metric_name);
    }
    level_manager_.RemoveMetricLevel(metric_name);
}

Metric* MetricsMap::GetMetric(const string &metric_name, const MetricsTagsPtr& tags) {
    autil::ScopedLock lock(metric_mutex_);
    Metric *metric = NULL;
    map<string, MetricsData*>::iterator iter = metric_data_map_.find(metric_name);
    if (iter != metric_data_map_.end()) {
        metric = iter->second->GetMetric(tags);
    }
    return metric;
}


END_KMONITOR_NAMESPACE(kmonitor);
