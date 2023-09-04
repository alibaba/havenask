/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 10:05
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#include "kmonitor/client/core/MetricsManager.h"

#include <map>
#include <set>
#include <string>

#include "autil/HashAlgorithm.h"
#include "kmonitor/client/core/MetricsCache.h"
#include "kmonitor/client/core/MetricsCollector.h"
#include "kmonitor/client/core/MetricsData.h"
#include "kmonitor/client/core/MetricsMap.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "kmonitor/client/metric/BuildInMetrics.h"
#include "kmonitor/client/metric/Metric.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);
AUTIL_LOG_SETUP(kmonitor, MetricsManager);

using std::map;
using std::set;
using std::string;

MetricsManager::MetricsManager(const string &name) {
    name_ = name;
    sharded_num_ = kShardedNum;
    sharded_metrics_ = new MetricsMap[sharded_num_];
    buildInMetrics_ = NULL;
}

MetricsManager::~MetricsManager() {
    delete[] sharded_metrics_;
    if (buildInMetrics_) {
        delete buildInMetrics_;
        buildInMetrics_ = NULL;
    }
}

void MetricsManager::Snapshot(MetricsCollector *collector, const set<MetricLevel> &levels, int64_t curTimeMs) {
    for (uint32_t i = 0; i < sharded_num_; ++i) {
        sharded_metrics_[i].Snapshot(collector, levels, curTimeMs);
    }
    autil::ScopedLock lock(buildin_mutex_);
    if (buildInMetrics_ != NULL) {
        buildInMetrics_->Snapshot(collector, curTimeMs, &levels);
    }
}

void MetricsManager::Register(const string &metric_name,
                              MetricType type,
                              MetricLevel level,
                              MetricsTagsManager *tags_manager,
                              MetricsCache *metricsCache) {
    uint32_t shared_id = GetShardId(metric_name);
    sharded_metrics_[shared_id].Register(metric_name, type, level, tags_manager, metricsCache);
    AUTIL_LOG(DEBUG, "register metrics success, metrics name[%s]", metric_name.c_str());
}

MutableMetric *MetricsManager::RegisterMetric(const std::string &metric_name,
                                              MetricType type,
                                              MetricLevel level,
                                              MetricsTagsManager *tags_manager,
                                              MetricsCache *metricsCache) {
    uint32_t shared_id = GetShardId(metric_name);
    MutableMetric *mutable_metric =
        sharded_metrics_[shared_id].RegisterMetric(metric_name, type, level, tags_manager, metricsCache);
    AUTIL_LOG(DEBUG, "register metrics success, metrics name [%s] sharded [%u]", metric_name.c_str(), shared_id);
    return mutable_metric;
}

void MetricsManager::Unregister(const string &metric_name) {
    uint32_t shared_id = GetShardId(metric_name);
    sharded_metrics_[shared_id].Unregister(metric_name);
    AUTIL_LOG(DEBUG, "unregister metrics success, metrics name[%s]", metric_name.c_str());
}

Metric *MetricsManager::GetMetric(const string &metric_name, const MetricsTagsPtr &tags) {
    uint32_t shard_id = GetShardId(metric_name);
    return sharded_metrics_[shard_id].GetMetric(metric_name, tags);
}

uint32_t MetricsManager::GetShardId(const string &metric_name) const {
    uint32_t hash_id = autil::HashAlgorithm::hashString64(metric_name.c_str());
    return hash_id % sharded_num_;
}

void MetricsManager::registerBuildInMetrics(const string &name, const MetricsTagsPtr &tags, MetricLevel level) {
    autil::ScopedLock lock(buildin_mutex_);
    if (buildInMetrics_ != NULL) {
        delete buildInMetrics_;
        buildInMetrics_ = NULL;
    }
    buildInMetrics_ = new BuildInMetrics(name, tags, level);
}

END_KMONITOR_NAMESPACE(kmonitor);
