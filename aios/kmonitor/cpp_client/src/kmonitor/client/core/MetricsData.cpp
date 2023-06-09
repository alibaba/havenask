/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 10:04
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#include <string>
#include <vector>
#include <map>
#include "autil/Log.h"
#include "kmonitor/client/core/MetricsFactory.h"
#include "kmonitor/client/metric/Metric.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor/client/core/MetricsCollector.h"
#include "kmonitor/client/core/MetricsData.h"
#include "kmonitor/client/core/MetricsTagsManager.h"
#include "kmonitor/client/core/MetricsCache.h"
#include "autil/Log.h"


//因为会复用，所以MAX_METRIC_POOL_SIZE这个可以较小
#define MAX_METRIC_POOL_SIZE 20

BEGIN_KMONITOR_NAMESPACE(kmonitor);
AUTIL_LOG_SETUP(kmonitor, MetricsData);

using std::string;
using std::vector;
using std::map;
using std::pair;
using namespace autil;

MetricsData::MetricsData(const string &name, MetricType type, MetricLevel level,
                         MetricsTagsManager *tags_manager, MetricsCache* metricsCache)
    : metric_name_(name)
    , type_(type)
    , level_(level)
    , registered_(true)
    , tags_manager_(tags_manager)
    , metricsCache_(metricsCache)
{
}

MetricsData::~MetricsData() {
    Metric *metric = NULL;
    auto it = metric_map_.begin();
    while (it != metric_map_.end()) {
        pair<MetricsTagsPtr, Metric*> & pair_metric = it->second;
        MetricsTagsPtr& tags = pair_metric.first;
        metric = pair_metric.second;

        tags.reset();
        delete metric;
        it = metric_map_.erase(it);
    }

    while (!metric_pool_.empty()) {
        metric = metric_pool_.front();
        metric_pool_.pop();
        delete metric;
    }
}

int MetricsData::Size() {
    ScopedReadLock lock(metric_rwlock_);
    return metric_map_.size();
}

Metric* MetricsData::GetMetric(const MetricsTags *tags) {
    if (!registered_) {
        AUTIL_LOG(ERROR, "can't get metric with unregistered, metric name:%s",
                     metric_name_.c_str());
        return nullptr;
    }
    uint64_t tagsHash = (tags == NULL) ? 0 : tags->Hashcode();
    {
        ScopedReadLock lock(metric_rwlock_);
        auto iter = metric_map_.find(tagsHash);
        if (iter != metric_map_.end()) {
            Metric *metric = iter->second.second;
            metric->Touch();
            return metric;
        }
    }
    ScopedWriteLock lock(metric_rwlock_);
    auto iter = metric_map_.find(tagsHash);
    if (iter != metric_map_.end()) {
        Metric *metric = iter->second.second;
        metric->Touch();
        return metric;
    }
    MetricsTagsPtr tagsPtr;
    if (tags != nullptr) {
        if (tags_manager_ != nullptr) {
            tagsPtr = tags_manager_->GetMetricsTags(tags->GetTagsMap());
        } else {
            tagsPtr.reset(new MetricsTags(*tags)); // for test
        }
    } else {
        if (tags_manager_ != nullptr) {
            tagsPtr = tags_manager_->GetMetricsTags({});
        } else {
            tagsPtr.reset(new MetricsTags()); // for test
        }
    }
    if (tagsPtr == nullptr) {
        AUTIL_INTERVAL_LOG(200,
                           ERROR,
                           "tag is empty, metric [%s] tags [%s]",
                           metric_name_.c_str(),
                           tags ? tags->ToString().c_str() : "nullptr");
        return nullptr;
    }
    Metric *metric = nullptr;
    if (!metric_pool_.empty()) {
        metric = metric_pool_.front();
        metric_pool_.pop();
    } else {
        metric = MetricsFactory::CreateMetric(metric_name_, type_);
        if (metric == NULL) {
            AUTIL_LOG(ERROR, "can't create metric for %s type[%u]",
                         metric_name_.c_str(), type_);
            return nullptr;
        }
    }
    metric->Touch(); //都在MetricsData的锁里面，可以不加metric的锁
    metric_map_[tagsHash] = std::make_pair(tagsPtr, metric);
    return metric;
}

Metric* MetricsData::GetMetric(const MetricsTagsPtr& tags) {
    if (!registered_ || !tags) {
        AUTIL_LOG(ERROR, "can't get metric with unregistered or empty tags[%p], metric name:%s",
                     tags.get(), metric_name_.c_str());
        return nullptr;
    }
    {
        ScopedReadLock lock(metric_rwlock_);
        auto iter = metric_map_.find(tags->Hashcode());
        if (iter != metric_map_.end()) {
            Metric *metric = iter->second.second;
            metric->Touch();
            return metric;
        }
    }
    ScopedWriteLock lock(metric_rwlock_);
    auto iter = metric_map_.find(tags->Hashcode());
    if (iter != metric_map_.end()) {
        Metric *metric = iter->second.second;
        metric->Touch();
        return metric;
    }
    Metric *metric = NULL;
    if (!metric_pool_.empty()) {
        metric = metric_pool_.front();
        metric_pool_.pop();
    } else {
        metric = MetricsFactory::CreateMetric(metric_name_, type_);
        if (metric == NULL) {
            AUTIL_LOG(ERROR, "can't create metric for %s type[%u]",
                         metric_name_.c_str(), type_);
            return metric;
        }
    }
    metric->Touch(); //都在MetricsData的锁里面，可以不加metric的锁
    metric_map_[tags->Hashcode()] = std::make_pair(tags, metric);
    return metric;
}

void MetricsData::Unregister() {
    registered_ = false;
}

MetricType MetricsData::GetMetricType() {
    return type_;
}

void MetricsData::Snapshot(MetricsCollector *collector, int64_t curTime) {
    vector<int64_t> toRemoveVec;
    {
        ScopedReadLock lock(metric_rwlock_);
        auto it = metric_map_.begin();
        while (it != metric_map_.end()) {
            pair<MetricsTagsPtr, Metric*> & pair_metric = it->second;
            MetricsTagsPtr& tags = pair_metric.first;
            Metric *metric = pair_metric.second;
            if (tags == NULL || metric == NULL) {
                AUTIL_LOG(ERROR, "error tags[%p] or metric[%p] for hashcode[%ld]",
                        tags.get(), metric, it->first);
                ++it;
                continue;
            }

            MetricsRecord *record = collector->AddRecord(metric_name_, tags, curTime);

            metric->Snapshot(record, 1000*(int64_t)MetricLevelManager::GetLevelPeriod(level_));
            AUTIL_LOG(DEBUG, "metrics[%s] tags[%s] is snapshot",
                         metric_name_.c_str(), tags->ToString().c_str());
            if (metric->canRecycle()) {
                AUTIL_LOG(DEBUG, "metrics[%s] tags[%s] is be recycled",
                        metric_name_.c_str(), tags->ToString().c_str());
                if (metricsCache_ != NULL) {
                    uint64_t tags_hash = (tags == NULL) ? 0 : tags->Hashcode();
                    metricsCache_->ClearCache(metric_name_, tags_hash, metric);
                }
                toRemoveVec.push_back(it->first);
            }
            ++it;
        }
    }
    if (!toRemoveVec.empty()) {
        ScopedWriteLock lock(metric_rwlock_);
        for (auto &key : toRemoveVec) {
            auto it = metric_map_.find(key);
            if (it == metric_map_.end()) {
                continue;
            }
            pair<MetricsTagsPtr, Metric*> & pair_metric = it->second;
            Metric *metric = pair_metric.second;
            size_t pool_size = metric_pool_.size();
            if (pool_size < MAX_METRIC_POOL_SIZE) {
                metric_pool_.push(metric);
            } else {
                delete metric;
            }
            metric_map_.erase(it);
        }
    }
}

END_KMONITOR_NAMESPACE(kmonitor);
