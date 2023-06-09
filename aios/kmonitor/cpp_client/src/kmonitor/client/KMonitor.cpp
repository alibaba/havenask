/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-28 10:02
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#include <set>
#include <string>
#include "autil/Log.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor/client/core/MetricsManager.h"
#include "kmonitor/client/core/MetricsCollector.h"
#include "kmonitor/client/core/MetricsSource.h"
#include "kmonitor/client/core/MetricsConfig.h"
#include "kmonitor/client/core/MetricsFactory.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "kmonitor/client/KMonitor.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);
AUTIL_LOG_SETUP(kmonitor, KMonitor);

using namespace std;

KMonitor::KMonitor() {
    metricsCache_ = NULL;
    manager_ = NULL;
    config_ = NULL;
    tags_manager_ = NULL;
    service_name_ = "";
    service_name_setted_ = false;
    tags_ = NULL;
}

KMonitor::KMonitor(const string &name, bool useMetricCache) {
    name_ = name;
    manager_ = new MetricsManager(name);
    config_ = NULL;
    if (useMetricCache) {
        metricsCache_ = new (std::nothrow)MetricsCache();
    } else {
        metricsCache_ = NULL;
    }
    service_name_ = "";
    service_name_setted_ = false;
    tags_ = new MetricsTags();
    tags_manager_ = new MetricsTagsManager(tags_);
}

KMonitor::~KMonitor() {
    Stop();
    delete manager_;
    delete metricsCache_;
    service_name_setted_ = false;
    delete tags_manager_;
    tags_manager_ = NULL;
    delete tags_;
    tags_ = NULL;
    AUTIL_LOG(INFO, "release kmonitor [%s].", name_.c_str());
}

const string &KMonitor::Name() {
    return name_;
}

void KMonitor::SetConfig(MetricsConfig *config, bool useConfigTags) {
    config_ = config;
    tags_manager_->SetConfig(config, useConfigTags);
}

void KMonitor::SetServiceName(const std::string& newServiceName)
{
    service_name_setted_ = true;
    service_name_ = newServiceName;
}

void KMonitor::Register(const string &name, MetricType metric_type, MetricLevel level) {
    string target_name = FullMetricsName(name);
    manager_->Register(target_name, metric_type, level, tags_manager_, metricsCache_);

    AUTIL_LOG(INFO, "register metric[%s] success", target_name.c_str());
}

MutableMetric* KMonitor::RegisterMetric(const std::string &name, MetricType metric_type, MetricLevel level) {
    string target_name = FullMetricsName(name);
    MutableMetric *mutable_metric = manager_->RegisterMetric(
            target_name, metric_type, level, tags_manager_, metricsCache_);

    AUTIL_LOG(INFO, "register mutable metric[%s] success", target_name.c_str());
    return mutable_metric;
}

void KMonitor::Unregister(const string &name) {
    string target_name = FullMetricsName(name);
    manager_->Unregister(target_name);
    AUTIL_LOG(INFO, "unregister metric[%s] success", target_name.c_str());
}

void KMonitor::GetMetrics(MetricsCollector *collector, const set<MetricLevel>& levels, int64_t curTimeMs) {
    manager_->Snapshot(collector, levels, curTimeMs);
    tags_manager_->ClearUselessTags();
}

void KMonitor::Report(const string &name, double value) {
    Report(name, NULL, value);
}


void KMonitor::Report(const string &name, const MetricsTags *tags, double value) {
    Metric *metric = NULL;
    if (metricsCache_ != NULL) {
        uint64_t tags_hash = (tags == NULL) ? 0 : tags->Hashcode();
        metric = metricsCache_->GetCachedMetic(name, tags_hash);
        if (metric != NULL) {
            Report(metric, value);
            return;
        }
    }

    metric = declareMetric(name, tags);
    Report(metric, value);
}

void KMonitor::Report(const std::string &name, const MetricsTagsPtr& tagsPtr, double value)
{
    string target_name = FullMetricsName(name);
    Metric* metric = manager_->GetMetric(target_name, tagsPtr);

    if (metric) {
        metric->Update(value);
    }
}

Metric* KMonitor::declareMetric(const string &name, const MetricsTags *tags) {
    string target_name = FullMetricsName(name);
    Metric *metric = NULL;
    uint64_t tags_hash = (NULL == tags) ? 0 : tags->Hashcode();
    MetricsTags target_tags = (NULL == tags) ? MetricsTags() : MetricsTags(*tags);
    if (tags_) {
        tags_->MergeTags(&target_tags);
    }
    MetricsTagsPtr tagsPtr = MetricsTagsPtr(new MetricsTags(target_tags));
    metric = manager_->GetMetric(target_name, tagsPtr);

    if (metric == NULL) {
        AUTIL_LOG(WARN, "metics[%s] tags[%s] can't match", target_name.c_str(),
                     tags->ToString().c_str());
        return NULL;
    }

    if (metricsCache_) {
        uint64_t mergedTags_hash = tagsPtr->Hashcode();
        metricsCache_->InsertCache(name, tags_hash, metric, target_name, mergedTags_hash);
    }

    return metric;
}

Metric* KMonitor::DeclareMetric(const string &name, const MetricsTags *tags) {
    Metric* metric = KMonitor::declareMetric(name, tags);
    if (metric != NULL) {
        metric->Acquire();
    }
    return metric;
}

bool KMonitor::UndeclareMetric(Metric* metric) {
    if (metric == NULL) {
        return false;
    }
    // 引用计数为0时，由下个上报周期强制回收该metric
    metric->Release();
    return true;
}

void KMonitor::registerBuildInMetrics(MetricsTags *tags, MetricLevel level, const string& metric_name_prefix) {

    string metric_name = string(config_->service_name());
    if (service_name_setted_ && !service_name_.empty()) {
        metric_name = service_name_;
    }
    if (metric_name_prefix != "") {
        metric_name = metric_name_prefix;
    }
    map<string, string> tags_map;
    if (NULL != tags) {
        tags_map = tags->GetTagsMap();
    }
    auto metricsTags = new MetricsTags(tags_map);
    if (tags_) {
        tags_->MergeTags(metricsTags);
    }
    MetricsTagsPtr tagsPtr(metricsTags);
    AUTIL_LOG(INFO, "register build metrics prefix [%s] tags [%s]",
                 metric_name.c_str(), tagsPtr->ToString().c_str());
    manager_->registerBuildInMetrics(metric_name, tagsPtr);
}

void KMonitor::Stop() {
    // manager_->Clear();
}

string KMonitor::FullMetricsName(const string &metric_name) {
    string target_metric;
    if (service_name_setted_) {
        if (service_name_.empty()) {
            return metric_name;
        }
        target_metric.append(service_name_);
    } else {
        target_metric.append(config_->service_name());
    }
    target_metric.append(".").append(metric_name);
    return target_metric;
}

END_KMONITOR_NAMESPACE(kmonitor);
