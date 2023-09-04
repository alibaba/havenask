/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2018-04-23 14:23
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#include "kmonitor/client/core/MutableMetric.h"

#include <map>

#include "kmonitor/client/KMonitor.h"
#include "kmonitor/client/core/MetricsConfig.h"
#include "kmonitor/client/core/MetricsData.h"
#include "kmonitor/client/core/MetricsTagsManager.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);
AUTIL_LOG_SETUP(kmonitor, MutableMetric);

MutableMetric::MutableMetric(MetricsData *metric_data,
                             const std::string &metric_name,
                             MetricsTagsManager *tags_manager) {
    metric_data_ = metric_data;
    tags_manager_ = tags_manager;
    metric_name_ = metric_name;
}

MutableMetric::~MutableMetric() {
    tags_manager_ = NULL;
    metric_data_ = NULL;
}

void MutableMetric::Report(double value) { Report(NULL, value); }

void MutableMetric::Report(const MetricsTags *tags, double value) {
    Metric *metric = metric_data_->GetMetric(tags);
    if (metric != NULL) {
        metric->Update(value);
    }
}

void MutableMetric::Report(const MetricsTagsPtr &tagsPtr, double value) {
    Metric *metric = metric_data_->GetMetric(tagsPtr);
    if (metric != NULL) {
        metric->Update(value);
    }
}

MetricsTagsPtr MutableMetric::GetMetricsTags(const std::map<std::string, std::string> &tags_map) {
    if (tags_manager_ == NULL) {
        return MetricsTagsPtr();
    }
    return tags_manager_->GetMetricsTags(tags_map);
}

Metric *MutableMetric::declareMetric(const MetricsTags *tags) {
    Metric *metric = metric_data_->GetMetric(tags);
    if (metric == NULL) {
        AUTIL_LOG(DEBUG, "tags[%s] can't match", tags != nullptr ? tags->ToString().c_str() : "null");
        return NULL;
    }
    return metric;
}

Metric *MutableMetric::DeclareMetric(const MetricsTags *tags) {
    Metric *metric = declareMetric(tags);
    if (metric != NULL) {
        metric->Acquire();
    }
    return metric;
}

bool MutableMetric::UndeclareMetric(Metric *metric) {
    if (metric == NULL) {
        return false;
    }
    // 由下个上报周期强制回收该metric
    metric->Release();

    return true;
}

MetricType MutableMetric::GetMetricType() { return metric_data_->GetMetricType(); }

END_KMONITOR_NAMESPACE(kmonitor);
