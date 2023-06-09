/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2018-04-23 14:23
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#ifndef KMONITOR_CLIENT_CORE_MUTABLEMETRIC_H_
#define KMONITOR_CLIENT_CORE_MUTABLEMETRIC_H_

#include "autil/Lock.h"
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/metric/Metric.h"
#include "kmonitor/client/core/MetricsCache.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class MetricsTagsManager;
class MetricsData;
enum MetricType : unsigned int;

class MutableMetric {
 public:
    explicit MutableMetric(MetricsData *metric_data,
                           const std::string& metric_name,
                           MetricsTagsManager *tags_manager);
    ~MutableMetric();

    void Report(double value);
    void Report(const MetricsTags* tags, double value);

    void Report(const MetricsTagsPtr& tagsPtr, double value);
    MetricsTagsPtr GetMetricsTags(
            const std::map<std::string, std::string>& tags_map);
    Metric* DeclareMetric(const MetricsTags *tags);
    bool UndeclareMetric(Metric* metric);
    MetricType GetMetricType();

 private:
    MutableMetric(const MutableMetric &);
    MutableMetric& operator=(const MutableMetric &);
    Metric* declareMetric(const MetricsTags *tags);

 private:
    MetricsData *metric_data_ = nullptr;
    std::string metric_name_;
    MetricsTagsManager *tags_manager_ = nullptr;

 private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MutableMetric> MutableMetricPtr;

END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_CORE_MUTABLEMETRIC_H_
