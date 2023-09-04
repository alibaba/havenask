/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 10:05
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#include "kmonitor/client/core/MetricsFactory.h"

#include <string>

#include "kmonitor/client/metric/CounterMetric.h"
#include "kmonitor/client/metric/GaugeMetric.h"
#include "kmonitor/client/metric/Metric.h"
#include "kmonitor/client/metric/QpsMetric.h"
#include "kmonitor/client/metric/RawMetric.h"
#include "kmonitor/client/metric/StatusMetric.h"
#include "kmonitor/client/metric/SummaryMetric.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

using std::string;

Metric *MetricsFactory::CreateMetric(const string &name, MetricType metric_type) {
    Metric *metric;
    switch (metric_type) {
    case GAUGE:
        metric = new GaugeMetric(name);
        break;
    case SUMMARY:
        metric = new SummaryMetric(name);
        break;
    case QPS:
        metric = new QpsMetric(name);
        break;
    case COUNTER:
        metric = new CounterMetric(name);
        break;
    case RAW:
        metric = new RawMetric(name);
        break;
    case STATUS:
        metric = new StatusMetric(name);
        break;
    default:
        metric = NULL;
    }
    return metric;
}

END_KMONITOR_NAMESPACE(kmonitor);
