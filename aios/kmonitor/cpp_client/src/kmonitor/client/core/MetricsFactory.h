/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 10:05
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#ifndef KMONITOR_CLIENT_CORE_METRICSFACTORY_H_
#define KMONITOR_CLIENT_CORE_METRICSFACTORY_H_

#include <string>

#include "kmonitor/client/MetricType.h"
#include "kmonitor/client/common/Common.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class Metric;

class MetricsFactory {
public:
    static Metric *CreateMetric(const std::string &name, MetricType metric_type);

private:
    MetricsFactory(const MetricsFactory &);
    MetricsFactory &operator=(const MetricsFactory &);
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_CORE_METRICSFACTORY_H_
