/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 15:35
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#ifndef KMONITOR_CLIENT_CORE_METRICSSOURCE_H_
#define KMONITOR_CLIENT_CORE_METRICSSOURCE_H_

#include <set>
#include <string>
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/MetricLevel.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class MetricsCollector;

class MetricsSource {
 public:
    MetricsSource() {}
    virtual ~MetricsSource() {}
    virtual const std::string &Name() = 0;
    virtual void GetMetrics(MetricsCollector *collector, const std::set<MetricLevel>& levels, int64_t curTime) = 0;
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_CORE_METRICSSOURCE_H_
