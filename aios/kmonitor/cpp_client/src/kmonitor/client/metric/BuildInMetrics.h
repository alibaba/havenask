/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 10:47
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#ifndef KMONITOR_CLIENT_BUILDINMETRICS_H_
#define KMONITOR_CLIENT_BUILDINMETRICS_H_

#include <map>
#include <set>
#include <string>
#include <vector>

#include "autil/metric/ProcessCpu.h"
#include "autil/metric/ProcessMemory.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/core/MetricsCollector.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor/client/metric/Metric.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class BuildInMetrics {
public:
    BuildInMetrics(const std::string &metricName, const MetricsTagsPtr &metricsTags, MetricLevel level = NORMAL);
    ~BuildInMetrics();
    void Snapshot(MetricsCollector *collector, int64_t curTimeMs, const std::set<MetricLevel> *levels = NULL);

private:
    void getProcCpu(MetricsCollector *collector, int64_t curTime);
    void getProcMem(MetricsCollector *collector, int64_t curTime);

private:
    autil::metric::ProcessCpu processCpu_;
    autil::metric::ProcessMemory procMem_;

    std::string metricName_;
    MetricsTagsPtr metricsTags_;
    MetricLevel level_;

    // for build once
    MetricsInfoPtr cpuInfo_;
    MetricsInfoPtr memSizeInfo_;
    MetricsInfoPtr memRssInfo_;
    MetricsInfoPtr memRssRatioInfo_;
    MetricsInfoPtr memUsedInfo_;
    MetricsInfoPtr memUsedRatioInfo_;
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_BUILDINMETRICS_H_
