/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2020-06-17
 * Author Name: dongdong.shan, wister.zwz
 * Author Email: dongdong.shan@alibaba-inc.com, wister.zwz@alibaba-inc.com
 * */

#pragma once
#include "autil/TimeUtility.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class ScopedLatencyReporter {
public:
    ScopedLatencyReporter(MetricsReporter *reporter, const std::string &metricName, MetricsTags *tags = nullptr)
        : _reporter(reporter), _metricName(metricName), _tags(tags), _beginTime(autil::TimeUtility::currentTime()) {}
    ScopedLatencyReporter(const ScopedLatencyReporter &) = delete;
    ~ScopedLatencyReporter() {
        auto latency = autil::TimeUtility::currentTime() - _beginTime;
        REPORT_USER_MUTABLE_LATENCY_TAGS(_reporter, _metricName, latency, _tags);
    }

private:
    MetricsReporter *_reporter;
    std::string _metricName;
    MetricsTags *_tags;
    int64_t _beginTime;
};

END_KMONITOR_NAMESPACE(kmonitor);
