#ifndef __INDEXLIB_MISC_METRIC_H
#define __INDEXLIB_MISC_METRIC_H

#include <kmonitor/client/MetricsReporter.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(misc);

// notice: no need to declare MutableMetrics first, just report it
class Metric {
public:
    Metric(const kmonitor::MetricsReporterPtr &reporter,
           const std::string& metricName,
           kmonitor::MetricType type);
    ~Metric();

public:
    void Report(double value)
    {
        mValue = value;
        if (mReporter)
        {
            mReporter->report(value, mMetricName, mType, nullptr);
        }
    }
    void Report(const kmonitor::MetricsTags* tags, double value)
    {
        mValue = value;
        if (mReporter)
        {
            mReporter->report(value, mMetricName, mType, tags);
        }
    }
    void Report(const kmonitor::MetricsTagsPtr tags, double value)
    {
        mValue = value;
        if (mReporter)
        {
            mReporter->report(value, mMetricName, mType, tags.get());
        }
    }

    void IncreaseQps(double value = 1.0)
    {
        mValue++;
        if (mReporter)
        {
            mReporter->report(value, mMetricName, mType, nullptr);
        }
    }

    double GetValue() { return mValue; }

private:
    kmonitor::MetricsReporterPtr mReporter;
    std::string mMetricName;
    kmonitor::MetricType mType;
    // store for test
    double mValue;
private:
    IE_LOG_DECLARE();

};
typedef std::shared_ptr<Metric> MetricPtr;

class ScopeLatencyReporter {
public:
    ScopeLatencyReporter(Metric* metric)
        : mBeginTime(autil::TimeUtility::currentTime())
        , mMetric(metric)
    {}
    ~ScopeLatencyReporter() {
        if (mMetric != NULL) {
            int64_t endTime = autil::TimeUtility::currentTime();
            mMetric->Report((endTime - mBeginTime) / 1000.0);
        }
    }
private:
    int64_t mBeginTime;
    Metric* mMetric;
};

IE_NAMESPACE_END(misc);

#endif //__INDEXLIB_IO_EXCEPTION_CONTROLLER_H
