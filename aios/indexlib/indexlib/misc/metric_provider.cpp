#include "indexlib/misc/metric_provider.h"
#include "indexlib/misc/indexlib_metric_control.h"

using namespace autil;
using namespace std;

IE_NAMESPACE_BEGIN(misc);
IE_LOG_SETUP(misc, MetricProvider);

const string MetricProvider::INDEXLIB_DEFAULT_KMONITOR_SERVICE_NAME = "indexlib";

MetricProvider::MetricProvider(kmonitor::MetricsReporterPtr reporter)
    : mReporter(reporter)
{
}

MetricProvider::~MetricProvider()
{
}

MetricPtr MetricProvider::DeclareMetric(const std::string& metricName,
                                        kmonitor::MetricType type)
{
    ScopedLock lock(mLock);
    auto iter = mMetricMap.find(metricName);
    if (iter != mMetricMap.end())
    {
        return iter->second;
    }
    IndexlibMetricControl::Status status =
        IndexlibMetricControl::GetInstance()->Get(metricName);

    if (status.prohibit)
    {
        IE_LOG(INFO, "register metric[%s] failed, prohibit to register",
               metricName.c_str());
        return misc::MetricPtr();
    }
    MetricPtr metric(new Metric(mReporter, metricName, type));
    mMetricMap.insert(make_pair(metricName, metric));
    return metric;
}


IE_NAMESPACE_END(misc);
