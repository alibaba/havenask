#include <autil/StringUtil.h>
#include "indexlib/misc/metric.h"
#include "indexlib/misc/indexlib_metric_control.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(misc);
IE_LOG_SETUP(misc, Metric);

Metric::Metric(const kmonitor::MetricsReporterPtr &reporter,
               const std::string& metricName,
               kmonitor::MetricType type)
    : mReporter(reporter)
    , mType(type)
    , mValue(std::numeric_limits<double>::min())
{
    mMetricName = metricName;
    StringUtil::replaceAll(mMetricName, "/", ".");
}

Metric::~Metric()
{
    if (mType == kmonitor::MetricType::STATUS && mReporter)
    {
        // when index partition is unload, should unregister status metric to invoid reporting this metric
        mReporter->unregister(mMetricName);
    }
}

IE_NAMESPACE_END(misc);
