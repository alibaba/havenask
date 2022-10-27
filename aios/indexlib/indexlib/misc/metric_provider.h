#ifndef __INDEXLIB_MISC_METRICPROVIDER_H
#define __INDEXLIB_MISC_METRICPROVIDER_H

#include <kmonitor/client/MetricsReporter.h>
#include <kmonitor/client/MetricType.h>
#include <autil/Log.h>
#include <autil/TimeUtility.h>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/metric.h"

IE_NAMESPACE_BEGIN(misc);

class MetricProvider {
public:
    MetricProvider(kmonitor::MetricsReporterPtr reporter = {});
    ~MetricProvider();
public:
    kmonitor::MetricsReporterPtr GetReporter() { return mReporter; }
    MetricPtr DeclareMetric(const std::string& metricName,
                            kmonitor::MetricType type);
public:
    static const std::string INDEXLIB_DEFAULT_KMONITOR_SERVICE_NAME;

private:
    IE_LOG_DECLARE();

private:
    mutable autil::RecursiveThreadMutex mLock;
    kmonitor::MetricsReporterPtr mReporter;
    std::map<std::string, MetricPtr> mMetricMap;

};
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;

IE_NAMESPACE_END(misc);

#endif //__INDEXLIB_MISC_METRICPROVIDER_H
