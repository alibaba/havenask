#ifndef ISEARCH_BS_END2ENDLATENCYREPORTER_H
#define ISEARCH_BS_END2ENDLATENCYREPORTER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <indexlib/misc/metric.h>


IE_NAMESPACE_BEGIN(misc);
class MetricProvider;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
IE_NAMESPACE_END(misc);

namespace build_service {
namespace common {

class End2EndLatencyReporter
{
public:
    End2EndLatencyReporter();
    ~End2EndLatencyReporter();
private:
    End2EndLatencyReporter(const End2EndLatencyReporter &);
    End2EndLatencyReporter& operator=(const End2EndLatencyReporter &);
public:
    void init(IE_NAMESPACE(misc)::MetricProviderPtr metricProvider);
    void report(const std::string &source, int64_t latency);
private:
    IE_NAMESPACE(misc)::MetricPtr getMetric(const std::string &source);
private:
    typedef std::map<std::string, IE_NAMESPACE(misc)::MetricPtr> MetricMap;
    IE_NAMESPACE(misc)::MetricProviderPtr _metricProvider;
    IE_NAMESPACE(misc)::MetricPtr _end2EndLatencyMetric;
    MetricMap _end2EndLatencyMetricMap;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(End2EndLatencyReporter);

}
}

#endif //ISEARCH_BS_END2ENDLATENCYREPORTER_H
