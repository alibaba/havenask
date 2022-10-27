#include "build_service/common/End2EndLatencyReporter.h"
#include "build_service/util/Monitor.h"

using namespace std;
IE_NAMESPACE_USE(misc);

namespace build_service {
namespace common {
BS_LOG_SETUP(common, End2EndLatencyReporter);

End2EndLatencyReporter::End2EndLatencyReporter()
{
}

End2EndLatencyReporter::~End2EndLatencyReporter() {
}

void End2EndLatencyReporter::init(MetricProviderPtr metricProvider) {
    _metricProvider = metricProvider;
    _end2EndLatencyMetric = DECLARE_METRIC(_metricProvider, basic/end2EndLatency, kmonitor::GAUGE, "ms");
}

void End2EndLatencyReporter::report(const std::string &source, int64_t latency) {
    REPORT_METRIC(_end2EndLatencyMetric, latency);
    if (source.empty()) {
        return;
    }
    IE_NAMESPACE(misc)::MetricPtr metric = getMetric(source);
    REPORT_METRIC(metric, latency);
}

IE_NAMESPACE(misc)::MetricPtr End2EndLatencyReporter::getMetric(const string &source) {
    MetricMap::iterator iter = _end2EndLatencyMetricMap.find(source);
    if (iter != _end2EndLatencyMetricMap.end()) {
        return iter->second;
    }
    if (!_metricProvider) {
        return NULL;
    }
    IE_NAMESPACE(misc)::MetricPtr metric;
    string metricName = "basic/end2EndLatency_" + source;
    metric = _metricProvider->DeclareMetric(metricName, kmonitor::GAUGE);
    if (!metric) {
        BS_LOG(ERROR, "register metric [%s] failed.", metricName.c_str());
    }
    _end2EndLatencyMetricMap[source] = metric;
    return metric;
}

}
}
