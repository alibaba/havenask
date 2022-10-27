#include "build_service/processor/ProcessorMetricReporter.h"
#include "build_service/util/Monitor.h"
#include <indexlib/misc/metric_provider.h>
#include <indexlib/misc/metric.h>

namespace build_service {
namespace processor {

BS_LOG_SETUP(processor, ProcessorMetricReporter);

bool ProcessorMetricReporter::declareMetrics(
        IE_NAMESPACE(misc)::MetricProviderPtr metricProvider)
{
    _processLatencyMetric = DECLARE_METRIC(metricProvider, basic/processLatency, kmonitor::GAUGE, "ms");
    _chainProcessLatencyMetric = DECLARE_METRIC(metricProvider, basic/chainProcessLatency, kmonitor::GAUGE, "ms");
    _prepareBatchProcessLatencyMetric = DECLARE_METRIC(metricProvider, basic/prepareBatchProcessLatency, kmonitor::GAUGE, "ms");
    _endBatchProcessLatencyMetric = DECLARE_METRIC(metricProvider, basic/endBatchProcessLatency, kmonitor::GAUGE, "ms");
    _processQpsMetric = DECLARE_METRIC(metricProvider, basic/processQps, kmonitor::QPS, "count");
    _processErrorQpsMetric = DECLARE_METRIC(metricProvider, debug/processErrorQps, kmonitor::QPS, "count");
    _totalDocCountMetric = DECLARE_METRIC(metricProvider, statistics/totalDocCount, kmonitor::STATUS, "count");
    return true;
}

void ProcessorMetricReporter::increaseDocCount(uint32_t docCount) {
    _totalDocCount += docCount;
    REPORT_METRIC(_totalDocCountMetric, _totalDocCount);
}

}
}
