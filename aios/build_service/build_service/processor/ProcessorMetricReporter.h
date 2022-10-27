#ifndef ISEARCH_BS_PROCESSORMETRICSREPORTER_H
#define ISEARCH_BS_PROCESSORMETRICSREPORTER_H

#include "build_service/util/Log.h"

IE_NAMESPACE_BEGIN(misc);
class MetricProvider;
class Metric;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
IE_NAMESPACE_END(misc);

namespace build_service {
namespace processor {

class ProcessorMetricReporter {
public:
    ProcessorMetricReporter()
        : _totalDocCount(0)
    {
    }
public:
    void increaseDocCount(uint32_t docCount = 1);
public:
    bool declareMetrics(IE_NAMESPACE(misc)::MetricProviderPtr metricProvider);
    IE_NAMESPACE(misc)::MetricPtr _processLatencyMetric;
    IE_NAMESPACE(misc)::MetricPtr _chainProcessLatencyMetric;
    IE_NAMESPACE(misc)::MetricPtr _prepareBatchProcessLatencyMetric;
    IE_NAMESPACE(misc)::MetricPtr _endBatchProcessLatencyMetric;
    IE_NAMESPACE(misc)::MetricPtr _processQpsMetric;
    IE_NAMESPACE(misc)::MetricPtr _processErrorQpsMetric;
    IE_NAMESPACE(misc)::MetricPtr _totalDocCountMetric;
private:
    uint32_t _totalDocCount;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_PROCESSORMETRICSREPORTER_H
