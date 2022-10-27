#ifndef ISEARCH_BS_PROCESSORMETRICSREPORTER_H
#define ISEARCH_BS_PROCESSORMETRICSREPORTER_H
 
IE_NAMESPACE_BEGIN(misc);
class MetricProvider;
class Metric;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
IE_NAMESPACE_END(misc);

struct ProcessorMetricsReporter {
    bool declareMetrics(IE_NAMESPACE(misc)::MetricProviderPtr metricProvider);
    IE_NAMESPACE(misc)::MetricPtr processLatencyMetric;
    IE_NAMESPACE(misc)::MetricPtr chainProcessLetencyMetric;
    IE_NAMESPACE(misc)::MetricPtr freshnessMetric;
    IE_NAMESPACE(misc)::MetricPtr errorQpsMetric;
    IE_NAMESPACE(misc)::MetricPtr waitProcessDocCount;
};

#endif //ISEARCH_BS_PROCESSORMETRICSREPORTER_H
