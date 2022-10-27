#ifndef ISEARCH_BS_BUILDERMETRICS_H
#define ISEARCH_BS_BUILDERMETRICS_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <indexlib/indexlib.h>

IE_NAMESPACE_BEGIN(misc);
class MetricProvider;
class Metric;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
IE_NAMESPACE_END(misc);

namespace build_service {
namespace builder {

class BuilderMetrics
{
public:
    BuilderMetrics();
    ~BuilderMetrics();
private:
    BuilderMetrics(const BuilderMetrics &);
    BuilderMetrics& operator=(const BuilderMetrics &);
public:
    bool declareMetrics(IE_NAMESPACE(misc)::MetricProviderPtr metricProvider, bool isKVorKKV = false);
    void reportMetrics(bool ret, DocOperateType op);
    void reportMetrics(size_t totalMsgCount, size_t consumedMsgCount, DocOperateType op);    
public:
    uint32_t getTotalDocCount() const {
        return _totalDocCount;
    }
public:
    uint32_t _addDocCount;
    uint32_t _addFailedDocCount;

    uint32_t _updateDocCount;
    uint32_t _updateFailedDocCount;

    uint32_t _deleteDocCount;
    uint32_t _deleteFailedDocCount;

    uint32_t _deleteSubDocCount;
    uint32_t _deleteSubFailedDocCount;

    uint32_t _totalDocCount;

    // metrics
    IE_NAMESPACE(misc)::MetricPtr _buildQpsMetric;

    IE_NAMESPACE(misc)::MetricPtr _addQpsMetric;
    IE_NAMESPACE(misc)::MetricPtr _addDocCountMetric;
    IE_NAMESPACE(misc)::MetricPtr _addFailedQpsMetric;
    IE_NAMESPACE(misc)::MetricPtr _addFailedDocCountMetric;

    IE_NAMESPACE(misc)::MetricPtr _updateQpsMetric;
    IE_NAMESPACE(misc)::MetricPtr _updateDocCountMetric;
    IE_NAMESPACE(misc)::MetricPtr _updateFailedQpsMetric;
    IE_NAMESPACE(misc)::MetricPtr _updateFailedDocCountMetric;

    IE_NAMESPACE(misc)::MetricPtr _deleteQpsMetric;
    IE_NAMESPACE(misc)::MetricPtr _deleteDocCountMetric;
    IE_NAMESPACE(misc)::MetricPtr _deleteFailedQpsMetric;
    IE_NAMESPACE(misc)::MetricPtr _deleteFailedDocCountMetric;

    IE_NAMESPACE(misc)::MetricPtr _deleteSubQpsMetric;
    IE_NAMESPACE(misc)::MetricPtr _deleteSubDocCountMetric;
    IE_NAMESPACE(misc)::MetricPtr _deleteSubFailedQpsMetric;
    IE_NAMESPACE(misc)::MetricPtr _deleteSubFailedDocCountMetric;

    IE_NAMESPACE(misc)::MetricPtr _totalDocCountMetric;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuilderMetrics);

}
}

#endif //ISEARCH_BS_BUILDERMETRICS_H
