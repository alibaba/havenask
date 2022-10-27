#include "build_service/builder/BuilderMetrics.h"
#include "build_service/util/Monitor.h"

using namespace std;
namespace build_service {
namespace builder {

BS_LOG_SETUP(builder, BuilderMetrics);

BuilderMetrics::BuilderMetrics() {
    memset(this, 0, sizeof(*this));
}

BuilderMetrics::~BuilderMetrics() {
}

bool BuilderMetrics::declareMetrics(
        IE_NAMESPACE(misc)::MetricProviderPtr metricProvider, bool isKVorKKV)
{
    _buildQpsMetric = DECLARE_METRIC(metricProvider, basic/buildQps, kmonitor::QPS, "count");
    _totalDocCountMetric = DECLARE_METRIC(metricProvider, statistics/totalDocCount, kmonitor::STATUS, "count");

#define DEFINE_METRIC(metric)                                           \
    do {                                                                \
        _ ## metric ##QpsMetric = DECLARE_METRIC(metricProvider, basic/metric##Qps, kmonitor::QPS, "count"); \
  _ ## metric ## DocCountMetric = DECLARE_METRIC(metricProvider, statistics/metric ## DocCount, kmonitor::STATUS, "count"); \
 _ ## metric ## FailedQpsMetric = DECLARE_METRIC(metricProvider, debug/metric ## FailedQps, kmonitor::QPS, "count"); \
_ ## metric ## FailedDocCountMetric = DECLARE_METRIC(metricProvider, statistics/metric ## FailedDocCount, kmonitor::STATUS, "count"); \
    } while(0)

    DEFINE_METRIC(add);
    if (!isKVorKKV) { 
        DEFINE_METRIC(update);
        DEFINE_METRIC(delete);
        DEFINE_METRIC(deleteSub);
    }
    return true;
}

void BuilderMetrics::reportMetrics(bool ret, DocOperateType op) {
    _totalDocCount++;
    INCREASE_QPS(_buildQpsMetric);
    REPORT_METRIC(_totalDocCountMetric, _totalDocCount);

#define REPORT_DOC_METRIC(op, metric)                                   \
    case op:                                                            \
        if (ret) {                                                      \
            _ ## metric ## DocCount++;                                  \
                REPORT_METRIC(_ ## metric ## DocCountMetric, _ ## metric ## DocCount); \
                INCREASE_QPS(_ ## metric ## QpsMetric);                 \
        } else {                                                        \
            _ ## metric ## FailedDocCount++;                            \
                REPORT_METRIC(_ ## metric ## FailedDocCountMetric, _ ## metric ## FailedDocCount); \
                INCREASE_QPS(_ ## metric ## FailedQpsMetric);           \
        }                                                               \
        break

    switch (op) {
        REPORT_DOC_METRIC(ADD_DOC, add);
        REPORT_DOC_METRIC(UPDATE_FIELD, update);
        REPORT_DOC_METRIC(DELETE_DOC, delete);
        REPORT_DOC_METRIC(DELETE_SUB_DOC, deleteSub);
    default:
        string errorMsg = "unknown doc operator";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
    }
#undef REPORT_DOC_METRIC
}

void BuilderMetrics::reportMetrics(size_t totalMsgCount, size_t consumedMsgCount,
                                   DocOperateType op) {
    _totalDocCount += totalMsgCount;
    INCREASE_VALUE(_buildQpsMetric, totalMsgCount);
    REPORT_METRIC(_totalDocCountMetric, _totalDocCount);

#define REPORT_DOC_METRIC(op, metric)                                                     \
    case op:                                                                              \
        if (consumedMsgCount > 0) {                                                       \
            _##metric##DocCount += consumedMsgCount;                                      \
            REPORT_METRIC(_##metric##DocCountMetric, _##metric##DocCount);                \
            INCREASE_VALUE(_##metric##QpsMetric, consumedMsgCount);                       \
        }                                                                                 \
        if (consumedMsgCount != totalMsgCount) {                                          \
            _##metric##FailedDocCount += totalMsgCount - consumedMsgCount;                \
            REPORT_METRIC(_##metric##FailedDocCountMetric, _##metric##FailedDocCount);    \
            INCREASE_VALUE(_##metric##FailedQpsMetric, totalMsgCount - consumedMsgCount); \
        }                                                                                 \
        break

    switch (op) {
        REPORT_DOC_METRIC(ADD_DOC, add);
        REPORT_DOC_METRIC(UPDATE_FIELD, update);
        REPORT_DOC_METRIC(DELETE_DOC, delete);
        REPORT_DOC_METRIC(DELETE_SUB_DOC, deleteSub);
    default:
        string errorMsg = "unknown doc operator";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
    }
}
}
}
