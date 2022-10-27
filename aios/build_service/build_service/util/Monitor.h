#ifndef ISEARCH_BS_MONITOR_H
#define ISEARCH_BS_MONITOR_H

#include <indexlib/misc/metric.h>
#include <indexlib/misc/metric_provider.h>
#include "build_service/common_define.h"

using kmonitor::GAUGE;
using kmonitor::QPS;
using kmonitor::STATUS;

#define DECLARE_METRIC(metricProvider, metricName, metricType, unit)    \
    ({                                                                  \
        IE_NAMESPACE(misc)::MetricPtr metric;                               \
        if ((metricProvider)) {                                         \
            metric = (metricProvider)->DeclareMetric(#metricName, metricType); \
            if (metric == nullptr) {                                    \
                std::string errorMsg = "register metric[" + std::string(#metricName) + "] failed"; \
                BS_LOG(ERROR, "%s", errorMsg.c_str());                  \
            }                                                           \
        }                                                               \
        metric;                                                         \
    })
    

#define REPORT_METRIC(metric, count)            \
    do {                                        \
        if ((metric)) {                         \
            (metric)->Report((count));    \
        }                                       \
    } while (0)

#define INCREASE_QPS(metric)                    \
    do {                                        \
        if ((metric)) {                         \
            (metric)->IncreaseQps();            \
        }                                       \
    } while(0)

#define INCREASE_VALUE(metric, value)           \
    do {                                        \
        if ((metric)) {                         \
            (metric)->IncreaseQps(value);       \
        }                                       \
    } while(0)

#define REPORT_KMONITOR_METRIC(metric, count)   \
    do {                                        \
        if ((metric)) {                         \
            (metric)->report((count));          \
        }                                       \
    } while (0)

#define REPORT_KMONITOR_METRIC2(metric, tags, count)    \
    do {                                                \
        if ((metric)) {                                 \
            (metric)->report(tags, count);              \
        }                                               \
    } while (0)

#endif // ISEARCH_BS_MONITOR_H
