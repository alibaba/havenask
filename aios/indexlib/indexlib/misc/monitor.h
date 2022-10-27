#ifndef __INDEXLIB_MONITOR_H
#define __INDEXLIB_MONITOR_H

#include "indexlib/misc/metric_provider.h"

using kmonitor::GAUGE;
using kmonitor::QPS;
using kmonitor::STATUS;

#define IE_DECLARE_METRIC(metric)               \
    misc::MetricPtr m##metric##Metric;

#define IE_DECLARE_REACHABLE_METRIC(metric)                      \
    misc::MetricPtr m##metric##Metric;                           \
public:                                                          \
const misc::MetricPtr& Get##metric##Metric() const               \
{ return m##metric##Metric; }                                    \

#define IE_DECLARE_PARAM_METRIC(ParamType, metric)      \
public:                                                 \
ParamType m##metric = 0;                                \
misc::MetricPtr m##metric##Metric;                      \
public:                                                 \
void Increase##metric##Value(ParamType incValue)        \
{ m##metric += incValue; }                              \
void Decrease##metric##Value(ParamType decValue)        \
{ m##metric -= decValue; }                              \
ParamType Get##metric##Value() const                    \
{ return m##metric; }                                   \
void Set##metric##Value(ParamType value)                \
{ m##metric = value; }                                  \


#define IE_INIT_METRIC_GROUP(metricProvider, metric, metricName, metricType, unit) \
    do {                                                                \
        if (!metricProvider) {                                          \
            break;                                                      \
        }                                                               \
        std::string declareMetricName = string("indexlib/") + metricName; \
        m##metric##Metric = metricProvider->DeclareMetric(declareMetricName, metricType); \
    } while (0)

#define IE_INIT_LOCAL_INPUT_METRIC(reporter, metric)    \
    do {                                                \
        reporter.Init((m##metric##Metric));   \
    } while(0)


#define IE_REPORT_METRIC(metric, count)                 \
    do {                                                \
        if ((m##metric##Metric)) {                      \
            (m##metric##Metric)->Report((count));       \
        }                                               \
    } while (0)

#define IE_REPORT_METRIC_WITH_TAGS(metric, tags, count) \
    do {                                                \
        if ((m##metric##Metric)) {                      \
            (m##metric##Metric)->Report(tags, (count)); \
        }                                               \
    } while (0)

#define IE_INCREASE_QPS(metric)                    \
    do {                                        \
        if ((m##metric##Metric)) {              \
            (m##metric##Metric)->IncreaseQps(); \
        }                                       \
    } while(0)

#endif //__INDEXLIB_MONITOR_H
