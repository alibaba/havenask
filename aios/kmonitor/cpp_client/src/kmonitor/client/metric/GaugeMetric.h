/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 10:47
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#ifndef KMONITOR_CLIENT_METRIC_GAUGEMETRIC_H_
#define KMONITOR_CLIENT_METRIC_GAUGEMETRIC_H_

#include <string>
#include <vector>

#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/common/MinMaxCalculator.h"
#include "kmonitor/client/metric/Metric.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class GaugeMetric : public Metric {
public:
    virtual ~GaugeMetric();
    GaugeMetric(const std::string &name);
    void doUpdate(double value) override;
    void doSnapshot(MetricsRecord *record, int64_t period) override;

private:
    GaugeMetric(const GaugeMetric &);
    GaugeMetric &operator=(const GaugeMetric &);

private:
    MinMaxCalculator calculator_;
};

TYPEDEF_PTR(GaugeMetric);

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_METRIC_GAUGEMETRIC_H_
