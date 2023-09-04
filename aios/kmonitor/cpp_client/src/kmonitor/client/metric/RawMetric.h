/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 10:47
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#ifndef KMONITOR_CLIENT_METRIC_RAWMETRIC_H_
#define KMONITOR_CLIENT_METRIC_RAWMETRIC_H_

#include <string>
#include <vector>

#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/metric/Metric.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class RawMetric : public Metric {
public:
    virtual ~RawMetric();
    explicit RawMetric(const std::string &name);

protected:
    void doUpdate(double value) override;
    void doSnapshot(MetricsRecord *record, int64_t period) override;

private:
    RawMetric(const RawMetric &);
    RawMetric &operator=(const RawMetric &);

private:
    std::vector<double> values_;

private:
    friend class RawMetricTest_TestUpdate_Test;
    friend class RawMetricTest_TestSnapshot_Test;
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_METRIC_RAWMETRIC_H_
