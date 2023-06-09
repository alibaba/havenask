/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 10:47
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#ifndef KMONITOR_CLIENT_METRIC_STATUS_H_
#define KMONITOR_CLIENT_METRIC_STATUS_H_

#include <string>
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/metric/Metric.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class StatusMetric : public Metric {
 public:
    virtual ~StatusMetric();
    explicit StatusMetric(const std::string &name);
    virtual void doUpdate(double value) override;
    virtual void doSnapshot(MetricsRecord *record, int64_t period) override;

 private:
    StatusMetric(const StatusMetric &);
    StatusMetric &operator=(const StatusMetric &);

 private:
    double value_;
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_METRIC_STATUS_H_
