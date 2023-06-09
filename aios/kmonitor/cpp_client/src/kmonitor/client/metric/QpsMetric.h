/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 10:47
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#ifndef KMONITOR_CLIENT_METRIC_QPSMETRIC_H_
#define KMONITOR_CLIENT_METRIC_QPSMETRIC_H_

#include <string>
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/metric/Metric.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class QpsMetric : public Metric {
 public:
    ~QpsMetric();
    explicit QpsMetric(const std::string &name);
    virtual void doUpdate(double value) override;
    virtual void doSnapshot(MetricsRecord *record, int64_t period/*ms*/) override;

 private:
    QpsMetric(const QpsMetric &);
    QpsMetric &operator=(const QpsMetric &);

 private:
    double value_;
 private:
    friend class QpsMetricTest_TestUpdate_Test;
    friend class QpsMetricTest_TestSnapshot_Test;
};

TYPEDEF_PTR(QpsMetric);

END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_METRIC_QPSMETRIC_H_
