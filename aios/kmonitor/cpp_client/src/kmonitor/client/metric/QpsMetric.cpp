/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 10:47
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#include <string>
#include "autil/TimeUtility.h"
#include "kmonitor/client/metric/QpsMetric.h"


BEGIN_KMONITOR_NAMESPACE(kmonitor);

using namespace std;
// IGRAPH_LOG_SETUP(client, QpsMetric);

QpsMetric::QpsMetric(const string &name)
    : Metric(name),
      value_(0) {
}

QpsMetric::~QpsMetric() {
}

void QpsMetric::doUpdate(double value) {
    value_ += value;
}

void QpsMetric::doSnapshot(MetricsRecord *record, int64_t period) {
    if (period > 0) {
        record->AddValue(info_, value_ * 1000/period);
    }
    value_ = 0;
}

END_KMONITOR_NAMESPACE(kmonitor);
