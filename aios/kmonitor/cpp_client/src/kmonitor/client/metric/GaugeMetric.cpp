/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 10:47
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#include "kmonitor/client/metric/GaugeMetric.h"

#include <math.h>
#include <string>
#include <vector>

BEGIN_KMONITOR_NAMESPACE(kmonitor);

using std::string;
using std::vector;

GaugeMetric::GaugeMetric(const string &name) : Metric(name) {}

GaugeMetric::~GaugeMetric() {}

void GaugeMetric::doUpdate(double value) { calculator_.Add(value); }

void GaugeMetric::doSnapshot(MetricsRecord *record, int64_t period) {
    if (calculator_.Count() == 0) {
        return;
    }
    record->AddValue(info_, calculator_.ToString());
    calculator_.Reset();
}

END_KMONITOR_NAMESPACE(kmonitor);
