/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 10:47
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#include "kmonitor/client/metric/CounterMetric.h"

#include <string>

BEGIN_KMONITOR_NAMESPACE(kmonitor);

using namespace std;

CounterMetric::CounterMetric(const string &name) : Metric(name), value_(0) {}

CounterMetric::~CounterMetric() {}

void CounterMetric::doUpdate(double value) { value_ += value; }

void CounterMetric::doSnapshot(MetricsRecord *record, int64_t period) { record->AddValue(info_, value_); }

END_KMONITOR_NAMESPACE(kmonitor);
